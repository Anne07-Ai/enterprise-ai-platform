"""Per-event handler for the embedding worker.

Receives one DocumentChunkedV1 payload and:
    1. Embed the chunk text via OpenAI.
    2. UPDATE the document_chunks row with the embedding vector.
    3. Check if all chunks for the document are now embedded.
       - If yes, mark the document 'ready' and emit DocumentReadyV1.
       - If no, do nothing — wait for the remaining chunks.

Idempotency:
    UPDATE is naturally idempotent (overwriting with the same value
    is fine). The 'mark ready' transition is guarded — it only fires
    when count_chunks_pending_embedding == 0, and only if the document
    isn't already 'ready'/'failed'.

Failure modes:
    OpenAI returns an error -> the worker re-raises so the Kafka
    consumer skips the offset commit; the message will redeliver on
    next worker start.

    Document already 'ready' or 'failed' -> we still embed the chunk
    (cheap) but skip the ready-transition. This handles the case
    where an earlier embedding failed mid-way and the document was
    marked failed manually.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any

from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import AsyncSession

# Import identity models so SQLAlchemy can resolve cross-table FKs
# when the worker only loads rag models.
from app.modules.identity import models as _identity_models  # noqa: F401
from app.modules.rag import service as rag_service
from app.modules.rag.embeddings import EmbeddingProvider
from app.modules.rag.events import DocumentReadyV1
from app.modules.rag.events_outbox import emit_document_ready
from app.modules.rag.models import DocumentStatus

logger = logging.getLogger(__name__)


async def handle_document_chunked(
    session: AsyncSession,
    embedder: EmbeddingProvider,
    *,
    event_payload: dict[str, Any],
) -> None:
    """Process one document.chunked.v1 event.

    Embeds the chunk and updates the row. If this was the last chunk
    for the document, marks the document ready and emits the event.
    """
    org_id = uuid.UUID(event_payload["org_id"])
    document_id = uuid.UUID(event_payload["document_id"])
    chunk_id = uuid.UUID(event_payload["chunk_id"])
    chunk_text_content = event_payload["text"]

    # RLS: scope to this org for the entire transaction.
    await session.execute(
        sql_text("SELECT set_config('app.current_org', :org, true)"),
        {"org": str(org_id)},
    )

    # ---- 1. embed the chunk ------------------------------------------------
    vectors = await embedder.embed([chunk_text_content])
    if not vectors:
        raise RuntimeError(f"embedder returned no vectors for chunk {chunk_id}")
    embedding = vectors[0]

    # ---- 2. UPDATE the chunk row ------------------------------------------
    await rag_service.update_chunk_embedding(
        session, chunk_id=chunk_id, embedding=embedding
    )

    # ---- 3. is this the last chunk? ---------------------------------------
    pending = await rag_service.count_chunks_pending_embedding(
        session, document_id=document_id
    )
    if pending > 0:
        logger.info(
            "embedding.chunk.done",
            extra={
                "document_id": str(document_id),
                "chunk_id": str(chunk_id),
                "remaining": pending,
            },
        )
        return

    # All chunks embedded. Mark ready (if not already).
    doc = await rag_service.get_document(session, document_id=document_id)
    if doc is None:
        logger.warning(
            "embedding.document.missing",
            extra={"document_id": str(document_id)},
        )
        return

    if doc.status == DocumentStatus.READY:
        logger.info("embedding.document.already_ready", extra={"document_id": str(document_id)})
        return

    if doc.status == DocumentStatus.FAILED:
        logger.warning(
            "embedding.document.marked_failed_elsewhere",
            extra={"document_id": str(document_id)},
        )
        return

    # Update chunk_count + status atomically.
    doc.status = DocumentStatus.READY
    doc.chunk_count = await _total_chunks(session, document_id=document_id)
    await session.flush()

    await emit_document_ready(
        session,
        DocumentReadyV1(
            org_id=org_id,
            document_id=document_id,
            chunk_count=doc.chunk_count,
        ),
    )

    logger.info(
        "embedding.document.ready",
        extra={
            "document_id": str(document_id),
            "chunk_count": doc.chunk_count,
        },
    )


async def _total_chunks(session: AsyncSession, *, document_id: uuid.UUID) -> int:
    """Count all chunks for a document, embedded or not."""
    from sqlalchemy import func, select

    from app.modules.rag.models import DocumentChunk

    stmt = (
        select(func.count())
        .select_from(DocumentChunk)
        .where(DocumentChunk.document_id == document_id)
    )
    return (await session.execute(stmt)).scalar_one()