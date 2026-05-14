"""Per-event handler for the ingestion worker.

Receives one DocumentUploadedV1 payload and:
    1. Marks status pending -> extracting (guarded; skip if already past)
    2. Fetches the file from object storage
    3. Extracts text by mime type
    4. Marks status -> chunking
    5. Chunks the text
    6. Bulk-INSERTs chunks (embedding=NULL, ON CONFLICT skip)
    7. Marks status -> embedding
    8. Emits one DocumentChunkedV1 per chunk via the outbox

Idempotency:
    The chunks table has UNIQUE (document_id, chunk_index), so re-running
    is a no-op at the INSERT layer. Status transitions guard re-processing
    of already-ready documents.

Failure:
    Any exception in extract/chunk marks the document 'failed' with the
    error message and emits a DocumentFailedV1 event. Kafka offset is
    still committed (caller's responsibility) so the message doesn't
    re-deliver forever.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any

from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import AsyncSession

from app.modules.rag import service as rag_service
from app.modules.rag.chunker import chunk_text
# Import identity models so SQLAlchemy can resolve the FK from
# documents.created_by -> users.id and documents.org_id ->
# organizations.id when the worker only loads rag models.
from app.modules.identity import models as _identity_models  # noqa: F401
from app.modules.rag.events import (
    DocumentChunkedV1,
    DocumentFailedV1,
)
from app.modules.rag.events_outbox import (
    emit_document_chunked,
    emit_document_failed,
)
from app.modules.rag.models import DocumentStatus
from app.modules.rag.storage import DocumentStorage
from app.workers.ingestion.extractors import ExtractionError, extract_text

logger = logging.getLogger(__name__)


# Status values from which it's safe to (re-)start ingestion. If the
# document is already 'ready' or further along, we treat the event as
# a re-delivery and skip.
_REPROCESSABLE_STATUSES = frozenset(
    {DocumentStatus.PENDING, DocumentStatus.EXTRACTING, DocumentStatus.CHUNKING}
)


async def handle_document_uploaded(
    session: AsyncSession,
    storage: DocumentStorage,
    *,
    event_payload: dict[str, Any],
) -> None:
    """Process one document.uploaded.v1 event.

    The caller owns the SQLAlchemy session lifecycle and Kafka offset
    commit. This function does its work inside the caller's transaction
    so the chunk INSERTs and outbox writes commit atomically.
    """
    org_id = uuid.UUID(event_payload["org_id"])
    document_id = uuid.UUID(event_payload["document_id"])
    storage_uri = event_payload["storage_uri"]
    mime_type = event_payload["mime_type"]
    name = event_payload["name"]

    # Bind RLS to this org for the duration of the transaction.
    await session.execute(
        sql_text("SELECT set_config('app.current_org', :org, true)"),
        {"org": str(org_id)},
    )

    doc = await rag_service.get_document(session, document_id=document_id)
    if doc is None:
        logger.warning(
            "ingestion.document.missing",
            extra={"document_id": str(document_id), "org_id": str(org_id)},
        )
        return

    if doc.status not in _REPROCESSABLE_STATUSES:
        logger.info(
            "ingestion.skip.already_processed",
            extra={
                "document_id": str(document_id),
                "status": doc.status,
            },
        )
        return

    try:
        await _do_ingest(
            session,
            storage,
            org_id=org_id,
            document_id=document_id,
            storage_uri=storage_uri,
            mime_type=mime_type,
        )
    except Exception as exc:  # noqa: BLE001
        # Mark failed + emit failure event. We swallow the exception so
        # the Kafka consumer can commit the offset — this isn't a
        # retry-forever situation, the document is poisoned.
        logger.exception(
            "ingestion.failed",
            extra={
                "document_id": str(document_id),
                "error": str(exc),
            },
        )
        await rag_service.mark_status(
            session,
            document_id=document_id,
            status=DocumentStatus.FAILED,
            error_message=str(exc)[:1000],
        )
        await emit_document_failed(
            session,
            DocumentFailedV1(
                org_id=org_id,
                document_id=document_id,
                failed_stage="ingestion",
                error_message=str(exc)[:1000],
            ),
        )


async def _do_ingest(
    session: AsyncSession,
    storage: DocumentStorage,
    *,
    org_id: uuid.UUID,
    document_id: uuid.UUID,
    storage_uri: str,
    mime_type: str,
) -> None:
    """The happy path. Raises on any failure; caller catches + marks failed."""

    # ---- 1. extract --------------------------------------------------------
    await rag_service.mark_status(
        session, document_id=document_id, status=DocumentStatus.EXTRACTING
    )

    storage_key = _storage_key_from_uri(storage_uri)
    raw = await storage.get(key=storage_key)
    try:
        text_content = extract_text(mime_type=mime_type, data=raw)
    except ExtractionError as exc:
        raise RuntimeError(f"extraction failed: {exc}") from exc

    if not text_content.strip():
        raise RuntimeError("extracted text is empty")

    # ---- 2. chunk ----------------------------------------------------------
    await rag_service.mark_status(
        session, document_id=document_id, status=DocumentStatus.CHUNKING
    )
    chunks = chunk_text(text_content)
    if not chunks:
        raise RuntimeError("chunker produced zero chunks from non-empty text")

    # ---- 3. insert chunks --------------------------------------------------
    inserted = await rag_service.insert_chunks(
        session,
        org_id=org_id,
        document_id=document_id,
        chunks=[(c.index, c.text, c.token_count) for c in chunks],
    )

    # ---- 4. mark embedding-pending + emit per-chunk events -----------------
    await rag_service.mark_status(
        session, document_id=document_id, status=DocumentStatus.EMBEDDING
    )

    for chunk_row in inserted:
        await emit_document_chunked(
            session,
            DocumentChunkedV1(
                org_id=org_id,
                document_id=document_id,
                chunk_id=chunk_row.id,
                chunk_index=chunk_row.chunk_index,
                text=chunk_row.text,
                token_count=chunk_row.token_count,
            ),
        )

    logger.info(
        "ingestion.completed",
        extra={
            "document_id": str(document_id),
            "chunk_count": len(inserted),
        },
    )


def _storage_key_from_uri(uri: str) -> str:
    """Convert s3://eaip-documents/<key...> -> <key...>.

    We don't use urllib.parse here because S3 URIs aren't standard URIs;
    the host segment is the bucket. We split on the third '/' instead.
    """
    if not uri.startswith("s3://"):
        raise ValueError(f"invalid storage_uri: {uri!r}")
    # s3://bucket/key/with/slashes -> split after bucket
    without_scheme = uri[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")
    if not key:
        raise ValueError(f"no key in storage_uri: {uri!r}")
    return key