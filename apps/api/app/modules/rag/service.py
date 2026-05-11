"""RAG service layer.

The API endpoints in ``app/modules/rag/api.py`` are thin wrappers
around these functions. All tenant isolation, persistence, event
publishing, and similarity search logic lives here so it can be
unit-tested independently of FastAPI.
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import Sequence

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.modules.rag.embeddings import EmbeddingProvider
from app.modules.rag.events import DocumentUploadedV1
from app.modules.rag.models import Document, DocumentChunk, DocumentStatus
from app.modules.rag.schemas import DocumentSearchHit, DocumentChunkOut
from app.modules.rag.storage import DocumentStorage

logger = logging.getLogger(__name__)


# --- upload ---------------------------------------------------------------


async def create_document(
    session: AsyncSession,
    storage: DocumentStorage,
    *,
    org_id: uuid.UUID,
    created_by: uuid.UUID,
    name: str,
    mime_type: str,
    data: bytes,
) -> tuple[Document, DocumentUploadedV1]:
    """Persist a document and stage it for async ingestion.

    Steps:
        1. Insert Document row with status='pending'.
        2. Upload bytes to object storage at the canonical key.
        3. Write the storage_uri back on the row.
        4. Build (but DO NOT publish) the DocumentUploadedV1 event —
           caller is responsible for publishing in the same DB tx via
           the outbox so the event is durable.

    Returns the persisted Document and the event payload the caller
    should hand to the outbox.

    NOTE: We deliberately do NOT publish to Kafka inline. The outbox
    pattern (Phase 2) ensures the event is committed atomically with
    the row insert. If we published to Kafka here, a crash between the
    INSERT and the publish would lose the event.
    """
    if not data:
        raise ValueError("data must not be empty")
    if not name:
        raise ValueError("name is required")

    document = Document(
        org_id=org_id,
        created_by=created_by,
        name=name,
        mime_type=mime_type,
        byte_size=len(data),
        storage_uri="",  # filled in after upload
        status=DocumentStatus.PENDING,
    )
    session.add(document)
    await session.flush()  # populates document.id

    key = storage.build_key(
        org_id=org_id, document_id=document.id, filename=name
    )
    uri = storage.build_uri(key)

    # Storage write happens BEFORE we finalize the row. If storage
    # fails the row's storage_uri is empty — the partial-state
    # reconciliation worker (status='pending' for too long) will
    # mark it failed.
    await storage.put(key=key, data=data, content_type=mime_type)
    document.storage_uri = uri
    await session.flush()

    event = DocumentUploadedV1(
        org_id=org_id,
        document_id=document.id,
        storage_uri=uri,
        mime_type=mime_type,
        byte_size=len(data),
        name=name,
        created_by=created_by,
    )

    logger.info(
        "rag.document.created",
        extra={
            "document_id": str(document.id),
            "org_id": str(org_id),
            "byte_size": len(data),
        },
    )
    return document, event


# --- read -----------------------------------------------------------------


async def get_document(
    session: AsyncSession,
    *,
    document_id: uuid.UUID,
) -> Document | None:
    """Fetch a single document by id. RLS scopes to current_org."""
    stmt = select(Document).where(Document.id == document_id)
    return (await session.execute(stmt)).scalars().one_or_none()


async def list_documents(
    session: AsyncSession,
    *,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[Document], int]:
    """List documents in the current tenant. Newest first."""
    if limit < 1 or limit > 200:
        raise ValueError("limit must be in [1, 200]")
    if offset < 0:
        raise ValueError("offset must be >= 0")

    total_stmt = select(func.count()).select_from(Document)
    total = (await session.execute(total_stmt)).scalar_one()

    stmt = (
        select(Document)
        .order_by(Document.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    rows = (await session.execute(stmt)).scalars().all()
    return list(rows), total


# --- search ---------------------------------------------------------------


async def search_chunks(
    session: AsyncSession,
    embedder: EmbeddingProvider,
    *,
    query: str,
    limit: int = 10,
    document_id: uuid.UUID | None = None,
) -> list[DocumentSearchHit]:
    """Semantic search across the current tenant's chunks.

    Steps:
        1. Embed the query.
        2. Run a pgvector cosine-distance kNN query, optionally scoped
           to a single document.
        3. JOIN documents for the name (so the UI can show it).
        4. Return ordered hits with similarity scores in [0, 1] where
           1 = identical (we convert cosine *distance* to *similarity*).

    Tenant isolation is enforced by RLS — we never filter by org_id
    here. If the GUC isn't set, the query returns zero rows.
    """
    if not query.strip():
        raise ValueError("query must not be empty")
    if limit < 1 or limit > 50:
        raise ValueError("limit must be in [1, 50]")

    vectors = await embedder.embed([query])
    if not vectors:
        return []
    query_vec = vectors[0]

    # pgvector's <=> operator is cosine distance: 0 = identical, 2 = opposite.
    # We compute similarity as 1 - (distance / 2) so 1 = identical, 0 = opposite.
    distance = DocumentChunk.embedding.cosine_distance(query_vec)
    similarity = (1 - distance / 2).label("similarity")

    stmt = (
        select(DocumentChunk, Document.name.label("document_name"), similarity)
        .join(Document, Document.id == DocumentChunk.document_id)
        .where(DocumentChunk.embedding.is_not(None))
    )
    if document_id is not None:
        stmt = stmt.where(DocumentChunk.document_id == document_id)
    stmt = stmt.order_by(distance.asc()).limit(limit)

    rows = (await session.execute(stmt)).all()
    return [
        DocumentSearchHit(
            chunk=DocumentChunkOut.model_validate(chunk),
            score=float(sim),
            document_name=doc_name,
        )
        for chunk, doc_name, sim in rows
    ]


# --- worker-facing helpers ------------------------------------------------


async def mark_status(
    session: AsyncSession,
    *,
    document_id: uuid.UUID,
    status: str,
    error_message: str | None = None,
) -> None:
    """Workers call this to transition a document's status."""
    if status not in DocumentStatus.ALL:
        raise ValueError(f"invalid status: {status}")

    doc = await get_document(session, document_id=document_id)
    if doc is None:
        logger.warning(
            "rag.mark_status.missing", extra={"document_id": str(document_id)}
        )
        return
    doc.status = status
    if error_message is not None:
        doc.error_message = error_message
    await session.flush()


async def insert_chunks(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    document_id: uuid.UUID,
    chunks: Sequence[tuple[int, str, int]],
) -> list[DocumentChunk]:
    """Bulk insert chunks for a document.

    ``chunks`` is a sequence of ``(index, text, token_count)`` tuples.
    Returns the persisted chunks. Embeddings are NULL — the embedding
    worker fills them in.

    Idempotent: the unique (document_id, chunk_index) constraint means
    re-running this for the same document is a no-op (or raises a
    handled IntegrityError — caller decides).
    """
    rows = [
        DocumentChunk(
            org_id=org_id,
            document_id=document_id,
            chunk_index=idx,
            text=text,
            token_count=token_count,
        )
        for idx, text, token_count in chunks
    ]
    session.add_all(rows)
    await session.flush()
    return rows


async def update_chunk_embedding(
    session: AsyncSession,
    *,
    chunk_id: uuid.UUID,
    embedding: list[float],
) -> None:
    """The embedding worker calls this for each chunk after embedding."""
    stmt = select(DocumentChunk).where(DocumentChunk.id == chunk_id)
    chunk = (await session.execute(stmt)).scalars().one_or_none()
    if chunk is None:
        logger.warning("rag.embedding.chunk_missing", extra={"chunk_id": str(chunk_id)})
        return
    chunk.embedding = embedding
    await session.flush()


async def count_chunks_pending_embedding(
    session: AsyncSession,
    *,
    document_id: uuid.UUID,
) -> int:
    """Returns how many chunks for a document still need embedding.

    Embedding-worker calls this after writing each chunk; when it
    drops to 0, the worker marks the document ready.
    """
    stmt = (
        select(func.count())
        .select_from(DocumentChunk)
        .where(
            and_(
                DocumentChunk.document_id == document_id,
                DocumentChunk.embedding.is_(None),
            )
        )
    )
    return (await session.execute(stmt)).scalar_one()