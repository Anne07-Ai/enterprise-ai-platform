"""HTTP endpoints for the RAG pipeline.

This router is intentionally thin: parse request, call service,
serialize response. All real logic lives in
``app.modules.rag.service``.

Endpoints:
    POST   /v1/documents               — upload
    GET    /v1/documents               — list
    GET    /v1/documents/{id}          — get one
    DELETE /v1/documents/{id}          — delete (cascades chunks + storage)
    POST   /v1/documents/search        — semantic search
"""
from __future__ import annotations

import logging
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status

from app.core.deps import CurrentPrincipalDep, DBSession
from app.modules.identity.rbac import require_permission
from app.modules.rag import service
from app.modules.rag.embeddings import build_default_provider
from app.modules.rag.schemas import (
    DocumentListOut,
    DocumentOut,
    DocumentSearchOut,
    DocumentSearchQuery,
)
from app.modules.rag.storage import get_storage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/documents", tags=["documents"])


# --- accepted upload mime types ------------------------------------------

ALLOWED_MIME = {
    "application/pdf",
    "text/plain",
    "text/markdown",
}

MAX_UPLOAD_BYTES = 25 * 1024 * 1024  # 25 MB


# --- upload ---------------------------------------------------------------


@router.post(
    "",
    response_model=DocumentOut,
    status_code=status.HTTP_201_CREATED,
    dependencies=[require_permission("documents:create")],
)
async def upload_document(
    db: DBSession,
    principal: CurrentPrincipalDep,
    file: Annotated[UploadFile, File(description="The document to ingest.")],
) -> DocumentOut:
    """Upload a document and stage it for ingestion.

    Returns 201 with the new document row. Status starts at 'pending'.
    The ingestion-worker picks it up via the document.uploaded event.
    """
    if principal.user_id is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="API keys cannot upload documents",
        )

    mime = (file.content_type or "").lower()
    if mime not in ALLOWED_MIME:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"unsupported mime_type {mime!r}; allowed: {sorted(ALLOWED_MIME)}",
        )

    name = file.filename or "untitled"
    data = await file.read()
    if len(data) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="file is empty",
        )
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"file exceeds {MAX_UPLOAD_BYTES} bytes",
        )

    storage = get_storage()
    document, event = await service.create_document(
        db,
        storage,
        org_id=principal.org_id,
        created_by=principal.user_id,
        name=name,
        mime_type=mime,
        data=data,
    )

    # TODO Phase 3.2: emit `event` to the outbox so it lands on Kafka.
    # For now we log it so the test suite can assert on the side-effect
    # shape without needing a running worker.
    logger.info(
        "rag.event.staged",
        extra={
            "topic": event.TOPIC,
            "document_id": str(event.document_id),
            "event_id": str(event.event_id),
        },
    )

    return DocumentOut.model_validate(document)


# --- list / get -----------------------------------------------------------


@router.get(
    "",
    response_model=DocumentListOut,
    dependencies=[require_permission("documents:read")],
)
async def list_documents(
    db: DBSession,
    principal: CurrentPrincipalDep,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> DocumentListOut:
    docs, total = await service.list_documents(db, limit=limit, offset=offset)
    return DocumentListOut(
        items=[DocumentOut.model_validate(d) for d in docs],
        total=total,
    )


@router.get(
    "/{document_id}",
    response_model=DocumentOut,
    dependencies=[require_permission("documents:read")],
)
async def get_document(
    document_id: uuid.UUID,
    db: DBSession,
    principal: CurrentPrincipalDep,
) -> DocumentOut:
    doc = await service.get_document(db, document_id=document_id)
    if doc is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="document not found",
        )
    return DocumentOut.model_validate(doc)


# --- delete ---------------------------------------------------------------


@router.delete(
    "/{document_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[require_permission("documents:delete")],
)
async def delete_document(
    document_id: uuid.UUID,
    db: DBSession,
    principal: CurrentPrincipalDep,
) -> None:
    """Delete the document row (CASCADE removes chunks).

    Storage cleanup is best-effort — orphaned objects are reaped by a
    nightly job, not this endpoint.
    """
    doc = await service.get_document(db, document_id=document_id)
    if doc is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="document not found",
        )
    storage = get_storage()
    # Storage key matches the upload-side build. CASCADE handles chunks.
    key = storage.build_key(
        org_id=principal.org_id, document_id=doc.id, filename=doc.name
    )
    await db.delete(doc)
    await db.flush()
    try:
        await storage.delete(key=key)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "rag.delete.storage_orphan",
            extra={"key": key, "error": str(e)},
        )


# --- search ---------------------------------------------------------------


@router.post(
    "/search",
    response_model=DocumentSearchOut,
    dependencies=[require_permission("documents:search")],
)
async def search_documents(
    payload: DocumentSearchQuery,
    db: DBSession,
    principal: CurrentPrincipalDep,
) -> DocumentSearchOut:
    """Semantic search over the tenant's documents."""
    embedder = build_default_provider()
    try:
        hits = await service.search_chunks(
            db,
            embedder,
            query=payload.query,
            limit=payload.limit,
            document_id=payload.document_id,
        )
    finally:
        await embedder.aclose()

    return DocumentSearchOut(query=payload.query, hits=hits)