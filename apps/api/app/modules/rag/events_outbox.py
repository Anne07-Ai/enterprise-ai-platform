"""Outbox publishers for RAG domain events.

Wraps app.infra.outbox.enqueue with one helper per event type. Each
helper builds the JSON payload from a typed Pydantic event object so
api.py / workers don't have to know the dict shape.

The actual Kafka publish is done asynchronously by the outbox publisher
loop (started in app.main) — this module only writes the outbox row.
"""
from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.infra.outbox import enqueue
from app.modules.rag.events import (
    DocumentChunkedV1,
    DocumentFailedV1,
    DocumentReadyV1,
    DocumentUploadedV1,
)


async def emit_document_uploaded(
    session: AsyncSession, event: DocumentUploadedV1
) -> None:
    """Stage a document.uploaded.v1 event for publication."""
    await enqueue(
        session,
        topic=DocumentUploadedV1.TOPIC,
        payload=event.model_dump(mode="json"),
        key=str(event.document_id),
        org_id=event.org_id,
        actor_user_id=event.created_by,
        event_type="document.uploaded",
    )


async def emit_document_chunked(
    session: AsyncSession, event: DocumentChunkedV1
) -> None:
    """Stage a document.chunked.v1 event for publication."""
    await enqueue(
        session,
        topic=DocumentChunkedV1.TOPIC,
        payload=event.model_dump(mode="json"),
        key=str(event.document_id),
        org_id=event.org_id,
        event_type="document.chunked",
    )


async def emit_document_ready(
    session: AsyncSession, event: DocumentReadyV1
) -> None:
    """Stage a document.ready.v1 event for publication."""
    await enqueue(
        session,
        topic=DocumentReadyV1.TOPIC,
        payload=event.model_dump(mode="json"),
        key=str(event.document_id),
        org_id=event.org_id,
        event_type="document.ready",
    )


async def emit_document_failed(
    session: AsyncSession, event: DocumentFailedV1
) -> None:
    """Stage a document.failed.v1 event for publication."""
    await enqueue(
        session,
        topic=DocumentFailedV1.TOPIC,
        payload=event.model_dump(mode="json"),
        key=str(event.document_id),
        org_id=event.org_id,
        event_type="document.failed",
    )