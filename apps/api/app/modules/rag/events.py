"""Kafka event schemas for the RAG pipeline.

Events flow:

    API (POST /v1/documents) ──> document.uploaded.v1
                                          │
                                          ▼
    ingestion-worker reads file, extracts text, chunks,
    INSERTs document_chunks rows (embedding=NULL),
    emits one event per chunk
                                          │
                                          ▼
                                  document.chunked.v1
                                          │
                                          ▼
    embedding-worker calls OpenAI, UPDATEs chunk.embedding,
    when all chunks for a doc are embedded, marks document ready

Schemas are Pydantic models for validation. Topic names follow the
``<aggregate>.<event>.v<n>`` convention so we can evolve later.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field


class _EventBase(BaseModel):
    """Base for all RAG events.

    The ``event_id`` and ``occurred_at`` are set on emit so consumers
    can deduplicate and trace.
    """

    model_config = ConfigDict(frozen=True)

    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now())
    org_id: uuid.UUID


class DocumentUploadedV1(_EventBase):
    """Emitted by the API immediately after a successful upload.

    Kafka topic: ``document.uploaded.v1``
    Consumed by: ingestion-worker.
    """

    TOPIC: ClassVar[str] = "document.uploaded.v1"

    document_id: uuid.UUID
    storage_uri: str
    mime_type: str
    byte_size: int
    name: str
    created_by: uuid.UUID


class DocumentChunkedV1(_EventBase):
    """Emitted by the ingestion-worker for each chunk it produces.

    Kafka topic: ``document.chunked.v1``
    Consumed by: embedding-worker.

    Carries the chunk text so the embedding worker doesn't need to
    re-read it from the database. Saves a round-trip in the hot path.
    """

    TOPIC: ClassVar[str] = "document.chunked.v1"

    document_id: uuid.UUID
    chunk_id: uuid.UUID
    chunk_index: int
    text: str
    token_count: int


class DocumentReadyV1(_EventBase):
    """Emitted by the embedding-worker once every chunk is embedded.

    Kafka topic: ``document.ready.v1``
    Consumed by: future chat / search subscribers.
    """

    TOPIC: ClassVar[str] = "document.ready.v1"

    document_id: uuid.UUID
    chunk_count: int


class DocumentFailedV1(_EventBase):
    """Emitted by any worker that gives up on a document.

    Kafka topic: ``document.failed.v1``
    Consumed by: alerting + dead-letter dashboard.
    """

    TOPIC: ClassVar[str] = "document.failed.v1"

    document_id: uuid.UUID
    failed_stage: str  # "extracting" | "chunking" | "embedding"
    error_message: str