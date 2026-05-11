"""Pydantic schemas for the RAG API.

Mirrors the patterns in app.modules.identity.schemas: separate Out
models for API responses (so we never accidentally leak internal
columns), and tight Create/Update models for inputs.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class DocumentOut(BaseModel):
    """Public view of a Document. Returned by upload, get, list."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    org_id: uuid.UUID
    created_by: uuid.UUID
    name: str
    mime_type: str
    byte_size: int
    status: str
    error_message: str | None = None
    chunk_count: int | None = None
    created_at: datetime
    updated_at: datetime


class DocumentListOut(BaseModel):
    """Paginated list response."""

    items: list[DocumentOut]
    total: int


class DocumentChunkOut(BaseModel):
    """Chunk view used by the search endpoint. Embeddings are NOT
    serialized — that would be 1536 floats per chunk and useful only
    inside the worker.
    """

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    document_id: uuid.UUID
    chunk_index: int
    text: str
    token_count: int


class DocumentSearchHit(BaseModel):
    """A single search hit: the chunk plus its similarity score."""

    chunk: DocumentChunkOut
    score: float = Field(
        ...,
        description="Cosine similarity 0..1 (1 = identical, 0 = orthogonal).",
    )
    document_name: str = Field(
        ...,
        description="Name of the parent document. Convenience for UI display.",
    )


class DocumentSearchOut(BaseModel):
    query: str
    hits: list[DocumentSearchHit]


class DocumentSearchQuery(BaseModel):
    """Search request body. Limits enforced server-side as well."""

    query: str = Field(..., min_length=1, max_length=2000)
    limit: int = Field(default=10, ge=1, le=50)
    document_id: uuid.UUID | None = Field(
        default=None,
        description="Optional: scope search to a single document.",
    )