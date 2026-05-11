"""ORM models for the RAG pipeline.

Mirrors migration 0002_documents_and_chunks.py. RLS is enforced at the
database layer; the ORM does not need to (and cannot) re-implement it.

The ``Document.status`` field is a plain string with a CHECK constraint
in the migration (see ADR-006 for why we avoid Postgres ENUMs). The
``DocumentStatus`` class below is a type-checking convenience, not a DB type.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infra.db import Base

if TYPE_CHECKING:
    pass  # type-only imports if needed later


class DocumentStatus:
    """String constants for valid ``Document.status`` values.

    Kept as a plain class (not StrEnum) so SQLAlchemy stores raw text
    and the migration's CHECK constraint is the single source of truth.
    """

    PENDING = "pending"
    EXTRACTING = "extracting"
    CHUNKING = "chunking"
    EMBEDDING = "embedding"
    READY = "ready"
    FAILED = "failed"

    ALL = (PENDING, EXTRACTING, CHUNKING, EMBEDDING, READY, FAILED)


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    org_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )
    created_by: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    mime_type: Mapped[str] = mapped_column(Text, nullable=False)
    byte_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    storage_uri: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        String, nullable=False, default=DocumentStatus.PENDING, server_default="pending"
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    chunk_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    chunks: Mapped[list["DocumentChunk"]] = relationship(
        "DocumentChunk",
        back_populates="document",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        CheckConstraint("byte_size >= 0", name="documents_byte_size_check"),
        # Status CHECK is enforced by the migration; not duplicated here.
    )

    def __repr__(self) -> str:
        return (
            f"<Document id={self.id} org_id={self.org_id} "
            f"name={self.name!r} status={self.status}>"
        )


class DocumentChunk(Base):
    __tablename__ = "document_chunks"

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    document_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("documents.id", ondelete="CASCADE"),
        nullable=False,
    )
    # Denormalized for RLS performance — see ADR-006.
    org_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    token_count: Mapped[int] = mapped_column(Integer, nullable=False)
    embedding: Mapped[list[float] | None] = mapped_column(
        Vector(1536), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    document: Mapped["Document"] = relationship("Document", back_populates="chunks")

    __table_args__ = (
        UniqueConstraint(
            "document_id", "chunk_index", name="document_chunks_document_id_chunk_index_key"
        ),
        CheckConstraint("chunk_index >= 0", name="document_chunks_chunk_index_check"),
        CheckConstraint("token_count >= 0", name="document_chunks_token_count_check"),
        Index("document_chunks_org_id_idx", "org_id"),
        Index("document_chunks_document_id_idx", "document_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<DocumentChunk id={self.id} doc={self.document_id} "
            f"index={self.chunk_index} embedded={self.embedding is not None}>"
        )