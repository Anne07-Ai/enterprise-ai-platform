"""documents and chunks for RAG ingestion

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-09

Adds two tenant-scoped tables for the RAG pipeline:

  * documents       — one row per uploaded file (PDF, txt, md). Tracks
                      ingestion lifecycle via the ``status`` column.
  * document_chunks — paragraph-aware chunks of each document, with an
                      optional embedding vector populated by the embedding
                      worker.

Both tables are RLS-enabled with the same policy used in 0001:
    org_id = current_org OR bypass_rls = on

Design notes:
  * org_id is denormalized onto document_chunks (despite the FK to
    documents) so the RLS policy doesn't need a JOIN. RLS with a JOIN is
    measurably slower under load and harder for the planner to optimize.
  * status is plain text + CHECK constraint, not a Postgres ENUM. ENUMs
    are painful to migrate (adding a value requires a special op,
    removing requires a full rebuild). Text + check is good enough.
  * embedding column uses pgvector(1536) — the dimension of OpenAI's
    text-embedding-3-small. If we swap providers we'll need a new
    migration to add another vector column or recompute. ADR-006 explains.
  * No index on the embedding column yet. We'll add an HNSW index in a
    later migration once we know the corpus size — premature indexing
    on tiny tables hurts more than it helps.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


VALID_STATUSES = (
    "pending",
    "extracting",
    "chunking",
    "embedding",
    "ready",
    "failed",
)


def upgrade() -> None:
    # --- pgvector extension ------------------------------------------------
    # Already enabled in 0001 for the platform's eventual semantic-search
    # needs, but make it explicit and idempotent.
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # --- documents ---------------------------------------------------------
    op.execute(
        """
        CREATE TABLE documents (
            id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id          UUID         NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
            created_by      UUID         NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
            name            TEXT         NOT NULL,
            mime_type       TEXT         NOT NULL,
            byte_size       BIGINT       NOT NULL CHECK (byte_size >= 0),
            storage_uri     TEXT         NOT NULL,
            status          TEXT         NOT NULL DEFAULT 'pending'
                                         CHECK (status IN ('pending','extracting','chunking','embedding','ready','failed')),
            error_message   TEXT,
            chunk_count     INT,
            created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX documents_org_id_idx ON documents(org_id)")
    op.execute("CREATE INDEX documents_status_idx ON documents(status) WHERE status != 'ready'")
    op.execute(
        """
        CREATE TRIGGER trg_documents_updated_at
            BEFORE UPDATE ON documents
            FOR EACH ROW EXECUTE FUNCTION set_updated_at()
        """
    )

    # --- document_chunks ---------------------------------------------------
    op.execute(
        """
        CREATE TABLE document_chunks (
            id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
            document_id     UUID         NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
            org_id          UUID         NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
            chunk_index     INT          NOT NULL CHECK (chunk_index >= 0),
            text            TEXT         NOT NULL,
            token_count     INT          NOT NULL CHECK (token_count >= 0),
            embedding       vector(1536),
            created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
            UNIQUE (document_id, chunk_index)
        )
        """
    )
    op.execute("CREATE INDEX document_chunks_org_id_idx ON document_chunks(org_id)")
    op.execute("CREATE INDEX document_chunks_document_id_idx ON document_chunks(document_id)")
    # Partial index to find chunks that still need embedding.
    op.execute(
        """
        CREATE INDEX document_chunks_pending_embedding_idx
            ON document_chunks(document_id, chunk_index)
            WHERE embedding IS NULL
        """
    )

    # --- RLS policies ------------------------------------------------------
    policy_using = (
        "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid "
        "OR current_setting('app.bypass_rls', true) = 'on'"
    )
    for tbl in ("documents", "document_chunks"):
        op.execute(f"ALTER TABLE {tbl} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {tbl} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"""
            CREATE POLICY {tbl}_tenant_isolation ON {tbl}
              USING ({policy_using})
              WITH CHECK ({policy_using})
            """
        )


def downgrade() -> None:
    for tbl in ("document_chunks", "documents"):
        op.execute(f"DROP POLICY IF EXISTS {tbl}_tenant_isolation ON {tbl}")
    op.execute("DROP TRIGGER IF EXISTS trg_documents_updated_at ON documents")
    op.drop_table("document_chunks")
    op.drop_table("documents")
