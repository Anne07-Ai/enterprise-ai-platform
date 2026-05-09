"""init identity and audit

Creates the Phase 2 schema:

  * organizations, users, memberships, api_keys, refresh_tokens
  * audit_log (range-partitioned by month) with three initial partitions
  * outbox (transactional outbox)

Plus row-level security policies on every tenant-scoped table — they read
``current_setting('app.current_org', true)::uuid`` which the API session
factory sets at the start of every transaction.

Revision ID: 0001
Revises:
Create Date: 2026-05-08
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Tenant-scoped tables — every entry gets RLS turned on with a uniform policy.
TENANT_TABLES = ("memberships", "api_keys", "refresh_tokens", "outbox")


def upgrade() -> None:
    # --- extensions ------------------------------------------------------
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.execute("CREATE EXTENSION IF NOT EXISTS citext")
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # --- enums -----------------------------------------------------------
    op.execute(
        "CREATE TYPE org_role AS ENUM ('owner', 'admin', 'member', 'viewer')"
    )
    op.execute(
        "CREATE TYPE api_key_status AS ENUM ('active', 'revoked')"
    )

    # --- organizations ---------------------------------------------------
    op.create_table(
        "organizations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("slug", sa.Text, nullable=False, unique=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("is_test", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_organizations_slug", "organizations", ["slug"], unique=True)

    # --- users -----------------------------------------------------------
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("email", sa.dialects.postgresql.CITEXT(), nullable=False, unique=True),
        sa.Column("display_name", sa.Text, nullable=False),
        sa.Column("password_hash", sa.Text, nullable=True),  # null when SSO-only
        sa.Column("is_active", sa.Boolean, nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_users_email", "users", ["email"], unique=True)

    # --- memberships -----------------------------------------------------
    op.create_table(
        "memberships",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("org_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("role", postgresql.ENUM("owner", "admin", "member", "viewer",
                                          name="org_role", create_type=False),
                  nullable=False, server_default="member"),
        sa.Column("invited_by", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint("org_id", "user_id", name="uq_memberships_org_user"),
    )
    op.create_index("ix_memberships_org", "memberships", ["org_id"])
    op.create_index("ix_memberships_user", "memberships", ["user_id"])

    # --- api_keys --------------------------------------------------------
    op.create_table(
        "api_keys",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("org_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("created_by", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("prefix", sa.Text, nullable=False),  # public, e.g. "eaip_live_xxxxxxxx"
        sa.Column("hash", sa.Text, nullable=False),    # argon2id of the key body
        sa.Column("scopes", postgresql.ARRAY(sa.Text), nullable=False,
                  server_default=sa.text("ARRAY[]::text[]")),
        sa.Column("status", postgresql.ENUM("active", "revoked",
                                            name="api_key_status", create_type=False),
                  nullable=False, server_default="active"),
        sa.Column("last_used_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_api_keys_org", "api_keys", ["org_id"])
    op.create_index("ix_api_keys_prefix", "api_keys", ["prefix"], unique=True)

    # --- refresh_tokens (hashed) ----------------------------------------
    op.create_table(
        "refresh_tokens",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("org_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("token_hash", sa.Text, nullable=False),
        sa.Column("issued_at", sa.TIMESTAMP(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("expires_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("rotated_to", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("user_agent", sa.Text, nullable=True),
        sa.Column("ip", sa.Text, nullable=True),
    )
    op.create_index("ix_refresh_tokens_user", "refresh_tokens", ["user_id"])
    op.create_index("ix_refresh_tokens_hash", "refresh_tokens", ["token_hash"], unique=True)

    # --- outbox ----------------------------------------------------------
    op.create_table(
        "outbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("topic", sa.Text, nullable=False),
        sa.Column("key", sa.Text, nullable=True),
        sa.Column("payload", postgresql.JSONB, nullable=False),
        sa.Column("headers", postgresql.JSONB, nullable=True),
        sa.Column("org_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("actor_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("event_type", sa.Text, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("published_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )

    # --- audit_log (partitioned by month) -------------------------------
    op.execute(
        """
        CREATE TABLE audit_log (
            id           UUID NOT NULL DEFAULT gen_random_uuid(),
            org_id       UUID NOT NULL,
            actor_user_id UUID,
            actor_kind   TEXT NOT NULL,        -- 'user' or 'api_key'
            action       TEXT NOT NULL,
            target_type  TEXT,
            target_id    TEXT,
            ip           TEXT,
            user_agent   TEXT,
            request_id   TEXT,
            trace_id     TEXT,
            attributes   JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at);
        """
    )
    # Pre-create three monthly partitions covering "this month" plus two
    # forward months, so a fresh deploy doesn't immediately hit a partition gap.
    op.execute(
        """
        DO $$
        DECLARE
          start_month DATE := date_trunc('month', now())::date;
          part_name TEXT;
          range_start DATE;
          range_end   DATE;
          i INT;
        BEGIN
          FOR i IN 0..2 LOOP
            range_start := start_month + (i || ' months')::interval;
            range_end   := range_start + interval '1 month';
            part_name   := 'audit_log_' || to_char(range_start, 'YYYY_MM');
            EXECUTE format(
              'CREATE TABLE IF NOT EXISTS %I PARTITION OF audit_log FOR VALUES FROM (%L) TO (%L)',
              part_name, range_start, range_end
            );
          END LOOP;
        END
        $$;
        """
    )
    op.create_index("ix_audit_log_org_created", "audit_log", ["org_id", "created_at"])

    # --- foreign key for memberships.invited_by (after users exists) ----
    op.create_foreign_key(
        "fk_memberships_invited_by_users",
        "memberships",
        "users",
        ["invited_by"],
        ["id"],
        ondelete="SET NULL",
    )

    # --- updated_at triggers --------------------------------------------
    op.execute(
        """
        CREATE OR REPLACE FUNCTION set_updated_at()
        RETURNS trigger AS $$
        BEGIN
          NEW.updated_at = now();
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    for tbl in ("organizations", "users"):
        op.execute(
            f"""
            CREATE TRIGGER trg_{tbl}_updated_at
            BEFORE UPDATE ON {tbl}
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
            """
        )

    # --- row level security ---------------------------------------------
    # Policy: a row is visible iff EITHER
    #   (a) ``app.current_org`` GUC matches the row's ``org_id`` (normal request path), OR
    #   (b) ``app.bypass_rls`` GUC is 'on' (auth flow before org context is known,
    #       and admin / migration tools — narrow, audited use only).
    # If neither applies, the policy excludes the row (fail-closed default).
    #
    # ``app.bypass_rls`` is ONLY set by ``app.infra.db.session_unscoped()`` and
    # by Alembic itself. Application request handlers never set it. Adding this
    # bypass is what allows /v1/auth/login to look up the user's memberships
    # before the org id is known, without weakening tenant isolation in the
    # request path.
    policy_using = (
        "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid "
        "OR current_setting('app.bypass_rls', true) = 'on'"
    )
    for tbl in TENANT_TABLES:
        op.execute(f"ALTER TABLE {tbl} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {tbl} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"""
            CREATE POLICY {tbl}_tenant_isolation ON {tbl}
              USING ({policy_using})
              WITH CHECK ({policy_using})
            """
        )
    op.execute("ALTER TABLE audit_log ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE audit_log FORCE ROW LEVEL SECURITY")
    op.execute(
        f"""
        CREATE POLICY audit_log_tenant_isolation ON audit_log
          USING ({policy_using})
          WITH CHECK ({policy_using})
        """
    )


def downgrade() -> None:
    # Drop in reverse dependency order. Most installations never run this in
    # production — left intact for completeness and local-dev convenience.
    for tbl in (*TENANT_TABLES, "audit_log"):
        op.execute(f"DROP POLICY IF EXISTS {tbl}_tenant_isolation ON {tbl}")
    op.execute("DROP TRIGGER IF EXISTS trg_organizations_updated_at ON organizations")
    op.execute("DROP TRIGGER IF EXISTS trg_users_updated_at ON users")
    op.execute("DROP FUNCTION IF EXISTS set_updated_at()")
    op.execute("DROP TABLE IF EXISTS audit_log CASCADE")
    op.drop_table("outbox")
    op.drop_table("refresh_tokens")
    op.drop_table("api_keys")
    op.drop_table("memberships")
    op.drop_table("users")
    op.drop_table("organizations")
    op.execute("DROP TYPE IF EXISTS api_key_status")
    op.execute("DROP TYPE IF EXISTS org_role")
