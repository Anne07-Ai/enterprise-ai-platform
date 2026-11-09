"""Add tenant-scoped receipts for Kafka consumer idempotency."""

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "consumer_inbox",
        sa.Column(
            "org_id",
            sa.Uuid(),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("consumer", sa.Text(), primary_key=True),
        sa.Column("event_id", sa.Uuid(), primary_key=True),
        sa.Column(
            "claimed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
    )
    op.execute("ALTER TABLE consumer_inbox ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE consumer_inbox FORCE ROW LEVEL SECURITY")
    op.execute(
        """
        CREATE POLICY consumer_inbox_tenant_isolation ON consumer_inbox
          USING (org_id = NULLIF(current_setting('app.current_org', true), '')::uuid)
          WITH CHECK (org_id = NULLIF(current_setting('app.current_org', true), '')::uuid)
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Consumer inbox downgrade requires an approved receipt preservation plan"
    )
