"""Durable, tenant-scoped Kafka consumer receipts."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, Text, Uuid, func, literal, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Mapped, mapped_column

from app.core.logging import get_logger
from app.infra.db import Base
from app.modules.identity.models import Organization

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)


class ConsumerInbox(Base):
    __tablename__ = "consumer_inbox"

    org_id: Mapped[UUID] = mapped_column(
        Uuid, ForeignKey("organizations.id", ondelete="CASCADE"), primary_key=True
    )
    consumer: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    claimed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


def parse_event_id(value: object) -> UUID:
    """Require the UUID sent on the wire, without invoking producer defaults."""
    if not isinstance(value, str):
        raise ValueError("Kafka event requires an event_id UUID")
    return UUID(value)


async def claim_event(
    session: AsyncSession, *, org_id: UUID, consumer: str, event_id: UUID
) -> bool:
    """Claim an event, or skip a duplicate or an event for a deleted tenant."""
    # The key-share lock prevents deletion between the existence check and
    # the receipt's foreign-key check. A retired tenant has no effects to apply.
    source = (
        select(Organization.id, literal(consumer, Text), literal(event_id, Uuid))
        .where(Organization.id == org_id)
        .with_for_update(read=True, key_share=True)
    )
    statement = (
        insert(ConsumerInbox)
        .from_select(["org_id", "consumer", "event_id"], source)
        .on_conflict_do_nothing(
            index_elements=[ConsumerInbox.org_id, ConsumerInbox.consumer, ConsumerInbox.event_id]
        )
        .returning(ConsumerInbox.event_id)
    )
    claimed = await session.scalar(statement) is not None
    if (
        not claimed
        and await session.scalar(select(Organization.id).where(Organization.id == org_id))
        is not None
    ):
        logger.info("kafka.consumer.duplicate", consumer=consumer)
    return claimed
