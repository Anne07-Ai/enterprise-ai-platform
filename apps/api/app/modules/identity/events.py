"""Domain events emitted to the audit topic via the transactional outbox.

Each event is a dict with a stable, versioned shape. The outbox publisher
ships these to ``audit.event.v1`` Kafka topic.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.infra.outbox import enqueue


async def emit_audit(
    session: AsyncSession,
    *,
    org_id: UUID | None,
    actor_user_id: UUID | None,
    actor_kind: str,
    action: str,
    target_type: str | None = None,
    target_id: str | None = None,
    request_id: str | None = None,
    trace_id: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> None:
    """Enqueue an audit event in the same transaction as the caller's mutations."""
    payload = {
        "schema_version": 1,
        "org_id": str(org_id) if org_id else None,
        "actor_user_id": str(actor_user_id) if actor_user_id else None,
        "actor_kind": actor_kind,
        "action": action,
        "target_type": target_type,
        "target_id": target_id,
        "request_id": request_id,
        "trace_id": trace_id,
        "attributes": attributes or {},
        "occurred_at": datetime.now(UTC).isoformat(),
    }
    await enqueue(
        session,
        topic=get_settings().kafka.audit_topic,
        payload=payload,
        key=str(org_id) if org_id else None,
        org_id=org_id,
        actor_user_id=actor_user_id,
        event_type=action,
    )
