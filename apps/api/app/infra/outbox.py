"""Transactional outbox.

Request handlers write to the ``outbox`` table inside the SAME transaction
that mutates application state. A background publisher polls the outbox,
publishes to Kafka, and marks rows as published. This guarantees we never
dual-write from the request handler. A rollback of that transaction removes
the outbox row. Publication is at least once: if Kafka acknowledges a send
before the publisher's DB transaction fails, the same event may be sent again.

A future migration can switch the publisher implementation to Debezium CDC
without changing the producer-side code.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from sqlalchemy import text

from app.core.config import get_settings
from app.core.logging import get_logger
from app.infra import kafka as kafka_infra
from app.infra.db import session_unscoped

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


logger = get_logger(__name__)

_PUBLISHER_TASK: asyncio.Task[None] | None = None
_PUBLISHER_STOP: asyncio.Event | None = None


async def enqueue(
    session: AsyncSession,
    *,
    topic: str,
    payload: dict[str, Any],
    key: str | None = None,
    org_id: UUID | None = None,
    actor_user_id: UUID | None = None,
    event_type: str | None = None,
) -> UUID:
    """Insert an outbox row inside the caller's transaction.

    The publisher loop will pick it up and ship it to Kafka.
    """
    event_id = uuid4()
    # Serialize the payload to JSON text and cast inside SQL — works under
    # asyncpg without needing a custom column type binding.
    await session.execute(
        text(
            """
            INSERT INTO outbox
                (id, topic, key, payload, org_id, actor_user_id, event_type, created_at)
            VALUES
                (:id, :topic, :key, CAST(:payload AS JSONB),
                 :org_id, :actor_user_id, :event_type, :created_at)
            """
        ),
        {
            "id": event_id,
            "topic": topic,
            "key": key,
            "payload": json.dumps(payload, default=str),
            "org_id": org_id,
            "actor_user_id": actor_user_id,
            "event_type": event_type,
            "created_at": datetime.now(UTC),
        },
    )
    return event_id


async def _publish_batch(batch_size: int = 100) -> int:
    """Publish up to ``batch_size`` outbox rows. Returns the number published."""
    published = 0
    oldest_created_at: datetime | None = None
    # The internal publisher scans all tenants, including system events
    # without an org. Keep its RLS bypass local to this batch transaction.
    async with contextlib.asynccontextmanager(session_unscoped)() as session:
        # SELECT FOR UPDATE SKIP LOCKED is the canonical worker pattern: many
        # publishers can run concurrently without stepping on each other.
        rows = (
            await session.execute(
                text(
                    """
                        SELECT id, topic, key, payload, created_at
                          FROM outbox
                         WHERE published_at IS NULL
                         ORDER BY created_at
                         LIMIT :n
                           FOR UPDATE SKIP LOCKED
                        """
                ),
                {"n": batch_size},
            )
        ).all()

        for row in rows:
            payload = row.payload if isinstance(row.payload, dict) else dict(row.payload)
            try:
                await kafka_infra.send(row.topic, payload, key=row.key)
            except Exception as exc:  # pragma: no cover — broker hiccup, retry next tick
                logger.warning("outbox.publish.failed", error_type=type(exc).__name__)
                continue

            await session.execute(
                text("UPDATE outbox SET published_at = now() WHERE id = :id"),
                {"id": row.id},
            )
            published += 1
            if oldest_created_at is None or row.created_at < oldest_created_at:
                oldest_created_at = row.created_at
    if oldest_created_at is not None:
        logger.info(
            "outbox.publish.batch",
            count=published,
            oldest_published_age_seconds=max(
                0.0, (datetime.now(UTC) - oldest_created_at).total_seconds()
            ),
        )
    return published


async def _publisher_loop(stop: asyncio.Event, *, interval_seconds: float = 1.0) -> None:
    logger.info("outbox.publisher.started")
    while not stop.is_set():
        try:
            published = await _publish_batch()
        except Exception as exc:  # pragma: no cover — keep loop alive
            logger.error("outbox.publisher.error", error_type=type(exc).__name__)
            published = 0
        # Backoff a little when the queue was empty.
        if published == 0:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
    logger.info("outbox.publisher.stopped")


async def start_publisher() -> None:
    global _PUBLISHER_TASK, _PUBLISHER_STOP
    if _PUBLISHER_TASK is not None:
        return
    _PUBLISHER_STOP = asyncio.Event()
    _PUBLISHER_TASK = asyncio.create_task(_publisher_loop(_PUBLISHER_STOP), name="outbox-publisher")


async def stop_publisher() -> None:
    global _PUBLISHER_TASK, _PUBLISHER_STOP
    task, stop = _PUBLISHER_TASK, _PUBLISHER_STOP
    if task is None or stop is None:
        return

    stop.set()
    joined = asyncio.gather(task, return_exceptions=True)
    try:
        _, pending = await asyncio.wait(
            (task,), timeout=get_settings().kafka.shutdown_grace_seconds
        )
        if pending:
            logger.warning("outbox.publisher.shutdown_deadline_exceeded")
            if not task.cancelling():
                task.cancel()
        # Cancellation must finish the transaction rollback before producer
        # disposal. Kafka may have accepted a record whose ack never arrived.
        await asyncio.shield(joined)
        if not task.cancelled():
            task.result()
    except asyncio.CancelledError:
        if not task.done() and not task.cancelling():
            task.cancel()
        await joined
        raise
    finally:
        if _PUBLISHER_TASK is task and task.done():
            _PUBLISHER_TASK = _PUBLISHER_STOP = None
