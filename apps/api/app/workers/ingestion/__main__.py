"""Ingestion worker entrypoint.

Run with:
    uv run python -m app.workers.ingestion

Subscribes to document.uploaded.v1 as the 'ingestion-worker' consumer
group. For each event:
    1. Open a fresh AsyncSession with RLS configured.
    2. Call handle_document_uploaded inside that transaction.
    3. Commit the transaction.
    4. Commit the Kafka offset.

Why manual offset commit (enable_auto_commit=False):
    Auto-commit ships offsets on a timer regardless of whether the
    handler succeeded. We commit only after successful processing so
    a crash mid-handler causes Kafka to re-deliver the message — at
    which point the handler's status and uniqueness guards reduce
    repeated effects. They do not provide durable event deduplication.

Why one consumer group:
    Multiple replicas of this worker can run side-by-side; Kafka will
    rebalance partitions across them automatically. A crash between the
    database commit and the offset commit can still cause re-delivery.

Graceful shutdown:
    SIGTERM / SIGINT triggers the stop event. The loop finishes the
    current record, commits its offset, and exits cleanly.
"""

from __future__ import annotations

import asyncio
import json
import signal
import sys
from typing import TYPE_CHECKING, Any

from app.core.config import get_settings
from app.core.logging import get_logger, setup_logging
from app.infra import kafka as kafka_infra
from app.infra import outbox as outbox_infra
from app.infra.db import dispose_engine, get_sessionmaker
from app.infra.kafka_offsets import OffsetOwnedConsumer
from app.infra.kafka_shutdown import stop_consumer
from app.modules.rag.events import DocumentUploadedV1
from app.modules.rag.storage import get_storage
from app.workers import run_until_stopped

if TYPE_CHECKING:
    from contextlib import AsyncExitStack

logger = get_logger(__name__)

CONSUMER_GROUP = "ingestion-worker"


async def _process_one(record_value: dict[str, Any]) -> None:
    """Handle one event in its own DB transaction."""
    # The handler loads tokenizer assets. Defer that until actual ingestion.
    from app.workers.ingestion.handler import handle_document_uploaded

    storage = get_storage()
    sm = get_sessionmaker()
    async with sm() as session, session.begin():
        await handle_document_uploaded(session, storage, event_payload=record_value)


async def run(stop_event: asyncio.Event) -> None:
    await run_until_stopped(_consume, stop_event)


async def _consume(stop_event: asyncio.Event, stack: AsyncExitStack) -> None:
    settings = get_settings()
    logger.info("ingestion.starting", group=CONSUMER_GROUP)

    # Register cleanup before startup so a partial failure releases resources.
    stack.push_async_callback(dispose_engine)
    stack.push_async_callback(kafka_infra.dispose_producer)
    await kafka_infra.get_producer()
    if stop_event.is_set():
        return
    stack.push_async_callback(outbox_infra.stop_publisher)
    await outbox_infra.start_publisher()

    consumer = OffsetOwnedConsumer(
        DocumentUploadedV1.TOPIC,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=CONSUMER_GROUP,
        client_id=f"{CONSUMER_GROUP}-{settings.kafka.client_id}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_interval_ms=10 * 60 * 1000,  # 10 min — chunking can be slow
        value_deserializer=lambda v: json.loads(v.decode()),
    )
    stack.push_async_callback(stop_consumer, consumer)
    await consumer.start()
    logger.info("ingestion.consumer.started")

    while not stop_event.is_set():
        try:
            batch = await asyncio.wait_for(
                consumer.getmany(timeout_ms=1000, max_records=10),
                timeout=2.0,
            )
        except TimeoutError:
            continue

        if not batch:
            continue

        for tp, records in batch.items():
            for record in records:
                if stop_event.is_set():
                    return
                logger.info(
                    "ingestion.message.received",
                    partition=tp.partition,
                    offset=record.offset,
                )
                stage = "processing"
                try:
                    await _process_one(record.value)
                    # Kafka expects the next offset for this partition. The fetch
                    # position may already include unprocessed records.
                    stage = "offset_commit"
                    await consumer.commit({tp: record.offset + 1})
                except Exception as exc:
                    logger.error(
                        "ingestion.message.failed",
                        stage=stage,
                        partition=tp.partition,
                        offset=record.offset,
                        error_type=type(exc).__name__,
                    )
                    # Exit so a restarted worker resumes from the last committed
                    # offset instead of passing the failed record.
                    raise


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event) -> None:
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)


async def amain() -> int:
    setup_logging(get_settings())
    stop_event = asyncio.Event()
    _install_signal_handlers(asyncio.get_running_loop(), stop_event)
    try:
        await run(stop_event)
    except Exception as exc:
        logger.error("ingestion.fatal", error_type=type(exc).__name__)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(amain()))
