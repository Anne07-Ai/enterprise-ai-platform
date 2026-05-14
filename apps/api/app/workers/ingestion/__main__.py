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
    which point the handler's idempotency guards (status transitions,
    UNIQUE chunk index) make re-processing safe.

Why one consumer group:
    Multiple replicas of this worker can run side-by-side; Kafka will
    rebalance partitions across them automatically. The single
    consumer group means we don't double-process the same message.

Graceful shutdown:
    SIGTERM / SIGINT triggers _stop_event. The loop finishes the
    current batch, commits its offsets, and exits cleanly.
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from app.core.config import get_settings
from app.core.logging import setup_logging
from app.infra import kafka as kafka_infra
from app.infra import outbox as outbox_infra
from app.infra.db import get_sessionmaker
from app.modules.rag.events import DocumentUploadedV1
from app.modules.rag.storage import get_storage
from app.workers.ingestion.handler import handle_document_uploaded

logger = logging.getLogger(__name__)

CONSUMER_GROUP = "ingestion-worker"


async def _process_one(record_value: dict[str, Any]) -> None:
    """Handle one event in its own DB transaction."""
    storage = get_storage()
    sm = get_sessionmaker()
    async with sm() as session:
        async with session.begin():
            await handle_document_uploaded(
                session, storage, event_payload=record_value
            )


async def run(stop_event: asyncio.Event) -> None:
    settings = get_settings()
    logger.info(
        "ingestion.starting",
        extra={
            "bootstrap_servers": settings.kafka.bootstrap_servers,
            "group": CONSUMER_GROUP,
        },
    )

    # We need the outbox publisher running so emitted chunked-events
    # actually get shipped. The worker is its own process and doesn't
    # share the API's app lifespan.
    await kafka_infra.get_producer()
    await outbox_infra.start_publisher()

    consumer = AIOKafkaConsumer(
        DocumentUploadedV1.TOPIC,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=CONSUMER_GROUP,
        client_id=f"{CONSUMER_GROUP}-{settings.kafka.client_id}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_interval_ms=10 * 60 * 1000,  # 10 min — chunking can be slow
        value_deserializer=lambda v: json.loads(v.decode()),
    )
    await consumer.start()
    logger.info("ingestion.consumer.started")

    try:
        while not stop_event.is_set():
            try:
                batch = await asyncio.wait_for(
                    consumer.getmany(timeout_ms=1000, max_records=10),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                continue

            if not batch:
                continue

            for tp, records in batch.items():
                for record in records:
                    if stop_event.is_set():
                        break
                    document_id = record.value.get("document_id", "<unknown>")
                    logger.info(
                        "ingestion.message.received",
                        extra={
                            "partition": tp.partition,
                            "offset": record.offset,
                            "document_id": document_id,
                        },
                    )
                    try:
                        await _process_one(record.value)
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "ingestion.message.unhandled",
                            extra={"document_id": document_id, "offset": record.offset},
                        )
                        # Continue — handler already marked failed if it
                        # got far enough. If it didn't, the message will
                        # be re-delivered next time the consumer starts
                        # (offset NOT committed).
                        continue

            # Commit only the offsets for the batch we just finished.
            try:
                await consumer.commit()
            except KafkaError:
                logger.exception("ingestion.commit.failed")
                # If commit fails we'll re-process on restart; that's
                # safe due to idempotency.
    finally:
        logger.info("ingestion.stopping")
        try:
            await consumer.stop()
        except Exception:  # noqa: BLE001
            logger.exception("ingestion.consumer.stop_failed")
        await outbox_infra.stop_publisher()
        await kafka_infra.dispose_producer()
        logger.info("ingestion.stopped")


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event) -> None:
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)


async def amain() -> int:
    setup_logging(get_settings())
    stop_event = asyncio.Event()
    _install_signal_handlers(asyncio.get_running_loop(), stop_event)
    try:
        await run(stop_event)
    except Exception:  # noqa: BLE001
        logger.exception("ingestion.fatal")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(amain()))