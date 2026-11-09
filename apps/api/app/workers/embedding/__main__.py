"""Embedding worker entrypoint.

Run with:
    uv run python -m app.workers.embedding

Subscribes to document.chunked.v1 as the 'embedding-worker' consumer
group. For each event, embeds the chunk text via OpenAI, UPDATEs the
chunk row, and (if all chunks for the document are now embedded) marks
the document ready.

The embedding provider is built once per process. The httpx client
inside it is reused across requests for connection pooling. We close
it on shutdown.

Otherwise the structure mirrors the ingestion worker:
    * enable_auto_commit=False — commit after success only
    * single consumer group — replicas auto-balance partitions
    * SIGTERM-aware graceful shutdown
    * outbox publisher running so document.ready.v1 events flow
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
from app.modules.rag.embeddings import EmbeddingProvider, build_default_provider
from app.modules.rag.events import DocumentChunkedV1
from app.workers import close_worker_dependency, run_until_stopped
from app.workers.embedding.handler import handle_document_chunked

if TYPE_CHECKING:
    from contextlib import AsyncExitStack

logger = get_logger(__name__)

CONSUMER_GROUP = "embedding-worker"


async def _process_one(embedder: EmbeddingProvider, record_value: dict[str, Any]) -> None:
    """Handle one event in its own DB transaction."""
    sm = get_sessionmaker()
    async with sm() as session, session.begin():
        await handle_document_chunked(session, embedder, event_payload=record_value)


async def run(stop_event: asyncio.Event) -> None:
    await run_until_stopped(_consume, stop_event)


async def _consume(stop_event: asyncio.Event, stack: AsyncExitStack) -> None:
    settings = get_settings()
    logger.info("embedding.starting", group=CONSUMER_GROUP)

    # Register cleanup before startup so a partial failure releases resources.
    stack.push_async_callback(dispose_engine)
    stack.push_async_callback(kafka_infra.dispose_producer)
    await kafka_infra.get_producer()
    if stop_event.is_set():
        return
    stack.push_async_callback(outbox_infra.stop_publisher)
    await outbox_infra.start_publisher()
    embedder = build_default_provider()
    stack.push_async_callback(close_worker_dependency, embedder.aclose)

    consumer = OffsetOwnedConsumer(
        DocumentChunkedV1.TOPIC,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=CONSUMER_GROUP,
        client_id=f"{CONSUMER_GROUP}-{settings.kafka.client_id}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_interval_ms=5 * 60 * 1000,  # 5 min — OpenAI calls can be slow
        value_deserializer=lambda v: json.loads(v.decode()),
    )
    stack.push_async_callback(stop_consumer, consumer)
    await consumer.start()
    logger.info("embedding.consumer.started")

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
                    "embedding.message.received",
                    partition=tp.partition,
                    offset=record.offset,
                )
                stage = "processing"
                try:
                    await _process_one(embedder, record.value)
                    # Kafka expects the next offset for this partition. The fetch
                    # position may already include unprocessed records.
                    stage = "offset_commit"
                    await consumer.commit({tp: record.offset + 1})
                except Exception as exc:
                    logger.error(
                        "embedding.message.failed",
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
        logger.error("embedding.fatal", error_type=type(exc).__name__)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(amain()))
