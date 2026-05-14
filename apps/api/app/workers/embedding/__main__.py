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
from app.modules.rag.embeddings import EmbeddingProvider, build_default_provider
from app.modules.rag.events import DocumentChunkedV1
from app.workers.embedding.handler import handle_document_chunked

logger = logging.getLogger(__name__)

CONSUMER_GROUP = "embedding-worker"


async def _process_one(embedder: EmbeddingProvider, record_value: dict[str, Any]) -> None:
    """Handle one event in its own DB transaction."""
    sm = get_sessionmaker()
    async with sm() as session:
        async with session.begin():
            await handle_document_chunked(
                session, embedder, event_payload=record_value
            )


async def run(stop_event: asyncio.Event) -> None:
    settings = get_settings()
    logger.info(
        "embedding.starting",
        extra={
            "bootstrap_servers": settings.kafka.bootstrap_servers,
            "group": CONSUMER_GROUP,
        },
    )

    await kafka_infra.get_producer()
    await outbox_infra.start_publisher()
    embedder = build_default_provider()

    consumer = AIOKafkaConsumer(
        DocumentChunkedV1.TOPIC,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=CONSUMER_GROUP,
        client_id=f"{CONSUMER_GROUP}-{settings.kafka.client_id}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_interval_ms=5 * 60 * 1000,  # 5 min — OpenAI calls can be slow
        value_deserializer=lambda v: json.loads(v.decode()),
    )
    await consumer.start()
    logger.info("embedding.consumer.started")

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
                    chunk_id = record.value.get("chunk_id", "<unknown>")
                    document_id = record.value.get("document_id", "<unknown>")
                    logger.info(
                        "embedding.message.received",
                        extra={
                            "partition": tp.partition,
                            "offset": record.offset,
                            "chunk_id": chunk_id,
                            "document_id": document_id,
                        },
                    )
                    try:
                        await _process_one(embedder, record.value)
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "embedding.message.unhandled",
                            extra={
                                "chunk_id": chunk_id,
                                "document_id": document_id,
                                "offset": record.offset,
                            },
                        )
                        # Don't commit — message will redeliver.
                        continue

            try:
                await consumer.commit()
            except KafkaError:
                logger.exception("embedding.commit.failed")
    finally:
        logger.info("embedding.stopping")
        try:
            await consumer.stop()
        except Exception:  # noqa: BLE001
            logger.exception("embedding.consumer.stop_failed")
        try:
            await embedder.aclose()
        except Exception:  # noqa: BLE001
            logger.exception("embedding.embedder.aclose_failed")
        await outbox_infra.stop_publisher()
        await kafka_infra.dispose_producer()
        logger.info("embedding.stopped")


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
        logger.exception("embedding.fatal")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(amain()))
    