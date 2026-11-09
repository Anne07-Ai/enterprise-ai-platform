"""aiokafka producer wrapper.

Keep one producer per process. Send is fire-and-await — we wait for the broker
ack so transactional outbox semantics are preserved at the publisher layer
(the request handler itself only writes to the DB outbox).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from aiokafka import AIOKafkaProducer
from opentelemetry import propagate

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_producer: AIOKafkaProducer | None = None
_producer_lock: asyncio.Lock | None = None
_producer_loop: asyncio.AbstractEventLoop | None = None


def _lifecycle_lock() -> asyncio.Lock:
    global _producer_lock, _producer_loop
    loop = asyncio.get_running_loop()
    if _producer_lock is None or _producer_loop is not loop:
        if _producer is not None or (_producer_lock is not None and _producer_lock.locked()):
            raise RuntimeError("Kafka producer lifecycle belongs to another event loop")
        _producer_lock = asyncio.Lock()
        _producer_loop = loop
    return _producer_lock


async def _stop_producer(producer: AIOKafkaProducer) -> None:
    try:
        await producer.stop()
        logger.info("kafka.producer.stopped")
    except Exception:
        logger.warning("kafka.producer.stop_failed")


async def get_producer() -> AIOKafkaProducer:
    global _producer
    async with _lifecycle_lock():
        if _producer is not None:
            return _producer
        settings = get_settings()
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka.bootstrap_servers,
            client_id=settings.kafka.client_id,
            enable_idempotence=settings.kafka.enable_idempotence,
            acks="all",
            compression_type=None,
            request_timeout_ms=settings.kafka.request_timeout_ms,
            value_serializer=lambda v: json.dumps(v, default=str, separators=(",", ":")).encode(),
            key_serializer=lambda k: k.encode() if isinstance(k, str) else k,
        )
        try:
            await producer.start()
        except (Exception, asyncio.CancelledError):
            await _stop_producer(producer)
            raise
        _producer = producer
        logger.info("kafka.producer.started")
        return producer


async def dispose_producer() -> None:
    """Dispose kafka producer. Best-effort during loop teardown."""
    global _producer
    async with _lifecycle_lock():
        producer, _producer = _producer, None
        if producer is not None:
            await _stop_producer(producer)


def _trace_headers() -> list[tuple[str, bytes]]:
    """Inject the current OTel context as Kafka headers for trace continuity."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return [(k, v.encode()) for k, v in carrier.items()]


async def send(
    topic: str,
    value: dict[str, Any],
    *,
    key: str | None = None,
    headers: list[tuple[str, bytes]] | None = None,
) -> None:
    """Send a single record. Awaits broker ack (acks=all)."""
    producer = await get_producer()
    all_headers = _trace_headers() + (headers or [])
    await producer.send_and_wait(topic, value=value, key=key, headers=all_headers)


async def ping() -> None:
    """Confirm we can reach a broker — readiness probe."""
    producer = await get_producer()
    # client.fetch_all_metadata() refreshes cluster metadata from any broker.
    await producer.client.fetch_all_metadata()
