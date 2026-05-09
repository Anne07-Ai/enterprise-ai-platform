"""aiokafka producer wrapper.

Keep one producer per process. Send is fire-and-await — we wait for the broker
ack so transactional outbox semantics are preserved at the publisher layer
(the request handler itself only writes to the DB outbox).
"""

from __future__ import annotations

import json
from typing import Any

from aiokafka import AIOKafkaProducer
from opentelemetry import propagate

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_producer: AIOKafkaProducer | None = None


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        settings = get_settings()
        _producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka.bootstrap_servers,
            client_id=settings.kafka.client_id,
            enable_idempotence=settings.kafka.enable_idempotence,
            acks="all",
            compression_type=None,
            request_timeout_ms=settings.kafka.request_timeout_ms,
            value_serializer=lambda v: json.dumps(v, default=str, separators=(",", ":")).encode(),
            key_serializer=lambda k: k.encode() if isinstance(k, str) else k,
        )
        await _producer.start()
        logger.info("kafka.producer.started", servers=settings.kafka.bootstrap_servers)
    return _producer


async def dispose_producer() -> None:
    """Dispose kafka producer. Best-effort during loop teardown."""
    global _producer
    if _producer is not None:
        try:
            await _producer.stop()
            logger.info("kafka.producer.stopped")
        except (RuntimeError, Exception) as e:  # noqa: BLE001
            logger.debug("kafka.producer.dispose_suppressed", error=str(e))
    _producer = None


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
