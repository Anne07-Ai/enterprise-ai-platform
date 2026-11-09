"""Exercise Kafka shutdown without a live broker."""

from __future__ import annotations

import asyncio
import gc
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from aiokafka import TopicPartition
from aiokafka.errors import ProducerClosed
from aiokafka.producer.sender import Sender
from app.core.config import KafkaSettings, get_settings
from app.infra import outbox
from app.infra.kafka_shutdown import ShutdownProducer, stop_consumer
from pydantic import ValidationError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def short_shutdown_deadlines(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = get_settings().kafka
    monkeypatch.setattr(settings, "shutdown_grace_seconds", 0.02)
    monkeypatch.setattr(settings, "shutdown_cleanup_seconds", 0.02)


@pytest.mark.parametrize("invalid", [0, -1, float("inf"), float("nan")])
def test_shutdown_deadlines_are_positive_and_finite(invalid: float) -> None:
    with pytest.raises(ValidationError):
        KafkaSettings(shutdown_grace_seconds=invalid)
    with pytest.raises(ValidationError):
        KafkaSettings(shutdown_cleanup_seconds=invalid)


async def test_producer_joins_requests_and_settles_undelivered_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    original_handler = loop.get_exception_handler()
    exceptions: list[object] = []
    loop.set_exception_handler(lambda _, context: exceptions.append(context))
    started = asyncio.Event()

    async def stalled_request(*_args: object) -> None:
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(Sender, "_send_produce_req", stalled_request)
    producer = ShutdownProducer(enable_idempotence=True)
    client_close = AsyncMock()
    monkeypatch.setattr(producer.client, "close", client_close)
    try:
        async with asyncio.timeout(1):
            delivery = await producer._message_accumulator.add_message(
                TopicPartition("shutdown-test", 0), b"key", b"payload", 0.1
            )
            request = asyncio.create_task(producer._sender._send_produce_req(0, {}))
            await started.wait()
            await producer.stop()
        assert request.cancelled()
        assert not producer._sender._requests
        client_close.assert_awaited_once()
        with pytest.raises(ProducerClosed):
            await delivery

        del producer, delivery
        gc.collect()
        assert exceptions == []
    finally:
        loop.set_exception_handler(original_handler)


async def test_publisher_deadline_cancels_send_and_awaits_rollback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created_at = datetime.now(UTC)
    rows = [
        SimpleNamespace(
            id=uuid4(), payload={"event": event}, topic="events", key="same", created_at=created_at
        )
        for event in (1, 2)
    ]
    sending, rolled_back = asyncio.Event(), asyncio.Event()
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=[SimpleNamespace(all=lambda: rows), None])
    )

    async def transaction() -> AsyncIterator[object]:
        try:
            yield session
        except asyncio.CancelledError:
            await asyncio.sleep(0)
            rolled_back.set()
            raise

    async def send(_topic: str, payload: dict[str, int], *, key: str) -> None:
        if payload["event"] == 2:
            sending.set()
            await asyncio.Event().wait()

    monkeypatch.setattr(outbox, "session_unscoped", transaction)
    monkeypatch.setattr(outbox.kafka_infra, "send", send)

    async with asyncio.timeout(1):
        await outbox.start_publisher()
        try:
            await sending.wait()
        finally:
            await outbox.stop_publisher()

    assert rolled_back.is_set()
    assert session.execute.await_count == 2  # SELECT plus first ack's uncommitted UPDATE
    assert outbox._PUBLISHER_TASK is None
    assert outbox._PUBLISHER_STOP is None


@pytest.mark.parametrize("stop_mode", ["deadline", "cancelled_child", "external_cancel"])
async def test_consumer_stop_still_closes_local_tasks_and_transport(stop_mode: str) -> None:
    async def stall() -> None:
        await asyncio.Event().wait()

    tasks = [asyncio.create_task(stall()) for _ in range(5)]
    started = asyncio.Event()

    async def stop() -> None:
        started.set()
        if stop_mode == "cancelled_child":
            tasks[3].cancel()
            await tasks[3]
        await stall()

    consumer = SimpleNamespace(
        stop=AsyncMock(side_effect=stop),
        _coordinator=SimpleNamespace(
            _coordination_task=tasks[0], _heartbeat_task=tasks[1], _commit_refresh_task=tasks[2]
        ),
        _fetcher=SimpleNamespace(
            _closed=True, _fetch_task=tasks[3], _pending_tasks=[tasks[4]], close=AsyncMock()
        ),
        _client=SimpleNamespace(close=AsyncMock()),
    )
    try:
        async with asyncio.timeout(1):
            shutdown = asyncio.create_task(stop_consumer(consumer))
            await started.wait()
            if stop_mode == "external_cancel":
                shutdown.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await shutdown
            else:
                await shutdown
        assert all(task.cancelled() for task in tasks)
        consumer._fetcher.close.assert_not_awaited()
        consumer._client.close.assert_awaited_once()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_cancelling_stop_caller_does_not_interrupt_publisher_rollback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sending, rollback_started, finish_rollback = (asyncio.Event() for _ in range(3))
    rolled_back = asyncio.Event()

    async def publish() -> int:
        sending.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            rollback_started.set()
            await finish_rollback.wait()
            rolled_back.set()
            raise
        return 0

    monkeypatch.setattr(outbox, "_publish_batch", publish)
    async with asyncio.timeout(1):
        await outbox.start_publisher()
        await sending.wait()
        shutdown = asyncio.create_task(outbox.stop_publisher())
        try:
            await rollback_started.wait()
            shutdown.cancel()
            await asyncio.sleep(0)
            assert not shutdown.done()
        finally:
            finish_rollback.set()
            with pytest.raises(asyncio.CancelledError):
                await shutdown

    assert rolled_back.is_set()
    assert outbox._PUBLISHER_TASK is None
