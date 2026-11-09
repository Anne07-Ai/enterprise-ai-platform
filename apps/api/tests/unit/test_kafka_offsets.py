"""Verify ownership of the SDK's shielded offset request without network calls."""

from __future__ import annotations

import asyncio
import gc
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.consumer.group_coordinator import GroupCoordinator
from aiokafka.errors import ConsumerStoppedError
from app.infra import kafka_offsets
from app.infra.kafka_offsets import OffsetOwnedConsumer

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

pytestmark = pytest.mark.unit


async def _start_consumer(
    monkeypatch: pytest.MonkeyPatch,
    send: Callable[[object, object], Awaitable[None]],
) -> OffsetOwnedConsumer:
    async def start(consumer: AIOKafkaConsumer) -> None:
        consumer._coordinator = SimpleNamespace(
            ensure_coordinator_known=AsyncMock(),
            _commit_lock=asyncio.Lock(),
            _do_commit_offsets=send,
        )

    monkeypatch.setattr(AIOKafkaConsumer, "start", start)
    consumer = OffsetOwnedConsumer(group_id="offset-ownership-test")
    await consumer.start()
    return consumer


async def _cancel_commit_waiter(consumer: OffsetOwnedConsumer, started: asyncio.Event) -> None:
    waiter = asyncio.create_task(GroupCoordinator.commit_offsets(consumer._coordinator, None, {}))
    await started.wait()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter


async def test_cancelled_commit_joins_connection_open_before_client_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started, release = asyncio.Event(), asyncio.Event()
    closed = asyncio.get_running_loop().create_future()
    closed.set_result(None)
    transport = SimpleNamespace(close=Mock(return_value=closed))
    connect_tasks: list[asyncio.Task[object]] = []

    async def create_connection(*args: object, **kwargs: object) -> object:
        task = asyncio.current_task()
        assert task is not None
        connect_tasks.append(task)
        started.set()
        await release.wait()
        return transport

    async def send(*args: object) -> None:
        await consumer._client._get_conn(0)

    consumer = await _start_consumer(monkeypatch, send)
    monkeypatch.setattr("aiokafka.client.create_conn", create_connection)
    monkeypatch.setattr(
        consumer._client.cluster,
        "broker_metadata",
        Mock(return_value=SimpleNamespace(host="127.0.0.1", port=1)),
    )
    try:
        async with asyncio.timeout(1):
            await _cancel_commit_waiter(consumer, started)
            assert not connect_tasks[0].done()  # The SDK shield is active.
            await consumer.stop_offset_commits(0.5)
            assert connect_tasks[0].cancelled()
            await consumer._client.close()
            release.set()
            assert not consumer._client._conns
            with pytest.raises(ConsumerStoppedError):
                await GroupCoordinator.commit_offsets(consumer._coordinator, None, {})
    finally:
        release.set()
        await asyncio.gather(*connect_tasks, return_exceptions=True)
        await consumer._client.close()
        consumer._closed = True


async def test_offset_cleanup_timeout_reports_request_that_delays_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started, finish_cancel = asyncio.Event(), asyncio.Event()

    async def send(*args: object) -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await finish_cancel.wait()
            raise

    consumer = await _start_consumer(monkeypatch, send)
    try:
        async with asyncio.timeout(1):
            await _cancel_commit_waiter(consumer, started)
            with pytest.raises(TimeoutError, match="Kafka offset requests"):
                await consumer.stop_offset_commits(0.02)
            assert len(consumer._offset_requests) == 1
            finish_cancel.set()
            await consumer.stop_offset_commits(0.5)
            assert not consumer._offset_requests
    finally:
        finish_cancel.set()
        await consumer.stop_offset_commits(0.5)
        await consumer._client.close()
        consumer._closed = True


@pytest.mark.parametrize("cancelled_waiter", [False, True])
async def test_offset_request_observes_failure_after_waiter_is_cancelled(
    monkeypatch: pytest.MonkeyPatch, cancelled_waiter: bool
) -> None:
    started, release, finished = (asyncio.Event() for _ in range(3))

    async def send(*args: object) -> None:
        started.set()
        await release.wait()
        raise RuntimeError("injected offset request failure")

    consumer = await _start_consumer(monkeypatch, send)
    loop = asyncio.get_running_loop()
    prior_handler = loop.get_exception_handler()
    errors: list[dict[str, object]] = []
    loop.set_exception_handler(lambda _, context: errors.append(context))
    try:
        async with asyncio.timeout(1):
            if cancelled_waiter:
                await _cancel_commit_waiter(consumer, started)
                next(iter(consumer._offset_requests)).add_done_callback(lambda _: finished.set())
                release.set()
                await finished.wait()
                gc.collect()
                assert errors == []
            else:
                release.set()
                with pytest.raises(RuntimeError, match="injected offset request failure"):
                    await GroupCoordinator.commit_offsets(consumer._coordinator, None, {})
    finally:
        release.set()
        await consumer.stop_offset_commits(0.5)
        await consumer._client.close()
        consumer._closed = True
        loop.set_exception_handler(prior_handler)


async def test_rejects_unverified_sdk_and_auto_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="manual commits"):
        OffsetOwnedConsumer(group_id="offset-ownership-test", enable_auto_commit=True)
    monkeypatch.setattr(kafka_offsets, "__version__", "0.15.0")
    with pytest.raises(RuntimeError, match="requires verification"):
        OffsetOwnedConsumer(group_id="offset-ownership-test")
