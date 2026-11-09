"""Regression tests for manual offsets and worker lifecycle cleanup."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, call

import pytest
from aiokafka import TopicPartition
from app.workers.embedding import __main__ as embedding
from app.workers.ingestion import __main__ as ingestion

if TYPE_CHECKING:
    from types import ModuleType

pytestmark = pytest.mark.unit


@pytest.fixture(params=[ingestion, embedding])
def worker(request: pytest.FixtureRequest) -> ModuleType:
    return request.param


@pytest.fixture
def worker_io(worker: ModuleType, monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    io = SimpleNamespace(
        start=AsyncMock(),
        getmany=AsyncMock(),
        commit=AsyncMock(),
        stop=AsyncMock(),
        stop_offset_commits=AsyncMock(),
        embedder_close=AsyncMock(),
        outbox_stop=AsyncMock(),
        producer_close=AsyncMock(),
        engine_close=AsyncMock(),
        process=AsyncMock(),
    )
    monkeypatch.setattr(worker, "OffsetOwnedConsumer", lambda *args, **kwargs: io)
    monkeypatch.setattr(worker.kafka_infra, "get_producer", AsyncMock())
    monkeypatch.setattr(worker.kafka_infra, "dispose_producer", io.producer_close)
    monkeypatch.setattr(worker.outbox_infra, "start_publisher", AsyncMock())
    monkeypatch.setattr(worker.outbox_infra, "stop_publisher", io.outbox_stop)
    monkeypatch.setattr(worker, "dispose_engine", io.engine_close)
    monkeypatch.setattr(worker, "_process_one", io.process)
    if worker is embedding:
        monkeypatch.setattr(
            worker, "build_default_provider", lambda: SimpleNamespace(aclose=io.embedder_close)
        )
    return io


@pytest.mark.parametrize("failure", ["process", "commit", "shutdown"])
async def test_commit_stops_at_last_successful_record(
    worker: ModuleType, worker_io: SimpleNamespace, failure: str
) -> None:
    stop = asyncio.Event()
    partition = TopicPartition("test-topic", 0)
    worker_io.getmany.return_value = {
        partition: [
            SimpleNamespace(offset=offset, value={"offset": offset}) for offset in (5, 6, 7)
        ]
    }

    async def process(*args: object) -> None:
        if worker_io.process.await_count == 2 and failure == "process":
            raise RuntimeError("database transaction failed")
        if worker_io.process.await_count == 1 and failure == "shutdown":
            stop.set()

    worker_io.process.side_effect = process
    if failure == "commit":
        worker_io.commit.side_effect = RuntimeError("broker commit failed")

    async with asyncio.timeout(1):
        if failure == "shutdown":
            await worker.run(stop)
        else:
            with pytest.raises(RuntimeError, match="failed"):
                await worker.run(stop)

    assert worker_io.commit.await_args_list == [call({partition: 6})]
    assert worker_io.process.await_count == (2 if failure == "process" else 1)
    worker_io.stop.assert_awaited_once()
    worker_io.outbox_stop.assert_awaited_once()
    worker_io.producer_close.assert_awaited_once()
    worker_io.engine_close.assert_awaited_once()


async def test_partial_startup_closes_all_resources(
    worker: ModuleType, worker_io: SimpleNamespace
) -> None:
    worker_io.start.side_effect = RuntimeError("consumer startup failed")

    with pytest.raises(RuntimeError, match="consumer startup failed"):
        await worker.run(asyncio.Event())

    worker_io.stop.assert_awaited_once()
    worker_io.outbox_stop.assert_awaited_once()
    worker_io.producer_close.assert_awaited_once()
    worker_io.engine_close.assert_awaited_once()
    if worker is embedding:
        worker_io.embedder_close.assert_awaited_once()


async def test_worker_signal_cancels_active_record_without_committing(
    worker: ModuleType, worker_io: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(worker.get_settings().kafka, "shutdown_grace_seconds", 0.02)
    stop, rolled_back = asyncio.Event(), asyncio.Event()
    partition = TopicPartition("test-topic", 0)
    worker_io.getmany.return_value = {partition: [SimpleNamespace(offset=4, value={})]}

    async def process(*args: object) -> None:
        stop.set()
        try:
            await asyncio.Event().wait()
        finally:
            rolled_back.set()

    worker_io.process.side_effect = process
    async with asyncio.timeout(1):
        await worker.run(stop)

    assert rolled_back.is_set()
    worker_io.commit.assert_not_awaited()
    worker_io.stop.assert_awaited_once()
    worker_io.outbox_stop.assert_awaited_once()
    worker_io.producer_close.assert_awaited_once()
    worker_io.engine_close.assert_awaited_once()
