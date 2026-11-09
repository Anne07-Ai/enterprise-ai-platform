"""The worker deadline is shared by drain, rollback, and dependent closes."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest
from app.core.config import KafkaSettings, get_settings
from app.infra import db, outbox
from app.infra.shutdown_budget import remaining_seconds
from app.workers import (
    WorkerShutdownDeadlineExceeded,
    close_worker_dependency,
    run_until_stopped,
)
from pydantic import ValidationError

if TYPE_CHECKING:
    from contextlib import AsyncExitStack


@pytest.mark.parametrize("invalid", [0.0, -1.0, float("inf"), float("nan")])
def test_worker_budget_requires_positive_finite_seconds(invalid: float) -> None:
    with pytest.raises(ValidationError):
        KafkaSettings(worker_shutdown_timeout_seconds=invalid)


async def test_combined_cleanup_stages_share_remaining_worker_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = get_settings().kafka
    monkeypatch.setattr(settings, "worker_shutdown_timeout_seconds", 0.15)
    monkeypatch.setattr(settings, "shutdown_grace_seconds", 2.0)
    monkeypatch.setattr(settings, "shutdown_cleanup_seconds", 2.0)
    stop = asyncio.Event()
    close_budgets: list[float] = []
    rolled_back = asyncio.Event()

    async def stalled_close() -> None:
        close_budgets.append(remaining_seconds(2.0))
        await asyncio.Event().wait()

    async def consume(signal: asyncio.Event, stack: AsyncExitStack) -> None:
        stack.push_async_callback(close_worker_dependency, stalled_close)
        stack.push_async_callback(close_worker_dependency, stalled_close)
        signal.set()
        try:
            await asyncio.Event().wait()
        finally:
            rolled_back.set()

    loop = asyncio.get_running_loop()
    started = loop.time()
    with pytest.raises(TimeoutError):
        await run_until_stopped(consume, stop)

    assert rolled_back.is_set()
    assert len(close_budgets) == 2
    assert 0.0 < close_budgets[0] <= 0.075
    assert close_budgets[1] == 0.0
    assert loop.time() - started < 0.35
    assert all(not task.get_name().startswith("kafka-worker") for task in asyncio.all_tasks())


async def test_stalled_rollback_aborts_only_worker_owned_database_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = get_settings().kafka
    monkeypatch.setattr(settings, "worker_shutdown_timeout_seconds", 0.20)
    monkeypatch.setattr(settings, "shutdown_grace_seconds", 0.03)
    monkeypatch.setattr(settings, "shutdown_cleanup_seconds", 0.05)
    stop, rolled_back = asyncio.Event(), asyncio.Event()
    owned_closed = asyncio.Event()
    owned = Mock(is_closed=owned_closed.is_set, terminate=owned_closed.set)
    returned = Mock(is_closed=lambda: False)
    outside = Mock(is_closed=lambda: False)
    outbox_stopped = asyncio.Event()

    def register(transport: Mock) -> None:
        db._track_worker_checkout(SimpleNamespace(driver_connection=transport), None, None)

    async def stalled_publisher() -> int:
        try:
            await asyncio.Event().wait()
        finally:
            outbox_stopped.set()
        return 0

    monkeypatch.setattr(outbox, "_publish_batch", stalled_publisher)
    register(outside)

    async def consume(signal: asyncio.Event, stack: AsyncExitStack) -> None:
        register(owned)
        register(returned)
        db._forget_worker_checkin(SimpleNamespace(driver_connection=returned), None)
        stack.push_async_callback(outbox.stop_publisher)
        await outbox.start_publisher()
        # Let the publisher enter its blocked operation before sending SIGTERM.
        await asyncio.sleep(0)
        signal.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await owned_closed.wait()
            rolled_back.set()
            raise

    async with asyncio.timeout(1):
        with pytest.raises(WorkerShutdownDeadlineExceeded):
            await run_until_stopped(consume, stop)

    assert rolled_back.is_set()
    assert outbox_stopped.is_set()
    assert owned_closed.is_set()
    returned.terminate.assert_not_called()
    outside.terminate.assert_not_called()
    assert outbox._PUBLISHER_TASK is None
