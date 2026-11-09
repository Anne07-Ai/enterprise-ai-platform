"""Shared worker shutdown coordination."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING

from app.core.config import get_settings
from app.core.logging import get_logger
from app.infra import outbox
from app.infra.db import track_worker_database_connections
from app.infra.shutdown_budget import ShutdownBudget, remaining_seconds, worker_shutdown_budget

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine
    from typing import Any

    from app.infra.db import WorkerDatabaseConnections

logger = get_logger(__name__)


class WorkerShutdownDeadlineExceeded(RuntimeError):
    """A clean worker exit could not complete within its shutdown policy."""


async def close_worker_dependency(close: Callable[[], Awaitable[None]]) -> None:
    async with asyncio.timeout(remaining_seconds(get_settings().kafka.shutdown_cleanup_seconds)):
        await close()


async def run_until_stopped(
    consume: Callable[[asyncio.Event, AsyncExitStack], Coroutine[Any, Any, None]],
    stop: asyncio.Event,
) -> None:
    """Own processing and cleanup under one monotonic shutdown deadline."""
    budget = ShutdownBudget()
    token = worker_shutdown_budget.set(budget)
    loop = asyncio.get_running_loop()
    try:
        with track_worker_database_connections() as connections:
            watchdog = asyncio.create_task(
                _watch_shutdown(budget, connections), name="kafka-worker-shutdown"
            )
            try:
                async with AsyncExitStack() as stack:
                    try:
                        await _run_active(consume(stop, stack), stop, budget)
                    finally:
                        budget.begin()
                        outbox.request_publisher_stop()
            finally:
                watchdog.cancel()
                await asyncio.gather(watchdog, return_exceptions=True)
                if not watchdog.cancelled():
                    watchdog.result()
                assert budget.started_at is not None and budget.deadline is not None
                now = loop.time()
                exhausted = now >= budget.deadline
                logger.info(
                    "kafka.worker.shutdown_finished",
                    duration_seconds=round(now - budget.started_at, 3),
                    deadline_exhausted=exhausted,
                    database_abort=connections.aborted,
                )
            if exhausted or connections.aborted:
                raise WorkerShutdownDeadlineExceeded("Worker shutdown required recovery")
    finally:
        worker_shutdown_budget.reset(token)


async def _run_active(
    worker: Coroutine[Any, Any, None], stop: asyncio.Event, budget: ShutdownBudget
) -> None:
    work = asyncio.create_task(worker, name="kafka-worker")
    signal = asyncio.create_task(stop.wait(), name="kafka-worker-stop")
    joined = asyncio.gather(work, return_exceptions=True)
    try:
        done, _ = await asyncio.wait((work, signal), return_when=asyncio.FIRST_COMPLETED)
        if work in done:
            work.result()
            return

        budget.begin()
        outbox.request_publisher_stop()
        _, pending = await asyncio.wait(
            (work,),
            timeout=remaining_seconds(get_settings().kafka.shutdown_grace_seconds, drain=True),
        )
        if pending:
            logger.warning("kafka.worker.shutdown_deadline_exceeded")
            if not work.cancelling():
                work.cancel()
        await asyncio.shield(joined)
        if not work.cancelled():
            work.result()
    finally:
        signal.cancel()
        if not work.done() and not work.cancelling():
            budget.begin()
            outbox.request_publisher_stop()
            work.cancel()
        # A second cancellation could interrupt SQLAlchemy's rollback and
        # leave a pooled connection with an unfinished transaction.
        await asyncio.gather(signal, joined, return_exceptions=True)


async def _watch_shutdown(budget: ShutdownBudget, connections: WorkerDatabaseConnections) -> None:
    await budget.started.wait()
    assert budget.started_at is not None and budget.drain_until is not None
    assert budget.deadline is not None
    loop = asyncio.get_running_loop()
    await asyncio.sleep(max(0.0, budget.drain_until - loop.time()))
    outbox.cancel_pending_publisher()

    # Leave time for normal rollback after task cancellation. If the DB is
    # still stalled, close its owned sockets and let PostgreSQL abort them.
    rollback_until = min(
        budget.drain_until + get_settings().kafka.shutdown_cleanup_seconds,
        budget.started_at + 0.75 * (budget.deadline - budget.started_at),
    )
    await asyncio.sleep(max(0.0, rollback_until - loop.time()))
    count = connections.abort_active()
    if count:
        logger.warning("kafka.worker.database_shutdown_aborted", connections=count)

    await asyncio.sleep(max(0.0, budget.deadline - loop.time()))
    logger.error("kafka.worker.shutdown_budget_exhausted")
