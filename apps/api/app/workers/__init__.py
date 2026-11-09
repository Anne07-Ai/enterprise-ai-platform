"""Shared worker shutdown coordination."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from app.core.config import get_settings
from app.core.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Coroutine
    from typing import Any

logger = get_logger(__name__)


async def run_until_stopped(worker: Coroutine[Any, Any, None], stop: asyncio.Event) -> None:
    """Allow the active record to finish on signal, then cancel and join."""
    work = asyncio.create_task(worker, name="kafka-worker")
    signal = asyncio.create_task(stop.wait(), name="kafka-worker-stop")
    joined = asyncio.gather(work, return_exceptions=True)
    try:
        done, _ = await asyncio.wait((work, signal), return_when=asyncio.FIRST_COMPLETED)
        if work in done:
            work.result()
            return

        _, pending = await asyncio.wait(
            (work,), timeout=get_settings().kafka.shutdown_grace_seconds
        )
        if pending:
            logger.warning("kafka.worker.shutdown_deadline_exceeded")
            work.cancel()
        await asyncio.shield(joined)
        if not work.cancelled():
            work.result()
    finally:
        signal.cancel()
        if not work.done() and not work.cancelling():
            work.cancel()
        await asyncio.gather(signal, joined, return_exceptions=True)
