"""One monotonic budget for a worker and the cleanup it owns."""

from __future__ import annotations

import asyncio
from contextvars import ContextVar

from app.core.config import get_settings


class ShutdownBudget:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.started_at: float | None = None
        self.drain_until: float | None = None
        self.deadline: float | None = None

    def begin(self) -> None:
        if self.started.is_set():
            return
        settings = get_settings().kafka
        now = asyncio.get_running_loop().time()
        total = settings.worker_shutdown_timeout_seconds
        self.started_at = now
        self.drain_until = now + min(settings.shutdown_grace_seconds, total / 2)
        self.deadline = now + total
        self.started.set()


worker_shutdown_budget: ContextVar[ShutdownBudget | None] = ContextVar(
    "worker_shutdown_budget", default=None
)


def remaining_seconds(limit: float, *, drain: bool = False) -> float:
    """Cap a stage without resetting or extending the worker deadline."""
    budget = worker_shutdown_budget.get()
    if budget is None:
        return limit
    deadline = budget.drain_until if drain else budget.deadline
    if deadline is None:
        return limit
    return max(0.0, min(limit, deadline - asyncio.get_running_loop().time()))
