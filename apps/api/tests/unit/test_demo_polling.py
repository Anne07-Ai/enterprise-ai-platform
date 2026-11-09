"""Offline checks for the demo command polling deadlines."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from app.scripts import demo_rag_search, test_agent

if TYPE_CHECKING:
    from types import ModuleType

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("script", "poll"), [(demo_rag_search, "_wait_for_ready"), (test_agent, "_wait_ready")]
)
async def test_demo_times_out_a_stalled_poll(
    monkeypatch: pytest.MonkeyPatch, script: ModuleType, poll: str
) -> None:
    org_id, user_id, document_id = uuid4(), uuid4(), uuid4()
    cancelled = asyncio.Event()

    async def stalled_poll(*_args: object) -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    def expire_immediately(seconds: float) -> asyncio.Timeout:
        assert seconds == 60.0
        return asyncio.timeout(0)

    monkeypatch.setattr(script, "_seed", AsyncMock(return_value=(org_id, user_id)))
    monkeypatch.setattr(script, "_upload", AsyncMock(return_value=document_id))
    monkeypatch.setattr(script, poll, stalled_poll)
    monkeypatch.setattr(script, "asyncio", SimpleNamespace(timeout=expire_immediately))

    async with asyncio.timeout(1):
        with pytest.raises(TimeoutError):
            await script.main()
    assert cancelled.is_set()
