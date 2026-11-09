"""Operational outbox logs distinguish selection, committed progress and safe failures."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock, call

import pytest
from app.infra import kafka, outbox

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@pytest.mark.parametrize("commit_fails", [False, True])
async def test_outbox_logs_only_committed_progress(
    monkeypatch: pytest.MonkeyPatch, commit_fails: bool
) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        SimpleNamespace(
            id=index,
            topic="internal-topic",
            key="internal-key",
            payload={"data": "private"},
            created_at=now - timedelta(seconds=age),
        )
        for index, age in enumerate((60, 20, 10))
    ]
    session = SimpleNamespace(execute=AsyncMock(side_effect=[Mock(all=lambda: rows), None, None]))

    async def transaction() -> AsyncIterator[object]:
        yield session
        if commit_fails:
            raise RuntimeError("commit failed")

    logger = Mock()
    monkeypatch.setattr(outbox, "logger", logger)
    monkeypatch.setattr(outbox, "datetime", SimpleNamespace(now=lambda _tz: now))
    monkeypatch.setattr(outbox, "session_unscoped", transaction)
    monkeypatch.setattr(
        kafka, "send", AsyncMock(side_effect=[RuntimeError("private request data"), None, None])
    )

    selection = call("outbox.publish.selected", count=3, oldest_selected_age_seconds=60.0)
    if commit_fails:
        with pytest.raises(RuntimeError, match="commit failed"):
            await outbox._publish_batch()
        assert logger.info.call_args_list == [selection]
    else:
        assert await outbox._publish_batch() == 2
        assert logger.info.call_args_list == [
            selection,
            call("outbox.publish.batch", count=2, oldest_published_age_seconds=20.0),
        ]
    logger.warning.assert_called_once_with("outbox.publish.failed", error_type="RuntimeError")
