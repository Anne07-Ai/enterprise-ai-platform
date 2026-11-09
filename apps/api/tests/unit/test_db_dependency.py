"""The request dependency forwards errors to the transaction context."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from app.core import deps
from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession


async def test_request_dependency_propagates_failure_to_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    org_id = uuid4()
    session = Mock(spec=AsyncSession)
    failure = RuntimeError("request failure")
    handled: list[Exception] = []

    async def transaction(*, org_id: UUID | None) -> AsyncIterator[AsyncSession]:
        assert org_id == request.state.org_id
        try:
            yield session
        except RuntimeError as error:
            handled.append(error)
            raise

    request = Mock(spec=Request, state=SimpleNamespace(org_id=org_id))
    monkeypatch.setattr(deps, "session_for_request", transaction)
    with pytest.raises(RuntimeError, match="request failure"):
        async with asynccontextmanager(deps.get_db)(request) as active_session:
            assert active_session is session
            raise failure
    assert handled == [failure]
