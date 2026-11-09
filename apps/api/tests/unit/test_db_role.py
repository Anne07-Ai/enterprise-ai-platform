"""Fail closed when the PostgreSQL login's privileges cannot be accepted."""

from unittest.mock import AsyncMock, Mock

import pytest
from app.core.config import Settings
from app.infra.db import _build_engine, _check_runtime_role, _verify_runtime_role
from sqlalchemy import event


@pytest.mark.parametrize("database_result", [True, None])
async def test_role_probe_rejects_and_terminates_unsafe_connection(
    database_result: bool | None,
) -> None:
    connection = Mock(fetchval=AsyncMock(return_value=database_result))
    with pytest.raises(RuntimeError, match="SUPERUSER or BYPASSRLS"):
        await _verify_runtime_role(connection)
    connection.terminate.assert_called_once_with()


async def test_role_probe_accepts_restricted_connection() -> None:
    connection = Mock(fetchval=AsyncMock(return_value=False))
    await _verify_runtime_role(connection)
    connection.terminate.assert_not_called()


async def test_role_probe_terminates_connection_on_catalog_failure() -> None:
    connection = Mock(fetchval=AsyncMock(side_effect=OSError("probe unavailable")))
    with pytest.raises(OSError, match="probe unavailable"):
        await _verify_runtime_role(connection)
    connection.terminate.assert_called_once_with()


async def test_runtime_engine_has_role_guard() -> None:
    # Creating an engine registers the pool listener without connecting.
    engine = _build_engine(Settings())
    try:
        assert event.contains(engine.sync_engine, "connect", _check_runtime_role)
    finally:
        await engine.dispose()
