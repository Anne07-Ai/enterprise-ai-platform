"""Health and readiness probes."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_healthz_is_ok(client: AsyncClient) -> None:
    r = await client.get("/healthz")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "version" in body


@pytest.mark.asyncio
async def test_readyz_exercises_dependencies(client: AsyncClient) -> None:
    r = await client.get("/readyz")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "ok"
    for dep in ("postgres", "redis", "kafka"):
        assert body["checks"][dep] == "ok"


@pytest.mark.asyncio
async def test_openapi_includes_security_schemes(client: AsyncClient) -> None:
    r = await client.get("/openapi.json")
    assert r.status_code == 200
    spec = r.json()
    assert spec["openapi"].startswith("3.")
    schemes = spec["components"]["securitySchemes"]
    assert "BearerJWT" in schemes
    assert "ApiKey" in schemes
