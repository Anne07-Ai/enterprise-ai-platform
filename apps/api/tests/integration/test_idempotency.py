"""Idempotency-Key behavior."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.integration._helpers import auth_headers, login, make_user_and_org

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_same_key_same_body_returns_cached(client: AsyncClient) -> None:
    await make_user_and_org()
    token = await login(client, email="owner@example.com", password="test-password-1234")

    headers = {**auth_headers(token), "Idempotency-Key": "test-key-1", "X-Test-Key": "1"}
    body = {"name": "k1", "scopes": ["org:read"]}
    r1 = await client.post("/v1/api-keys", json=body, headers=headers)
    r2 = await client.post("/v1/api-keys", json=body, headers=headers)
    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r2.headers.get("Idempotent-Replay") == "true"
    assert r1.json()["id"] == r2.json()["id"]


@pytest.mark.asyncio
async def test_same_key_different_body_returns_conflict(client: AsyncClient) -> None:
    await make_user_and_org()
    token = await login(client, email="owner@example.com", password="test-password-1234")

    headers = {**auth_headers(token), "Idempotency-Key": "test-key-2", "X-Test-Key": "1"}
    await client.post("/v1/api-keys", json={"name": "first", "scopes": []}, headers=headers)
    r2 = await client.post(
        "/v1/api-keys", json={"name": "different", "scopes": ["org:read"]}, headers=headers
    )
    assert r2.status_code == 422
    assert "idempotency-conflict" in r2.json()["type"]


@pytest.mark.asyncio
async def test_different_keys_execute_fresh(client: AsyncClient) -> None:
    await make_user_and_org()
    token = await login(client, email="owner@example.com", password="test-password-1234")

    base_headers = {**auth_headers(token), "X-Test-Key": "1"}
    r1 = await client.post(
        "/v1/api-keys",
        json={"name": "a", "scopes": []},
        headers={**base_headers, "Idempotency-Key": "k-a"},
    )
    r2 = await client.post(
        "/v1/api-keys",
        json={"name": "b", "scopes": []},
        headers={**base_headers, "Idempotency-Key": "k-b"},
    )
    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r1.json()["id"] != r2.json()["id"]
