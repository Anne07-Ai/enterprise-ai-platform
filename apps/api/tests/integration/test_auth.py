"""Auth flow: login, refresh, logout, and token semantics."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.integration._helpers import auth_headers, login, make_user_and_org

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_login_returns_token_pair(client: AsyncClient) -> None:
    await make_user_and_org()
    r = await client.post(
        "/v1/auth/login",
        json={"email": "owner@example.com", "password": "test-password-1234"},
    )
    assert r.status_code == 200
    body = r.json()
    assert "access_token" in body and "refresh_token" in body
    assert body["token_type"] == "Bearer"
    assert body["expires_in"] == 900  # 15 minutes


@pytest.mark.asyncio
async def test_invalid_password_returns_401(client: AsyncClient) -> None:
    await make_user_and_org()
    r = await client.post(
        "/v1/auth/login",
        json={"email": "owner@example.com", "password": "wrong-password-x"},
    )
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_unknown_user_returns_401(client: AsyncClient) -> None:
    r = await client.post(
        "/v1/auth/login",
        json={"email": "ghost@example.com", "password": "doesnt-matter-x"},
    )
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_refresh_rotates_and_invalidates_old(client: AsyncClient) -> None:
    await make_user_and_org()
    r = await client.post(
        "/v1/auth/login",
        json={"email": "owner@example.com", "password": "test-password-1234"},
    )
    body = r.json()
    old_refresh = body["refresh_token"]

    r2 = await client.post("/v1/auth/refresh", json={"refresh_token": old_refresh})
    assert r2.status_code == 200
    new_refresh = r2.json()["refresh_token"]
    assert new_refresh != old_refresh

    # Old refresh must now be unusable.
    r3 = await client.post("/v1/auth/refresh", json={"refresh_token": old_refresh})
    assert r3.status_code == 401


@pytest.mark.asyncio
async def test_me_requires_auth(client: AsyncClient) -> None:
    r = await client.get("/v1/me")
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_me_returns_user_and_memberships(client: AsyncClient) -> None:
    await make_user_and_org()
    token = await login(client, email="owner@example.com", password="test-password-1234")
    r = await client.get("/v1/me", headers=auth_headers(token))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["user"]["email"] == "owner@example.com"
    assert body["current_org"]["slug"] == "acme"
    assert body["role"] == "owner"


@pytest.mark.asyncio
async def test_logout_revokes_refresh(client: AsyncClient) -> None:
    await make_user_and_org()
    r = await client.post(
        "/v1/auth/login",
        json={"email": "owner@example.com", "password": "test-password-1234"},
    )
    body = r.json()
    token = body["access_token"]
    refresh = body["refresh_token"]

    await client.post(
        "/v1/auth/logout",
        json={"refresh_token": refresh},
        headers=auth_headers(token),
    )
    r2 = await client.post("/v1/auth/refresh", json={"refresh_token": refresh})
    assert r2.status_code == 401
