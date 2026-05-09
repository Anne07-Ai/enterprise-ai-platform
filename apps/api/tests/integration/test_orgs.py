"""Organization endpoints — create, get, update, delete."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.integration._helpers import auth_headers, login, make_user_and_org

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_get_own_org(client: AsyncClient) -> None:
    _, org = await make_user_and_org()
    token = await login(client, email="owner@example.com", password="test-password-1234")
    r = await client.get(f"/v1/orgs/{org.id}", headers=auth_headers(token))
    assert r.status_code == 200, r.text
    assert r.json()["slug"] == "acme"


@pytest.mark.asyncio
async def test_update_org_name(client: AsyncClient) -> None:
    _, org = await make_user_and_org()
    token = await login(client, email="owner@example.com", password="test-password-1234")
    r = await client.patch(
        f"/v1/orgs/{org.id}",
        json={"name": "New Name"},
        headers=auth_headers(token),
    )
    assert r.status_code == 200
    assert r.json()["name"] == "New Name"


@pytest.mark.asyncio
async def test_get_other_org_is_forbidden(client: AsyncClient) -> None:
    await make_user_and_org()
    token = await login(client, email="owner@example.com", password="test-password-1234")
    # Use a random UUID for a different org id.
    r = await client.get(
        "/v1/orgs/00000000-0000-0000-0000-000000000000",
        headers=auth_headers(token),
    )
    assert r.status_code == 403
