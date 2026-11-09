"""API key creation, listing, and revocation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.integration._helpers import TEST_PASSWORD, auth_headers, login, make_user_and_org

if TYPE_CHECKING:
    from httpx import AsyncClient

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_plaintext_returned_only_on_create(client: AsyncClient) -> None:
    await make_user_and_org()
    token = await login(client, email="owner@example.com", password=TEST_PASSWORD)
    create = await client.post(
        "/v1/api-keys",
        json={"name": "build-bot", "scopes": ["org:read"]},
        headers={**auth_headers(token), "X-Test-Key": "1"},
    )
    assert create.status_code == 201, create.text
    body = create.json()
    assert body["plaintext"].startswith("eaip_test_")
    assert body["prefix"].startswith("eaip_test_")

    listed = await client.get("/v1/api-keys", headers=auth_headers(token))
    assert listed.status_code == 200
    items = listed.json()
    assert len(items) == 1
    assert "plaintext" not in items[0], "plaintext must never be returned on list"


@pytest.mark.asyncio
async def test_api_key_can_authenticate_then_revoke(client: AsyncClient) -> None:
    await make_user_and_org()
    token = await login(client, email="owner@example.com", password=TEST_PASSWORD)
    create = await client.post(
        "/v1/api-keys",
        json={"name": "k", "scopes": ["org:read"]},
        headers={**auth_headers(token), "X-Test-Key": "1"},
    )
    plaintext = create.json()["plaintext"]
    key_id = create.json()["id"]

    # The API key authenticates.
    me_r = await client.get("/v1/api-keys", headers={"X-Api-Key": plaintext})
    assert me_r.status_code == 200, me_r.text

    # Revoke and confirm.
    rev = await client.delete(f"/v1/api-keys/{key_id}", headers=auth_headers(token))
    assert rev.status_code == 204

    after = await client.get("/v1/api-keys", headers={"X-Api-Key": plaintext})
    assert after.status_code == 401
