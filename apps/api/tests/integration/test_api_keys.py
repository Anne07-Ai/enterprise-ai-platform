"""API key creation, listing, and revocation."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.integration._helpers import auth_headers, login, make_user_and_org

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_plaintext_returned_only_on_create(client: AsyncClient) -> None:
    await make_user_and_org()
    token = await login(client, email="owner@example.com", password="test-password-1234")
    create = await client.post(
        "/v1/api-keys",
        json={"name": "build-bot", "scopes": ["org:read"]},
        headers={**auth_headers(token), "X-Test-Key": "1"},
    )
    assert create.status_code == 201, create.text
    body = create.json()
    assert body["plaintext"].startswith("eaip_test_")
    assert body["prefix"].startswith("eaip_test_")
    plaintext = body["plaintext"]

    listed = await client.get("/v1/api-keys", headers=auth_headers(token))
    assert listed.status_code == 200
    items = listed.json()
    assert len(items) == 1
    assert "plaintext" not in items[0], "plaintext must never be returned on list"


@pytest.mark.asyncio
async def test_api_key_can_authenticate_then_revoke(client: AsyncClient) -> None:
    await make_user_and_org()
    token = await login(client, email="owner@example.com", password="test-password-1234")
    create = await client.post(
        "/v1/api-keys",
        json={"name": "k", "scopes": ["org:read"]},
        headers={**auth_headers(token), "X-Test-Key": "1"},
    )
    plaintext = create.json()["plaintext"]
    key_id = create.json()["id"]

    # The API key authenticates.
    r = await client.get(
        f"/v1/orgs/{create.json()['id'][:0]}",
        headers={"X-Api-Key": plaintext},
    )
    # Either 404 (org id segment is empty) or 401 — what we care about is that
    # the key is recognized so we don't get a 401-from-auth-middleware. So we
    # try a real org endpoint instead.
    me_r = await client.get("/v1/api-keys", headers={"X-Api-Key": plaintext})
    assert me_r.status_code == 200, me_r.text

    # Revoke and confirm.
    rev = await client.delete(f"/v1/api-keys/{key_id}", headers=auth_headers(token))
    assert rev.status_code == 204

    after = await client.get("/v1/api-keys", headers={"X-Api-Key": plaintext})
    assert after.status_code == 401
