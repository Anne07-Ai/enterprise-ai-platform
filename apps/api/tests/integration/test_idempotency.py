"""Idempotency-Key behavior."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from app.modules.identity.security import mint_access_token

from tests.integration._helpers import TEST_PASSWORD, auth_headers, login, make_user_and_org

if TYPE_CHECKING:
    from httpx import AsyncClient

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_same_key_same_body_returns_cached(client: AsyncClient) -> None:
    await make_user_and_org()
    token = await login(client, email="owner@example.com", password=TEST_PASSWORD)

    headers = {**auth_headers(token), "Idempotency-Key": "test-key-1"}
    body = {"name": "Replay", "slug": "replay", "is_test": True}
    r1 = await client.post("/v1/orgs", json=body, headers=headers)
    r2 = await client.post("/v1/orgs", json=body, headers=headers)
    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r2.headers.get("Idempotent-Replay") == "true"
    assert r1.json()["id"] == r2.json()["id"]


@pytest.mark.asyncio
async def test_same_key_different_body_returns_conflict(client: AsyncClient) -> None:
    await make_user_and_org()
    token = await login(client, email="owner@example.com", password=TEST_PASSWORD)

    headers = {**auth_headers(token), "Idempotency-Key": "test-key-2"}
    first = await client.post(
        "/v1/orgs", json={"name": "First", "slug": "first", "is_test": True}, headers=headers
    )
    assert first.status_code == 201
    r2 = await client.post(
        "/v1/orgs",
        json={"name": "Different", "slug": "different", "is_test": True},
        headers=headers,
    )
    assert r2.status_code == 422
    assert "idempotency-conflict" in r2.json()["type"]


@pytest.mark.asyncio
async def test_different_keys_execute_fresh(client: AsyncClient) -> None:
    await make_user_and_org()
    token = await login(client, email="owner@example.com", password=TEST_PASSWORD)

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


@pytest.mark.asyncio
async def test_api_key_issuance_is_not_replayed_or_duplicated(client: AsyncClient) -> None:
    owner, org = await make_user_and_org()
    token = await login(client, email="owner@example.com", password=TEST_PASSWORD)
    headers = {**auth_headers(token), "Idempotency-Key": "issue-once", "X-Test-Key": "1"}
    body = {"name": "issue-once", "scopes": []}
    issued = await client.post("/v1/api-keys", json=body, headers=headers)
    assert issued.status_code == 201
    assert issued.json()["plaintext"]

    replay = await client.post("/v1/api-keys", json=body, headers=headers)
    assert replay.status_code == 422
    assert "idempotency-conflict" in replay.json()["type"]
    assert issued.json()["plaintext"] not in replay.text

    member_token, _, _ = mint_access_token(user_id=uuid4(), org_id=org.id, role="member")
    downgraded_token, _, _ = mint_access_token(user_id=owner.id, org_id=org.id, role="member")
    for lower_privilege_token in (member_token, downgraded_token):
        forbidden = await client.post(
            "/v1/api-keys",
            json=body,
            headers={**headers, **auth_headers(lower_privilege_token)},
        )
        assert forbidden.status_code == 403
        assert issued.json()["plaintext"] not in forbidden.text

    listed = await client.get("/v1/api-keys", headers=auth_headers(token))
    assert listed.status_code == 200
    assert [key["id"] for key in listed.json()] == [issued.json()["id"]]
