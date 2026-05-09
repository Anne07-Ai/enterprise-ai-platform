"""Membership invite / list / change-role / remove."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.integration._helpers import auth_headers, login, make_user_and_org

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_invite_lists_member(client: AsyncClient) -> None:
    _, org = await make_user_and_org()
    token = await login(client, email="owner@example.com", password="test-password-1234")
    r = await client.post(
        f"/v1/orgs/{org.id}/memberships",
        json={"email": "new@example.com", "role": "member"},
        headers=auth_headers(token),
    )
    assert r.status_code == 201
    list_r = await client.get(
        f"/v1/orgs/{org.id}/memberships",
        headers=auth_headers(token),
    )
    members = list_r.json()
    assert len(members) == 2  # owner + invited


@pytest.mark.asyncio
async def test_change_member_role(client: AsyncClient) -> None:
    _, org = await make_user_and_org()
    token = await login(client, email="owner@example.com", password="test-password-1234")
    invite = await client.post(
        f"/v1/orgs/{org.id}/memberships",
        json={"email": "elev@example.com", "role": "member"},
        headers=auth_headers(token),
    )
    membership_id = invite.json()["id"]
    r = await client.patch(
        f"/v1/orgs/{org.id}/memberships/{membership_id}",
        json={"role": "admin"},
        headers=auth_headers(token),
    )
    assert r.status_code == 200
    assert r.json()["role"] == "admin"
