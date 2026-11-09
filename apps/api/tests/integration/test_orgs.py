"""Organization endpoints — create, get, update, delete."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from uuid import UUID

import pytest
from app.infra.db import session_for_request
from app.modules.identity import events, service
from app.modules.identity.models import Membership, Organization
from sqlalchemy import select, text

if TYPE_CHECKING:
    from httpx import AsyncClient
    from sqlalchemy.ext.asyncio import AsyncSession

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


@pytest.mark.asyncio
async def test_create_org_grants_owner_only_in_new_org(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner, original_org = await make_user_and_org()
    outsider, outsider_org = await make_user_and_org(
        email="outsider@example.com", org_slug="outsider"
    )
    original_token = await login(client, email=owner.email, password="test-password-1234")
    outsider_token = await login(client, email=outsider.email, password="test-password-1234")

    bootstrap = service.create_org

    async def verify_restored_scope(session: AsyncSession, **kwargs: Any) -> Organization:
        org = await bootstrap(session, **kwargs)
        assert (await session.scalars(select(Membership.org_id))).all() == [original_org.id]
        assert await session.scalar(
            text("SELECT COALESCE(current_setting('app.bypass_rls', true), '') <> 'on'")
        )
        return org

    monkeypatch.setattr(service, "create_org", verify_restored_scope)

    created = await client.post(
        "/v1/orgs",
        json={"slug": "new-org", "name": "New Org", "is_test": True},
        headers=auth_headers(original_token),
    )
    assert created.status_code == 201, created.text
    body = created.json()
    new_org_id = UUID(body["id"])
    assert new_org_id not in {original_org.id, outsider_org.id}
    assert body["slug"] == "new-org"
    assert body["name"] == "New Org"
    assert body["is_test"] is True

    scoped = asynccontextmanager(session_for_request)
    async with scoped(org_id=new_org_id) as session:
        memberships = (await session.scalars(select(Membership))).all()
        assert [(row.org_id, row.user_id, row.role) for row in memberships] == [
            (new_org_id, owner.id, "owner")
        ]
        audit = await session.execute(
            text("SELECT org_id, actor_user_id FROM outbox WHERE event_type = 'org.created'")
        )
        assert [tuple(row) for row in audit] == [(new_org_id, owner.id)]

    for token in (original_token, outsider_token):
        denied = await client.get(f"/v1/orgs/{new_org_id}", headers=auth_headers(token))
        assert denied.status_code == 403, denied.text

    new_token = await login(
        client, email=owner.email, password="test-password-1234", org_slug="new-org"
    )
    me = await client.get("/v1/me", headers=auth_headers(new_token))
    assert me.status_code == 200, me.text
    assert me.json()["current_org"]["id"] == str(new_org_id)
    assert me.json()["role"] == "owner"
    denied = await client.get(f"/v1/orgs/{original_org.id}", headers=auth_headers(new_token))
    assert denied.status_code == 403, denied.text


@pytest.mark.asyncio
async def test_create_org_rejects_unauthenticated_and_attributed_api_key(
    client: AsyncClient,
) -> None:
    payload = {"slug": "forbidden-org", "name": "Forbidden Org"}
    unauthenticated = await client.post("/v1/orgs", json=payload)
    assert unauthenticated.status_code == 401, unauthenticated.text

    owner, _ = await make_user_and_org()
    token = await login(client, email=owner.email, password="test-password-1234")
    created_key = await client.post(
        "/v1/api-keys",
        json={"name": "org-client", "scopes": ["org:read"]},
        headers={**auth_headers(token), "X-Test-Key": "1"},
    )
    assert created_key.status_code == 201, created_key.text
    denied = await client.post(
        "/v1/orgs",
        json=payload,
        headers={"X-Api-Key": created_key.json()["plaintext"]},
    )
    assert denied.status_code == 403, denied.text

    async with asynccontextmanager(session_for_request)(org_id=None) as session:
        assert await session.scalar(select(Organization).filter_by(slug=payload["slug"])) is None


@pytest.mark.asyncio
async def test_bootstrap_rollback_restores_scope_and_keeps_outer_transaction_usable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner, original_org = await make_user_and_org()
    await make_user_and_org(email="outsider@example.com", org_slug="outsider")
    failed_org_id: UUID | None = None
    real_emit = events.emit_audit

    async def emit_then_fail(session: AsyncSession, **kwargs: Any) -> None:
        nonlocal failed_org_id
        failed_org_id = kwargs["org_id"]
        current_org = await session.scalar(text("SELECT current_setting('app.current_org', true)"))
        assert current_org == str(failed_org_id)
        assert await session.scalar(
            text("SELECT COALESCE(current_setting('app.bypass_rls', true), '') <> 'on'")
        )
        # The temporary tenant context can see its own new membership only.
        assert (await session.scalars(select(Membership.org_id))).all() == [failed_org_id]
        await real_emit(session, **kwargs)
        assert (
            await session.scalar(
                text("SELECT count(*) FROM outbox WHERE event_type = 'org.created'")
            )
            == 1
        )
        raise RuntimeError("audit failure after enqueue")

    monkeypatch.setattr(events, "emit_audit", emit_then_fail)
    scoped = asynccontextmanager(session_for_request)
    async with scoped(org_id=original_org.id) as session:
        with pytest.raises(RuntimeError, match="audit failure after enqueue"):
            await service.create_org(
                session, slug="rolled-back", name="Rolled Back", creator_user_id=owner.id
            )
        current_org = await session.scalar(text("SELECT current_setting('app.current_org', true)"))
        assert current_org == str(original_org.id)
        assert (await session.scalars(select(Membership.org_id))).all() == [original_org.id]
        assert await session.scalar(select(Organization).filter_by(slug="rolled-back")) is None

    assert failed_org_id is not None
    async with scoped(org_id=failed_org_id) as session:
        assert (await session.scalars(select(Membership))).all() == []
        assert await session.scalar(text("SELECT count(*) FROM outbox")) == 0
