"""Shared helpers for integration tests."""

from __future__ import annotations

from httpx import AsyncClient
from sqlalchemy import text

from app.infra.db import get_sessionmaker
from app.modules.identity import service as identity_service
from app.modules.identity.models import Organization, User
from app.modules.identity.security import hash_password


async def make_user_and_org(
    *,
    email: str = "owner@example.com",
    password: str = "test-password-1234",
    org_slug: str = "acme",
    org_name: str = "Acme Co.",
) -> tuple[User, Organization]:
    sm = get_sessionmaker()
    async with sm() as session:
        async with session.begin():
            # Bypass RLS to create the initial user/org/membership tuple —
            # there is no current_org yet at this point.
            await session.execute(text("SELECT set_config('app.bypass_rls', 'on', true)"))
            user = User(email=email, display_name=email, password_hash=hash_password(password))
            org = Organization(slug=org_slug, name=org_name, is_test=True)
            session.add_all([user, org])
            await session.flush()

            from app.modules.identity.models import Membership

            session.add(Membership(org_id=org.id, user_id=user.id, role="owner"))
            await session.flush()
            user_id, org_id = user.id, org.id

    # Re-fetch to get the persisted shape with bypass on so RLS doesn't filter.
    async with sm() as session:
        async with session.begin():
            await session.execute(text("SELECT set_config('app.bypass_rls', 'on', true)"))
            user = await session.get(User, user_id)
            org = await session.get(Organization, org_id)
        assert user is not None and org is not None
        return user, org


async def login(client: AsyncClient, *, email: str, password: str, org_slug: str | None = None) -> str:
    payload: dict[str, object] = {"email": email, "password": password}
    if org_slug:
        payload["org_slug"] = org_slug
    r = await client.post("/v1/auth/login", json=payload)
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


def auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}
