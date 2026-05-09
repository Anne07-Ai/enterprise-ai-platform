"""**LOAD-BEARING SECURITY TEST.**

Two organizations exist with their own memberships, API keys, and refresh tokens.
We bind a session to org A and assert that:

  * Queries WITHOUT a tenant predicate at the app layer return only A's rows.
  * Direct INSERTs targeting B's org_id are rejected by the RLS WITH CHECK clause.
  * A session bound to no tenant (current_org GUC unset) sees zero tenant rows.

This test is the sentinel for the entire platform's tenancy model. If it ever
fails, every release is blocked until the policy is fixed.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from app.infra.db import get_sessionmaker
from app.modules.identity.models import ApiKey, Membership, Organization, RefreshToken, User
from app.modules.identity.security import generate_api_key, hash_password


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_cross_tenant_rows_are_invisible_under_rls() -> None:
    sm = get_sessionmaker()

    # 1. Create the universe (no tenant binding while inserting orgs/users —
    #    organizations and users are not RLS-restricted, only their per-tenant
    #    relations are).
    async with sm() as session:
        async with session.begin():
            org_a = Organization(slug="org-a", name="A", is_test=True)
            org_b = Organization(slug="org-b", name="B", is_test=True)
            user_a = User(email="a@example.com", display_name="A", password_hash=hash_password("xxxxxxxx"))
            user_b = User(email="b@example.com", display_name="B", password_hash=hash_password("yyyyyyyy"))
            session.add_all([org_a, org_b, user_a, user_b])
            await session.flush()
            org_a_id, org_b_id = org_a.id, org_b.id
            user_a_id, user_b_id = user_a.id, user_b.id

    # 2. Insert tenant-scoped rows for each org with the GUC bound to that tenant.
    async def _populate(org_id: object, user_id: object) -> None:
        async with sm() as session:
            async with session.begin():
                await session.execute(
                    text("SELECT set_config('app.current_org', :o, true)"),
                    {"o": str(org_id)},
                )
                session.add(Membership(org_id=org_id, user_id=user_id, role="owner"))
                _, prefix, h = generate_api_key(is_test=True)
                session.add(
                    ApiKey(
                        org_id=org_id,
                        created_by=user_id,
                        name="seed",
                        prefix=prefix,
                        hash=h,
                        scopes=[],
                    )
                )

    await _populate(org_a_id, user_a_id)
    await _populate(org_b_id, user_b_id)

    # 3. Bound to A: queries with NO app-layer tenant predicate must return ONLY A's rows.
    async with sm() as session:
        async with session.begin():
            await session.execute(
                text("SELECT set_config('app.current_org', :o, true)"),
                {"o": str(org_a_id)},
            )
            memberships = (await session.execute(select(Membership))).scalars().all()
            api_keys = (await session.execute(select(ApiKey))).scalars().all()

            assert len(memberships) == 1, "RLS leaked B's membership row to A"
            assert memberships[0].org_id == org_a_id
            assert len(api_keys) == 1, "RLS leaked B's API key row to A"
            assert api_keys[0].org_id == org_a_id

    # 4. Bound to A: WITH CHECK should reject an insert targeting B.
    async with sm() as session:
        async with session.begin():
            await session.execute(
                text("SELECT set_config('app.current_org', :o, true)"),
                {"o": str(org_a_id)},
            )
            session.add(Membership(org_id=org_b_id, user_id=user_a_id, role="member"))
            with pytest.raises(DBAPIError):
                await session.flush()

    # 5. With NO GUC set: tenant tables must return zero rows (fail-closed).
    async with sm() as session:
        async with session.begin():
            memberships = (await session.execute(select(Membership))).scalars().all()
            api_keys = (await session.execute(select(ApiKey))).scalars().all()
            tokens = (await session.execute(select(RefreshToken))).scalars().all()
            assert memberships == []
            assert api_keys == []
            assert tokens == []
