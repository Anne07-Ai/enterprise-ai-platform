"""Local-dev seed: creates one demo org + one demo user.

Gated by EAIP_SEED_DEMO=1 so it cannot run unintentionally in production
(the script will refuse to run when EAIP_ENVIRONMENT=production).

Usage::

    EAIP_SEED_DEMO=1 uv run python -m app.scripts.seed
"""

from __future__ import annotations

import asyncio
import os
import sys

from sqlalchemy import select, text

from app.core.config import Environment, get_settings
from app.core.logging import get_logger, setup_logging
from app.infra.db import get_sessionmaker
from app.modules.identity import service as identity_service
from app.modules.identity.models import Organization, User
from app.modules.identity.security import hash_password

DEMO_EMAIL = "demo@local"
DEMO_PASSWORD = "demo1234"  # noqa: S105 — local-only fixture
DEMO_ORG_SLUG = "demo"


async def _seed() -> None:
    settings = get_settings()
    setup_logging(settings)
    logger = get_logger("app.scripts.seed")

    if settings.environment == Environment.PRODUCTION:
        logger.error("seed.refused", reason="EAIP_ENVIRONMENT=production")
        sys.exit(2)
    if not settings.seed_demo and os.environ.get("EAIP_SEED_DEMO") not in {"1", "true", "yes"}:
        logger.error("seed.refused", reason="EAIP_SEED_DEMO is not set")
        sys.exit(2)

    sm = get_sessionmaker()
    async with sm() as session:
        async with session.begin():
            # Bypass RLS for the seed — we are creating the very first
            # org/user pair in an empty database.
            await session.execute(text("SELECT set_config('app.bypass_rls', 'on', true)"))
            user = (
                await session.execute(select(User).where(User.email == DEMO_EMAIL))
            ).scalar_one_or_none()
            if user is None:
                user = User(
                    email=DEMO_EMAIL,
                    display_name="Demo User",
                    password_hash=hash_password(DEMO_PASSWORD),
                )
                session.add(user)
                await session.flush()
                logger.info("seed.user.created", user_id=str(user.id))

            org = (
                await session.execute(
                    select(Organization).where(Organization.slug == DEMO_ORG_SLUG)
                )
            ).scalar_one_or_none()
            if org is None:
                org = await identity_service.create_org(
                    session,
                    slug=DEMO_ORG_SLUG,
                    name="Demo Organization",
                    creator_user_id=user.id,
                    is_test=True,
                )
                logger.info("seed.org.created", org_id=str(org.id))
            else:
                logger.info("seed.org.exists", org_id=str(org.id))

    print(f"\nDemo credentials:\n  email:    {DEMO_EMAIL}\n  password: {DEMO_PASSWORD}\n  org slug: {DEMO_ORG_SLUG}\n")


if __name__ == "__main__":
    asyncio.run(_seed())
