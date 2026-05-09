"""factory-boy factories for tests.

These are async-aware: they emit dicts/objects but don't write to the DB
themselves. Tests pass them through ``identity.service`` to hit the real
service layer (which writes via the repository).
"""

from __future__ import annotations

import factory
from faker import Faker

fake = Faker()


class OrgKwargsFactory(factory.DictFactory):
    slug = factory.LazyAttribute(lambda _: fake.unique.slug()[:32])
    name = factory.LazyAttribute(lambda _: fake.company())
    is_test = True


class UserKwargsFactory(factory.DictFactory):
    email = factory.LazyAttribute(lambda _: fake.unique.email())
    display_name = factory.LazyAttribute(lambda _: fake.name())
    password = "test-password-1234"  # noqa: S105
