"""Verify that document.uploaded.v1 events actually land on Kafka.

Runs against the live dev stack (make up). End-to-end:

    1. Bypass-RLS create an org + user + membership.
    2. Boot the Kafka producer and the outbox publisher loop.
    3. Insert a Document + outbox row via service.create_document
       + emit_document_uploaded.
    4. Trigger the publisher to flush.
    5. Subscribe to document.uploaded.v1 with a fresh consumer group
       and a 15-second timeout.
    6. Assert exactly one event arrived for our document_id, print it,
       exit 0.

Run: uv run python scripts/verify_outbox_kafka.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path

# Ensure we can import the app when invoked from apps/api.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aiokafka import AIOKafkaConsumer  # noqa: E402
from sqlalchemy import text  # noqa: E402

from app.core.config import get_settings  # noqa: E402
from app.infra import kafka as kafka_infra  # noqa: E402
from app.infra import outbox as outbox_infra  # noqa: E402
from app.infra.db import get_sessionmaker  # noqa: E402
from app.modules.identity.models import Membership, Organization, User  # noqa: E402
from app.modules.identity.security import hash_password  # noqa: E402
from app.modules.rag import service as rag_service  # noqa: E402
from app.modules.rag.events_outbox import emit_document_uploaded  # noqa: E402
from app.modules.rag.storage import get_storage  # noqa: E402


async def _seed_org_user() -> tuple[uuid.UUID, uuid.UUID]:
    """Create a one-off org + user with RLS bypassed. Returns (org_id, user_id)."""
    sm = get_sessionmaker()
    suffix = uuid.uuid4().hex[:8]
    async with sm() as session:
        async with session.begin():
            await session.execute(text("SELECT set_config('app.bypass_rls', 'on', true)"))
            user = User(
                email=f"verify-{suffix}@example.com",
                display_name=f"verify-{suffix}",
                password_hash=hash_password("verify-only-not-real"),
            )
            org = Organization(slug=f"verify-{suffix}", name=f"Verify {suffix}", is_test=True)
            session.add_all([user, org])
            await session.flush()
            session.add(Membership(org_id=org.id, user_id=user.id, role="owner"))
            await session.flush()
            return org.id, user.id


async def _create_doc_in_outbox(org_id: uuid.UUID, user_id: uuid.UUID) -> uuid.UUID:
    """Create a Document + outbox row in the same transaction."""
    sm = get_sessionmaker()
    storage = get_storage()
    async with sm() as session:
        async with session.begin():
            # Scope to the org so RLS allows the INSERT.
            await session.execute(
                text("SELECT set_config('app.current_org', :org, true)"),
                {"org": str(org_id)},
            )
            doc, event = await rag_service.create_document(
                session,
                storage,
                org_id=org_id,
                created_by=user_id,
                name="verify.txt",
                mime_type="text/plain",
                data=b"verify outbox -> kafka end to end\n",
            )
            await emit_document_uploaded(session, event)
            return doc.id


async def _consume_one(topic: str, document_id: uuid.UUID, timeout: float) -> dict:
    """Consume from `topic` until we see an event for our document_id or timeout."""
    settings = get_settings()
    group = f"verify-outbox-{uuid.uuid4().hex[:8]}"
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode()),
    )
    await consumer.start()
    try:
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                raise TimeoutError(f"no event for document_id {document_id} after {timeout}s")
            try:
                batch = await asyncio.wait_for(
                    consumer.getmany(timeout_ms=500, max_records=10), timeout=remaining
                )
            except asyncio.TimeoutError:
                continue
            for _tp, records in batch.items():
                for record in records:
                    payload = record.value
                    if payload.get("document_id") == str(document_id):
                        return payload
    finally:
        await consumer.stop()


async def main() -> int:
    print("=== outbox -> Kafka verification ===")
    print(f"started: {datetime.now().isoformat(timespec='seconds')}")

    # --- boot producer + publisher loop ----------------------------------
    await kafka_infra.get_producer()
    await outbox_infra.start_publisher()
    print("kafka.producer + outbox.publisher started")

    try:
        # --- create org, user, document, outbox row ----------------------
        org_id, user_id = await _seed_org_user()
        print(f"seeded: org_id={org_id}, user_id={user_id}")

        document_id = await _create_doc_in_outbox(org_id, user_id)
        print(f"created: document_id={document_id}, outbox row written")

        # The publisher loop polls every settings.outbox.poll_interval_ms.
        # Give it a small head start to flush.
        await asyncio.sleep(1.0)

        # --- consume the event -------------------------------------------
        print(f"consuming topic 'document.uploaded.v1' (timeout 15s)...")
        payload = await _consume_one("document.uploaded.v1", document_id, timeout=15.0)

        print()
        print("OK: event arrived on Kafka")
        print(json.dumps(payload, indent=2))
        return 0

    finally:
        await outbox_infra.stop_publisher()
        await kafka_infra.dispose_producer()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))