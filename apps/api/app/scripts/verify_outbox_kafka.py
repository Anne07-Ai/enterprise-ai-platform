"""Verify that document.uploaded.v1 events actually land on Kafka.

Runs against the live dev stack (make up). End-to-end:

    1. Bypass-RLS create an org + user + membership.
    2. Boot the Kafka producer and the outbox publisher loop.
    3. Insert a Document + outbox row via service.create_document
       + emit_document_uploaded.
    4. Trigger the publisher to flush.
    5. Subscribe to document.uploaded.v1 with a fresh consumer group
       and a 15-second timeout.
    6. Validate a received event for our document_id, print it,
       exit 0.

Run from apps/api: uv run python -m app.scripts.verify_outbox_kafka
"""

from __future__ import annotations

import asyncio
import json
import uuid
from contextlib import AsyncExitStack
from datetime import datetime

from aiokafka import AIOKafkaConsumer
from sqlalchemy import text

from app.core.config import get_settings
from app.infra import kafka as kafka_infra
from app.infra import outbox as outbox_infra
from app.infra.db import dispose_engine, get_sessionmaker
from app.modules.identity.models import Membership, Organization, User
from app.modules.identity.security import hash_password
from app.modules.rag import service as rag_service
from app.modules.rag.events import DocumentUploadedV1
from app.modules.rag.events_outbox import emit_document_uploaded
from app.modules.rag.storage import get_storage


async def _seed_org_user() -> tuple[uuid.UUID, uuid.UUID]:
    """Create a one-off org + user with RLS bypassed. Returns (org_id, user_id)."""
    sm = get_sessionmaker()
    suffix = uuid.uuid4().hex[:8]
    async with sm() as session, session.begin():
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
    async with sm() as session, session.begin():
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


async def _consume_one(document_id: uuid.UUID) -> DocumentUploadedV1:
    """Consume from `topic` until we see an event for our document_id or timeout."""
    settings = get_settings()
    group = f"verify-outbox-{uuid.uuid4().hex[:8]}"
    consumer = AIOKafkaConsumer(
        DocumentUploadedV1.TOPIC,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    try:
        async with asyncio.timeout(15):
            await consumer.start()
            async for record in consumer:
                payload: object = json.loads(record.value)
                if isinstance(payload, dict) and payload.get("document_id") == str(document_id):
                    return DocumentUploadedV1.model_validate(payload)
    finally:
        await consumer.stop()

    raise RuntimeError("consumer stopped before the verification event arrived")


async def main() -> int:
    print("=== outbox -> Kafka verification ===")
    print(f"started: {datetime.now().isoformat(timespec='seconds')}")

    async with AsyncExitStack() as stack:
        stack.push_async_callback(dispose_engine)
        stack.push_async_callback(kafka_infra.dispose_producer)
        await kafka_infra.get_producer()
        stack.push_async_callback(outbox_infra.stop_publisher)
        await outbox_infra.start_publisher()
        print("kafka.producer + outbox.publisher started")

        # --- create org, user, document, outbox row ----------------------
        org_id, user_id = await _seed_org_user()
        print(f"seeded: org_id={org_id}, user_id={user_id}")

        document_id = await _create_doc_in_outbox(org_id, user_id)
        print(f"created: document_id={document_id}, outbox row written")

        print("consuming topic 'document.uploaded.v1' (timeout 15s)...")
        payload = await _consume_one(document_id)

        print()
        print("OK: event arrived on Kafka")
        print(payload.model_dump_json(indent=2))
        return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
