"""Exercise the real PostgreSQL outbox and Kafka transport on the test stack."""

from __future__ import annotations

import asyncio
import socket
from contextlib import asynccontextmanager
from datetime import datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer, TopicPartition
from app.core.config import get_settings
from app.infra import db, kafka, outbox
from app.modules.rag.events import DocumentUploadedV1
from app.workers.ingestion import __main__ as ingestion
from sqlalchemy import text

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = pytest.mark.integration


class _AbortTransaction(Exception):
    pass


@pytest_asyncio.fixture
async def kafka_topic() -> AsyncIterator[tuple[str, AIOKafkaConsumer]]:
    topic = f"eaip-test-outbox-{uuid4().hex}"
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=get_settings().kafka.bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    try:
        async with asyncio.timeout(15):
            await consumer.start()
        yield topic, consumer
    finally:
        await consumer.stop()


def _uploaded_event() -> DocumentUploadedV1:
    return DocumentUploadedV1(
        org_id=uuid4(),
        document_id=uuid4(),
        storage_uri="s3://test-bucket/fixture.txt",
        mime_type="text/plain",
        byte_size=7,
        name="fixture.txt",
        created_by=uuid4(),
    )


async def _enqueue(topic: str, event: DocumentUploadedV1, *, rollback: bool = False) -> UUID:
    async with asynccontextmanager(db.session_for_request)(org_id=event.org_id) as session:
        event_id = await outbox.enqueue(
            session,
            topic=topic,
            key=str(event.document_id),
            payload=event.model_dump(mode="json"),
            org_id=event.org_id,
            actor_user_id=event.created_by,
            event_type=event.TOPIC,
        )
        if rollback:
            raise _AbortTransaction
        return event_id


async def _published_at(event: DocumentUploadedV1, event_id: UUID) -> datetime | None:
    async with asynccontextmanager(db.session_for_request)(org_id=event.org_id) as session:
        published_at = await session.scalar(
            text("SELECT published_at FROM outbox WHERE id = :id"), {"id": event_id}
        )
        assert published_at is None or isinstance(published_at, datetime)
        return published_at


async def _assert_record(
    consumer: AIOKafkaConsumer, topic: str, event: DocumentUploadedV1, *, offset: int
) -> None:
    async with asyncio.timeout(15):
        record = await consumer.getone()
    assert record.topic == topic
    assert record.offset == offset
    assert record.key == str(event.document_id).encode()
    assert DocumentUploadedV1.model_validate_json(record.value) == event

    # Check the broker's high-water mark rather than sleeping to detect duplicates.
    tp = TopicPartition(record.topic, record.partition)
    async with asyncio.timeout(15):
        assert (await consumer.end_offsets([tp]))[tp] == offset + 1


async def test_publish_committed_row_and_stop_publisher(
    kafka_topic: tuple[str, AIOKafkaConsumer],
) -> None:
    topic, consumer = kafka_topic
    with pytest.raises(_AbortTransaction):
        await _enqueue(topic, _uploaded_event(), rollback=True)
    event = _uploaded_event()
    event_id = await _enqueue(topic, event)

    await outbox.start_publisher()
    try:
        await _assert_record(consumer, topic, event, offset=0)
    finally:
        async with asyncio.timeout(15):
            await outbox.stop_publisher()
        await kafka.dispose_producer()

    assert await _published_at(event, event_id) is not None
    assert not any(task.get_name() == "outbox-publisher" for task in asyncio.all_tasks())
    assert await outbox._publish_batch() == 0


async def test_unavailable_broker_leaves_row_pending_until_retry(
    kafka_topic: tuple[str, AIOKafkaConsumer], monkeypatch: pytest.MonkeyPatch
) -> None:
    topic, consumer = kafka_topic
    event = _uploaded_event()
    event_id = await _enqueue(topic, event)

    # Reserve a real loopback TCP endpoint without listening, so the producer
    # gets connection refused without interrupting the shared broker.
    with socket.socket() as reserved, monkeypatch.context() as patch:
        reserved.bind(("127.0.0.1", 0))
        patch.setattr(
            get_settings().kafka, "bootstrap_servers", f"127.0.0.1:{reserved.getsockname()[1]}"
        )
        async with asyncio.timeout(15):
            assert await outbox._publish_batch() == 0

    assert await _published_at(event, event_id) is None
    async with asyncio.timeout(15):
        assert await outbox._publish_batch() == 1
    await _assert_record(consumer, topic, event, offset=0)
    assert await _published_at(event, event_id) is not None
    assert await outbox._publish_batch() == 0


async def test_acked_record_is_replayed_if_database_transaction_rolls_back(
    kafka_topic: tuple[str, AIOKafkaConsumer], monkeypatch: pytest.MonkeyPatch
) -> None:
    topic, consumer = kafka_topic
    event = _uploaded_event()
    event_id = await _enqueue(topic, event)

    async def abort_before_commit() -> AsyncIterator[AsyncSession]:
        async with asynccontextmanager(db.session_unscoped)() as session:
            yield session
            raise _AbortTransaction

    with monkeypatch.context() as patch, pytest.raises(_AbortTransaction):
        patch.setattr(outbox, "session_unscoped", abort_before_commit)
        async with asyncio.timeout(15):
            await outbox._publish_batch()

    await _assert_record(consumer, topic, event, offset=0)
    assert await _published_at(event, event_id) is None
    async with asyncio.timeout(15):
        assert await outbox._publish_batch() == 1
    await _assert_record(consumer, topic, event, offset=1)
    assert await _published_at(event, event_id) is not None


async def test_worker_restarts_at_failed_record(
    kafka_topic: tuple[str, AIOKafkaConsumer], monkeypatch: pytest.MonkeyPatch
) -> None:
    topic, _consumer = kafka_topic
    monkeypatch.setattr(ingestion, "DocumentUploadedV1", SimpleNamespace(TOPIC=topic))
    monkeypatch.setattr(ingestion, "CONSUMER_GROUP", topic)

    # Use one key so every event belongs to the same Kafka partition.
    original = _uploaded_event()
    events = [original.model_copy(update={"event_id": uuid4()}) for _ in range(3)]
    for event in events:
        await _enqueue(topic, event)

    async with asyncio.timeout(15):
        assert await outbox._publish_batch() == 3

    processed: list[UUID] = []

    async def fail_second(payload: dict[str, object]) -> None:
        event = DocumentUploadedV1.model_validate(payload)
        processed.append(event.event_id)
        if event.event_id == events[1].event_id:
            raise _AbortTransaction

    monkeypatch.setattr(ingestion, "_process_one", fail_second)
    async with asyncio.timeout(30):
        with pytest.raises(_AbortTransaction):
            await ingestion.run(asyncio.Event())

    assert processed == [event.event_id for event in events[:2]]
    processed.clear()
    stop = asyncio.Event()

    async def finish(payload: dict[str, object]) -> None:
        event = DocumentUploadedV1.model_validate(payload)
        processed.append(event.event_id)
        if event.event_id == events[-1].event_id:
            stop.set()

    monkeypatch.setattr(ingestion, "_process_one", finish)
    async with asyncio.timeout(30):
        await ingestion.run(stop)

    assert processed == [event.event_id for event in events[1:]]
