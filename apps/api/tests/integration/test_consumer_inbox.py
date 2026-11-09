"""Exercise transactional consumer idempotency using the restricted PostgreSQL role."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from app.infra.consumer_inbox import ConsumerInbox, claim_event
from app.infra.db import get_sessionmaker, session_for_request
from app.infra.outbox import enqueue
from app.modules.identity.repository import delete_org
from app.modules.rag.embeddings import EmbeddingProvider
from app.modules.rag.events import DocumentChunkedV1, DocumentUploadedV1
from app.modules.rag.models import Document, DocumentChunk, DocumentStatus
from app.modules.rag.storage import DocumentStorage
from app.workers.embedding.handler import handle_document_chunked
from app.workers.ingestion import handler as ingestion
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

from tests.integration._helpers import make_user_and_org

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = pytest.mark.integration
_scoped = asynccontextmanager(session_for_request)


async def _seed_chunk_event() -> DocumentChunkedV1:
    owner, org = await make_user_and_org()
    async with _scoped(org_id=org.id) as session:
        document = Document(
            org_id=org.id,
            created_by=owner.id,
            name="sample.txt",
            mime_type="text/plain",
            byte_size=6,
            storage_uri="s3://eaip-documents/sample.txt",
            status=DocumentStatus.EMBEDDING,
        )
        session.add(document)
        await session.flush()
        chunk = DocumentChunk(
            document_id=document.id,
            org_id=org.id,
            chunk_index=0,
            text="sample",
            token_count=1,
        )
        session.add(chunk)
        await session.flush()
        return DocumentChunkedV1(
            org_id=org.id,
            document_id=document.id,
            chunk_id=chunk.id,
            chunk_index=0,
            text=chunk.text,
            token_count=chunk.token_count,
        )


async def _assert_embedding_effects(event: DocumentChunkedV1, *, committed: bool) -> None:
    async with _scoped(org_id=event.org_id) as session:
        document = await session.get(Document, event.document_id)
        chunk = await session.get(DocumentChunk, event.chunk_id)
        assert document is not None and chunk is not None
        assert document.status == (DocumentStatus.READY if committed else DocumentStatus.EMBEDDING)
        assert (chunk.embedding is not None) is committed
        assert await session.scalar(
            select(func.count())
            .select_from(ConsumerInbox)
            .where(ConsumerInbox.event_id == event.event_id)
        ) == int(committed)
        assert await session.scalar(
            text("SELECT count(*) FROM outbox WHERE topic = 'document.ready.v1'")
        ) == int(committed)


async def test_overlapping_duplicate_blocks_then_skips_committed_embedding() -> None:
    event = await _seed_chunk_event()
    entered, release = asyncio.Event(), asyncio.Event()
    embedder = AsyncMock(spec=EmbeddingProvider)

    async def embed(_texts: list[str]) -> list[list[float]]:
        entered.set()
        await release.wait()
        return [[0.1] * 1536]

    embedder.embed.side_effect = embed
    loop = asyncio.get_running_loop()
    first_pid: asyncio.Future[int] = loop.create_future()
    second_pid: asyncio.Future[int] = loop.create_future()

    async def deliver(pid: asyncio.Future[int]) -> None:
        async with get_sessionmaker()() as session, session.begin():
            pid.set_result(await session.scalar(text("SELECT pg_backend_pid()")))
            await handle_document_chunked(
                session, embedder, event_payload=event.model_dump(mode="json")
            )

    async with asyncio.timeout(8), asyncio.TaskGroup() as tasks:
        tasks.create_task(deliver(first_pid))
        await entered.wait()
        duplicate = tasks.create_task(deliver(second_pid))
        leader, waiter = await first_pid, await second_pid
        async with _scoped(org_id=event.org_id) as observer:
            for _ in range(300):
                if await observer.scalar(
                    text("SELECT :leader = ANY(pg_blocking_pids(:waiter))"),
                    {"leader": leader, "waiter": waiter},
                ):
                    break
                await asyncio.sleep(0.01)
            else:
                pytest.fail("PostgreSQL did not report the duplicate waiting for the first claim")
            assert not duplicate.done()
            embedder.embed.assert_awaited_once()
        release.set()

    embedder.embed.assert_awaited_once_with([event.text])
    await _assert_embedding_effects(event, committed=True)


async def test_rollback_discards_receipt_vector_transition_and_outbox_before_replay() -> None:
    event = await _seed_chunk_event()
    embedder = AsyncMock(spec=EmbeddingProvider)
    embedder.embed.return_value = [[0.1] * 1536]
    payload = event.model_dump(mode="json")

    with pytest.raises(RuntimeError, match="test transaction rollback"):
        async with get_sessionmaker()() as session, session.begin():
            await handle_document_chunked(session, embedder, event_payload=payload)
            assert await session.scalar(select(func.count()).select_from(ConsumerInbox)) == 1
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM outbox WHERE topic = 'document.ready.v1'")
                )
                == 1
            )
            raise RuntimeError("test transaction rollback")

    await _assert_embedding_effects(event, committed=False)
    for _ in range(2):
        async with get_sessionmaker()() as session, session.begin():
            await handle_document_chunked(session, embedder, event_payload=payload)

    # Provider calls can repeat after rollback. The committed replay is skipped.
    assert embedder.embed.await_count == 2
    await _assert_embedding_effects(event, committed=True)


async def test_ingestion_terminal_failure_is_atomic_and_replay_is_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner, org = await make_user_and_org()
    async with _scoped(org_id=org.id) as session:
        document = Document(
            org_id=org.id,
            created_by=owner.id,
            name="sample.txt",
            mime_type="text/plain",
            byte_size=6,
            storage_uri="s3://eaip-documents/sample.txt",
            status=DocumentStatus.PENDING,
        )
        session.add(document)
        await session.flush()
        event = DocumentUploadedV1(
            org_id=org.id,
            document_id=document.id,
            created_by=owner.id,
            name=document.name,
            mime_type=document.mime_type,
            byte_size=document.byte_size,
            storage_uri=document.storage_uri,
        )

    async def partially_stage(session: AsyncSession, *_args: object, **_kwargs: object) -> None:
        await enqueue(session, topic="document.chunked.v1", payload={}, org_id=org.id)
        raise RuntimeError("test failure after a partial write")

    process = AsyncMock(side_effect=partially_stage)
    monkeypatch.setattr(ingestion, "_do_ingest", process)
    storage = AsyncMock(spec=DocumentStorage)
    for _ in range(2):
        async with get_sessionmaker()() as session, session.begin():
            await ingestion.handle_document_uploaded(
                session, storage, event_payload=event.model_dump(mode="json")
            )

    process.assert_awaited_once()
    async with _scoped(org_id=org.id) as session:
        document = await session.get(Document, event.document_id)
        assert document is not None and document.status == DocumentStatus.FAILED
        assert await session.scalar(select(func.count()).select_from(ConsumerInbox)) == 1
        assert list(await session.scalars(text("SELECT topic FROM outbox"))) == [
            "document.failed.v1"
        ]


async def test_inbox_rls_denies_cross_tenant_access_without_runtime_mutation_privileges() -> None:
    _, org = await make_user_and_org()
    _, other = await make_user_and_org(email="other@example.com", org_slug="other")
    event_id = uuid4()
    async with _scoped(org_id=org.id) as session:
        assert await claim_event(
            session, org_id=org.id, consumer="embedding-worker", event_id=event_id
        )
        assert not await claim_event(
            session, org_id=org.id, consumer="embedding-worker", event_id=event_id
        )
        assert await claim_event(
            session, org_id=org.id, consumer="ingestion-worker", event_id=event_id
        )

    with pytest.raises(DBAPIError) as denied:
        async with _scoped(org_id=other.id) as session:
            await session.execute(text("SELECT set_config('app.bypass_rls', 'on', true)"))
            assert not list(await session.scalars(select(ConsumerInbox)))
            assert await session.scalar(
                text(
                    "SELECT has_table_privilege('consumer_inbox', 'SELECT') "
                    "AND has_table_privilege('consumer_inbox', 'INSERT') "
                    "AND NOT has_table_privilege('consumer_inbox', 'UPDATE,DELETE,TRUNCATE')"
                )
            )
            await claim_event(
                session, org_id=org.id, consumer="embedding-worker", event_id=event_id
            )
    assert denied.value.orig.sqlstate == "42501"

    async with _scoped(org_id=other.id) as session:
        assert await claim_event(
            session, org_id=other.id, consumer="embedding-worker", event_id=event_id
        )


async def test_deleted_tenant_events_skip_without_inserting_a_receipt() -> None:
    event = await _seed_chunk_event()
    async with _scoped(org_id=event.org_id) as session:
        await delete_org(session, org_id=event.org_id)

    embedder = AsyncMock(spec=EmbeddingProvider)
    async with get_sessionmaker()() as session, session.begin():
        await handle_document_chunked(
            session, embedder, event_payload=event.model_dump(mode="json")
        )
        assert await session.scalar(select(func.count()).select_from(ConsumerInbox)) == 0
    embedder.embed.assert_not_awaited()


@pytest.mark.parametrize("value", [None, "malformed"])
async def test_missing_or_invalid_inbound_event_id_fails_before_effects(value: str | None) -> None:
    event = await _seed_chunk_event()
    payload = event.model_dump(mode="json")
    if value is None:
        payload.pop("event_id")
    else:
        payload["event_id"] = value
    embedder = AsyncMock(spec=EmbeddingProvider)
    with pytest.raises(ValueError):
        async with get_sessionmaker()() as session, session.begin():
            await handle_document_chunked(session, embedder, event_payload=payload)
    embedder.embed.assert_not_awaited()
    await _assert_embedding_effects(event, committed=False)
