"""Quick smoke test for the chat agent.

Seeds a fresh org + user, uploads the engineering handbook, runs the
ingestion + embedding workers (assumed already running in separate
terminals), waits for ready, then asks the agent three questions.

Use this BEFORE writing the HTTP endpoint to confirm the agent loop
works end-to-end with real Anthropic + real pgvector search.

Run:
    Terminal A: uv run python -m app.workers.ingestion
    Terminal B: uv run python -m app.workers.embedding
    Terminal C: uv run python app/scripts/test_agent.py
"""
from __future__ import annotations

import asyncio
import sys
import textwrap
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from sqlalchemy import text  # noqa: E402

from app.infra.db import get_sessionmaker  # noqa: E402
from app.modules.chat.agent import run as agent_run  # noqa: E402
from app.modules.identity.models import Membership, Organization, User  # noqa: E402
from app.modules.identity.security import hash_password  # noqa: E402
from app.modules.rag import service as rag_service  # noqa: E402
from app.modules.rag.events_outbox import emit_document_uploaded  # noqa: E402
from app.modules.rag.models import DocumentStatus  # noqa: E402
from app.modules.rag.storage import get_storage  # noqa: E402


SAMPLE_DOCUMENT = textwrap.dedent("""\
    # Engineering Handbook

    ## Database guidelines

    We use PostgreSQL 16 as our primary store across all services and rely on
    its mature feature set rather than reaching for specialized databases
    when avoidable. Row-level security policies enforce tenant isolation at
    the database layer. For vector search we use the pgvector extension
    with HNSW indexes. Backups run nightly with point-in-time recovery
    enabled, retained for thirty-five days.

    ## API design

    All endpoints are versioned under /v1 and follow RFC 7807 for error
    responses. We return JSON by default. Pagination uses limit and offset
    query parameters. Authentication is JWT-based with refresh tokens
    stored in the database for revocation. Rate limiting is applied per
    organization, not per user.

    ## On-call rotation

    Engineers rotate through on-call every six weeks, with each rotation
    lasting one full week from Monday morning. The primary on-call handles
    alerts during business hours. Pages should be acknowledged within five
    minutes and resolved within fifteen. The on-call handoff happens
    every Monday at ten in the morning.

    ## Cost management

    Every service has an assigned monthly budget tracked in the cost
    dashboard. Service owners review actual spend versus budget weekly.
    Anomalies above twenty percent of the previous week's spend trigger
    a Slack alert in the finance channel.
""").encode("utf-8")


QUESTIONS = [
    "What database does the team use, and what's the backup policy?",
    "How long is the on-call rotation and what is the response SLA?",
    "Hi! How are you today?",  # off-topic — should answer without tool use
]


async def _seed() -> tuple[uuid.UUID, uuid.UUID]:
    sm = get_sessionmaker()
    suffix = uuid.uuid4().hex[:8]
    async with sm() as session:
        async with session.begin():
            await session.execute(text("SELECT set_config('app.bypass_rls', 'on', true)"))
            user = User(
                email=f"agent-test-{suffix}@example.com",
                display_name=f"agent-test-{suffix}",
                password_hash=hash_password("not-real-test-pw"),
            )
            org = Organization(slug=f"agent-test-{suffix}", name=f"Agent Test {suffix}", is_test=True)
            session.add_all([user, org])
            await session.flush()
            session.add(Membership(org_id=org.id, user_id=user.id, role="owner"))
            await session.flush()
            return org.id, user.id


async def _upload(org_id: uuid.UUID, user_id: uuid.UUID) -> uuid.UUID:
    sm = get_sessionmaker()
    storage = get_storage()
    async with sm() as session:
        async with session.begin():
            await session.execute(
                text("SELECT set_config('app.current_org', :org, true)"),
                {"org": str(org_id)},
            )
            doc, event = await rag_service.create_document(
                session, storage,
                org_id=org_id, created_by=user_id,
                name="engineering-handbook.md",
                mime_type="text/markdown",
                data=SAMPLE_DOCUMENT,
            )
            await emit_document_uploaded(session, event)
            return doc.id


async def _wait_ready(org_id: uuid.UUID, doc_id: uuid.UUID, timeout: float = 60.0) -> None:
    sm = get_sessionmaker()
    deadline = asyncio.get_event_loop().time() + timeout
    last = None
    while True:
        if asyncio.get_event_loop().time() > deadline:
            raise TimeoutError(f"document {doc_id} never reached ready (last={last})")
        async with sm() as session:
            async with session.begin():
                await session.execute(
                    text("SELECT set_config('app.current_org', :org, true)"),
                    {"org": str(org_id)},
                )
                doc = await rag_service.get_document(session, document_id=doc_id)
        if doc is None:
            raise RuntimeError("document disappeared")
        if doc.status != last:
            print(f"    status: {doc.status}")
            last = doc.status
        if doc.status == DocumentStatus.READY:
            return
        if doc.status == DocumentStatus.FAILED:
            raise RuntimeError(f"document failed: {doc.error_message}")
        await asyncio.sleep(0.5)


async def main() -> int:
    print("=" * 70)
    print("Agent end-to-end test")
    print("=" * 70)

    print("\n[1] Seeding org + user...")
    org_id, user_id = await _seed()
    print(f"    org_id={org_id}")

    print("\n[2] Uploading document...")
    doc_id = await _upload(org_id, user_id)
    print(f"    document_id={doc_id}")

    print("\n[3] Waiting for workers...")
    await _wait_ready(org_id, doc_id)
    print("    READY")

    print("\n[4] Running agent against three questions:")
    sm = get_sessionmaker()
    for i, q in enumerate(QUESTIONS, 1):
        print(f"\n    --- Question {i} ---")
        print(f"    Q: {q}")
        async with sm() as session:
            async with session.begin():
                result = await agent_run(
                    session, org_id=org_id, user_message=q,
                )
        print(f"    iterations: {result.iterations}, truncated: {result.truncated}")
        print(f"    citations: {len(result.citations)}")
        for c in result.citations:
            print(f"      - {c.document_name} #{c.chunk_index} (score={c.score:.3f})")
        print(f"    A: {result.answer}")

    print()
    print("=" * 70)
    print("AGENT TEST OK")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))