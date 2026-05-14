"""End-to-end RAG demo: upload, wait, search.

Demonstrates the full async pipeline working together:

    1. Seed a fresh org + user (RLS bypassed).
    2. Upload a multi-paragraph document via the service layer.
    3. Emit document.uploaded.v1 into the outbox.
    4. The outbox publisher ships it to Kafka.
    5. The ingestion worker consumes, chunks, INSERTs rows, emits
       document.chunked.v1 per chunk.
    6. The embedding worker consumes, calls OpenAI, UPDATEs each
       chunk's vector, marks the document 'ready' when done.
    7. This script polls until status=ready, then runs three
       semantic searches and prints the top hits with scores.

Prerequisites (3 terminals):
    A: uv run python -m app.workers.ingestion
    B: uv run python -m app.workers.embedding
    C: uv run python app/scripts/demo_rag_search.py
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
from app.modules.identity.models import Membership, Organization, User  # noqa: E402
from app.modules.identity.security import hash_password  # noqa: E402
from app.modules.rag import service as rag_service  # noqa: E402
from app.modules.rag.embeddings import build_default_provider  # noqa: E402
from app.modules.rag.events_outbox import emit_document_uploaded  # noqa: E402
from app.modules.rag.models import DocumentStatus  # noqa: E402
from app.modules.rag.storage import get_storage  # noqa: E402

SAMPLE_DOCUMENT = textwrap.dedent("""\
    # Engineering Handbook

    ## Database guidelines

    We use PostgreSQL 16 as our primary store across all services and rely on
    its mature feature set rather than reaching for specialized databases
    when avoidable. Migrations are managed with Alembic and run on application
    startup in development, but as a pre-deploy step in production where they
    are gated behind manual approval for any change that touches existing
    columns. Row-level security policies enforce tenant isolation at the
    database layer, so every query implicitly filters by the current
    organization. We avoid stored procedures and triggers except for the
    audit trigger on the outbox table, which captures every insert for
    compliance. For full-text search we use Postgres's built-in tsvector
    type; for vector search we use the pgvector extension with HNSW indexes
    tuned for our typical embedding dimensions. Backups run nightly with
    point-in-time recovery enabled, retained for thirty-five days.

    ## API design

    All endpoints are versioned under /v1 and follow RFC 7807 (Problem
    Details for HTTP APIs) for error responses. We return JSON by default
    and prefer flat response shapes over deeply nested structures to keep
    client deserialization simple. Pagination uses limit and offset query
    parameters for normal endpoints; cursor pagination is reserved for
    high-cardinality endpoints where the result set may exceed ten thousand
    items. Authentication is JWT-based with refresh tokens stored in the
    database for revocation; access tokens are short-lived at fifteen
    minutes. Rate limiting is applied per organization, not per user,
    because we want noisy tenants to be visible to ops without surprising
    individual end users. Every endpoint must declare its required
    permission in the route decorator, and the permission must exist in
    the RBAC policy table.

    ## On-call rotation

    Engineers rotate through on-call every six weeks, with each rotation
    lasting one full week from Monday morning through the following Monday
    morning. The primary on-call handles all alerts during business hours
    Monday through Friday. The secondary on-call covers nights, weekends,
    and acts as backup when the primary is unavailable. Pages should be
    acknowledged within five minutes and resolved or escalated within
    fifteen. If you cannot resolve within thirty minutes, escalate to the
    engineering manager regardless of severity. The on-call handoff
    happens every Monday at ten in the morning local time, with a fifteen
    minute meeting covering open incidents, pending alerts, and any
    knowledge that needs to transfer. Compensation for on-call weeks is
    paid as a flat stipend plus time off in lieu for any page handled
    outside business hours.

    ## Cost management

    Every service has an assigned monthly budget tracked in the cost
    dashboard. Service owners review actual spend versus budget weekly
    during the Friday afternoon review meeting. Anomalies above twenty
    percent of the previous week's spend trigger a Slack alert in the
    finance channel. Larger anomalies above fifty percent require an
    incident-style postmortem regardless of whether the spend was
    intentional, because unexpected spending is itself a signal that
    something changed. Reserved instances for our cloud compute are
    purchased annually and reviewed quarterly. Storage tiering is
    automated: any blob unaccessed for ninety days moves to cold storage,
    and unaccessed for one year moves to archive. The finance team
    publishes a monthly cost report to the whole engineering organization
    every first Monday.
""").encode("utf-8")


QUERIES = [
    "What database does the team use?",
    "How do we handle errors in our API?",
    "How often does the on-call rotation change?",
]


async def _seed() -> tuple[uuid.UUID, uuid.UUID]:
    sm = get_sessionmaker()
    suffix = uuid.uuid4().hex[:8]
    async with sm() as session:
        async with session.begin():
            await session.execute(text("SELECT set_config('app.bypass_rls', 'on', true)"))
            user = User(
                email=f"demo-{suffix}@example.com",
                display_name=f"demo-{suffix}",
                password_hash=hash_password("demo-only-not-real"),
            )
            org = Organization(slug=f"demo-{suffix}", name=f"Demo {suffix}", is_test=True)
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
                session,
                storage,
                org_id=org_id,
                created_by=user_id,
                name="engineering-handbook.md",
                mime_type="text/markdown",
                data=SAMPLE_DOCUMENT,
            )
            await emit_document_uploaded(session, event)
            return doc.id


async def _wait_for_ready(org_id: uuid.UUID, document_id: uuid.UUID, timeout: float) -> int:
    """Poll until status=ready. Returns the final chunk_count."""
    sm = get_sessionmaker()
    deadline = asyncio.get_event_loop().time() + timeout
    last_status = None
    while True:
        if asyncio.get_event_loop().time() > deadline:
            raise TimeoutError(f"document {document_id} never reached 'ready' (last={last_status})")
        async with sm() as session:
            async with session.begin():
                await session.execute(
                    text("SELECT set_config('app.current_org', :org, true)"),
                    {"org": str(org_id)},
                )
                doc = await rag_service.get_document(session, document_id=document_id)
        if doc is None:
            raise RuntimeError(f"document {document_id} disappeared")
        if doc.status != last_status:
            print(f"    status: {doc.status}")
            last_status = doc.status
        if doc.status == DocumentStatus.READY:
            return doc.chunk_count or 0
        if doc.status == DocumentStatus.FAILED:
            raise RuntimeError(f"document failed: {doc.error_message}")
        await asyncio.sleep(0.5)


async def _search(org_id: uuid.UUID, query: str, limit: int = 3) -> list[tuple[float, str]]:
    sm = get_sessionmaker()
    embedder = build_default_provider()
    try:
        async with sm() as session:
            async with session.begin():
                await session.execute(
                    text("SELECT set_config('app.current_org', :org, true)"),
                    {"org": str(org_id)},
                )
                hits = await rag_service.search_chunks(
                    session, embedder, query=query, limit=limit
                )
        return [(h.score, h.chunk.text) for h in hits]
    finally:
        await embedder.aclose()


def _truncate(s: str, n: int = 110) -> str:
    s = " ".join(s.split())  # collapse whitespace
    return s if len(s) <= n else s[: n - 1] + "…"


async def main() -> int:
    print("=" * 70)
    print("RAG end-to-end demo")
    print("=" * 70)

    print("\n[1] Seeding org + user...")
    org_id, user_id = await _seed()
    print(f"    org_id={org_id}")
    print(f"    user_id={user_id}")

    print("\n[2] Uploading document (4 paragraphs, markdown)...")
    document_id = await _upload(org_id, user_id)
    print(f"    document_id={document_id}")
    print(f"    bytes={len(SAMPLE_DOCUMENT)}")

    print("\n[3] Waiting for workers to process (status transitions)...")
    chunk_count = await _wait_for_ready(org_id, document_id, timeout=60.0)
    print(f"    READY with chunk_count={chunk_count}")

    print("\n[4] Running semantic searches:")
    for i, q in enumerate(QUERIES, 1):
        print(f"\n    Query {i}: {q!r}")
        hits = await _search(org_id, q, limit=3)
        if not hits:
            print("      (no hits)")
            continue
        for rank, (score, chunk_text) in enumerate(hits, 1):
            print(f"      #{rank}  score={score:.3f}  {_truncate(chunk_text)}")

    print()
    print("=" * 70)
    print("DEMO OK")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))