# Enterprise AI Workflow Platform

> Production reference architecture for multi-tenant RAG with tool-using agents.
> FastAPI · Postgres RLS · Kafka workers · Anthropic · OpenAI · pgvector.

A self-hostable runtime for building AI features over private organizational
documents — async ingestion, citation-faithful RAG, tool-using agents, and
SSE-streamed chat — with strict per-tenant isolation enforced at the database
layer.

## What this is

ChatGPT Enterprise and Glean solve a slice of this problem as closed SaaS but
offer no extension points for custom agents, custom tools, or domain-specific
evaluation. This repository is the open substrate underneath: an opinionated,
observable, multi-tenant runtime that an engineering team can fork and
extend.

The boring-but-critical parts are already correct: Postgres row-level
security enforcing tenant isolation on every query, transactional outbox
shipping domain events to Kafka, idempotent async workers handling ingestion
and embedding, OpenTelemetry tracing the whole flow from API to LLM
provider.

## What works today

Verified end-to-end against the local stack:

- **Auth & RBAC** — JWT sessions, per-org RBAC with `chat:create`,
  `documents:read/create/search/delete`, API keys.
- **Tenant isolation** — Postgres RLS bound via
  `set_config('app.current_org', ...)` on every session; bypass GUC for
  service-internal paths.
- **Async ingestion** — upload → outbox → Kafka → ingestion-worker extracts &
  chunks → Kafka → embedding-worker calls OpenAI → pgvector. ~3 seconds from
  upload to ready for a 1KB document.
- **Semantic search** — `POST /v1/documents/search` over pgvector HNSW with
  cosine similarity.
- **Tool-using agent** — Claude Sonnet 4.6 with `search_documents` and
  `get_document` tools, bounded iteration loop, citations carried back to
  the caller. ReAct pattern implemented directly against the Anthropic SDK
  (see `docs/adr/0008-no-langgraph.md`).
- **Chat endpoints** — `POST /v1/chat` (synchronous JSON) and
  `POST /v1/chat/stream` (SSE: `token`, `tool_use`, `tool_result`,
  `citations`, `done`, `error`).
- **Observability** — OpenTelemetry collector, Prometheus, Loki, Tempo,
  Grafana.

## Architecture
Three principles:

1. **One transactional store for everything (Postgres)** including embeddings
   via pgvector. Tenant isolation is a database-layer property, not an
   application-layer convention.
2. **Async by default for anything that talks to a paid API.** Document
   ingestion runs through Kafka workers so API uploads are non-blocking and
   embedding spend is decoupled from request lifetime.
3. **Transparent agent loop.** The ReAct loop is ~80 lines of explicit Python
   against the Anthropic SDK rather than a framework abstraction; this keeps
   the agent debuggable and the cost ceiling visible.

## Tech stack

| Layer         | Choice                                                                          |
| ------------- | ------------------------------------------------------------------------------- |
| API           | FastAPI · Python 3.12 · Pydantic v2 · uv                                        |
| Auth          | JWT (RS256) · per-org RBAC · API keys                                           |
| Data          | Postgres 16 + pgvector · Redis 7 · MinIO (S3-compatible)                        |
| Async         | Redpanda (Kafka API) · transactional outbox · idempotent consumers              |
| AI            | Anthropic Claude Sonnet 4.6 (chat) · OpenAI text-embedding-3-small (embeddings) |
| Observability | OpenTelemetry → Prometheus · Loki · Tempo · Grafana                             |
| Dev infra     | Docker Compose · Colima on macOS · pinned images                                |

## Running locally

Prerequisites: Docker (Colima or Docker Desktop), Python 3.12, `uv`, and an
Anthropic + OpenAI API key.

```bash
git clone https://github.com/Anne07-Ai/enterprise-ai-platform.git
cd enterprise-ai-platform
cp .env.example .env
# edit .env to add EAIP_ANTHROPIC_API_KEY and EAIP_OPENAI_API_KEY

make up
make healthcheck
```

Boots the full local infrastructure plane (Postgres+pgvector, Redis,
Redpanda, MinIO, OTel collector, Prometheus, Loki, Tempo, Grafana) and
verifies every service is healthy.

To run the API and workers (3 separate terminals):

```bash
# Terminal A — ingestion worker
cd apps/api && set -a; source ../../.env; set +a
uv run python -m app.workers.ingestion

# Terminal B — embedding worker
cd apps/api && set -a; source ../../.env; set +a
uv run python -m app.workers.embedding

# Terminal C — API server
cd apps/api && set -a; source ../../.env; set +a
uv run uvicorn app.main:app --port 8000
```

Then seed a demo user and login:

```bash
cd apps/api
EAIP_SEED_DEMO=1 uv run python -m app.scripts.seed
# user: demo@example.com / demo1234
```

## API surface

| Endpoint                          | Auth | Description                                  |
| --------------------------------- | ---- | -------------------------------------------- |
| `POST /v1/auth/login`             | —    | Email + password → JWT access/refresh tokens |
| `POST /v1/documents`              | JWT  | Multipart upload, queues async ingestion     |
| `GET /v1/documents/{id}`          | JWT  | Fetch document metadata + status             |
| `POST /v1/documents/search`       | JWT  | Semantic search over the org's chunks        |
| `POST /v1/chat`                   | JWT  | One-shot tool-using agent turn (JSON)        |
| `POST /v1/chat/stream`            | JWT  | Same agent, SSE-streamed events              |
| `GET /healthz` / `GET /readyz`    | —    | Liveness and readiness probes                |

OpenAPI spec at `GET /openapi.json` when the server is running.

## What's inside
## Design decisions

Architecture Decision Records live in `docs/adr/`:

- **ADR-001** — Module structure and bounded contexts
- **ADR-002** — RLS as the tenant isolation primitive
- **ADR-003** — Outbox pattern for at-least-once event delivery
- **ADR-004** — Versioned topic names (`.v1` suffix)
- **ADR-005** — JWT key handling and rotation strategy
- **ADR-006** — RAG schema and pgvector configuration
- **ADR-008** — No LangGraph; direct ReAct loop against Anthropic SDK
- **ADR-009** — SSE (not WebSocket) for streaming chat

## Roadmap

What's **not** built yet, in honest order:

- Persistent server-side conversation sessions
- Helm chart and Terraform for production deployment (AKS target)
- Next.js frontend (chat UI consuming `/v1/chat/stream`)
- Per-org rate limiting and usage accounting
- Drift detection on embedding model upgrades
- DLQ replay tooling for failed events

Phases 5–12 of the original delivery plan cover these. The current
repository proves the architectural substrate is correct; phases 5+ are
mostly integration and operational work on top.

## License

Apache 2.0. See [LICENSE](LICENSE).
