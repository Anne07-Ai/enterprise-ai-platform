# Architecture

The API is a FastAPI modular monolith with separate document ingestion and
embedding workers. The local Compose stack provides Postgres with pgvector,
Redis, Redpanda and S3-compatible storage. Identity, documents and chat are
implemented in the API. The [roadmap](ROADMAP.md) tracks the web client,
production infrastructure and persistent conversations.

## Identity and tenant isolation

The identity module owns organizations, users, memberships, API keys and
refresh tokens. Auth middleware derives the active organization from the
validated principal. Request sessions bind `app.current_org` with
transaction-local `set_config`. Migrations install row policies and
`FORCE ROW LEVEL SECURITY` on protected tables. Scoped application queries
also use organization predicates where implemented.

Internal identity operations can use a transaction-local `app.bypass_rls`
setting before the tenant is known. The outbox publisher uses the same
trusted setting within each transaction to process batches across tenants
and include system events without an organization.
The runtime login is separate from the schema owner. New physical
connections reject a login with access to a superuser or `BYPASSRLS` role.
See [Database roles](docs/database-roles.md) for runtime grants and local setup.
PostgreSQL superusers and roles with `BYPASSRLS` bypass those policies even
when `FORCE ROW LEVEL SECURITY` is set. See [PostgreSQL RLS documentation](https://www.postgresql.org/docs/16/ddl-rowsecurity.html)
and the current verification status in [Known Issues](KNOWN_ISSUES.md).
[ADR-0004](docs/adr/0004-tenant-isolation-with-postgres-rls.md) describes the
design. Its proposed tenant SQL lint is outside the current CI workflow.

## Document flow

1. The upload handler stores bytes in S3-compatible storage and stages
   `document.uploaded.v1` in the Postgres outbox with the document metadata.
2. The ingestion worker extracts and chunks the document and stages
   `document.chunked.v1` events.
3. The embedding worker calls the configured OpenAI embedding provider,
   stores vectors on the chunks and stages `document.ready.v1` when ready.
4. Search embeds the query and retrieves chunks using pgvector cosine
   similarity.

The outbox supports at-least-once delivery. Ingestion and embedding handlers
have replay guards. Production recovery, provider fallback and DLQ replay
remain operational work.

## Chat flow

The API offers synchronous JSON and SSE chat. It runs a bounded tool loop
against the Anthropic SDK with document search and retrieval tools. Provider
calls occur during the chat request or stream. SSE carries progress and
citations to the caller. See [ADR-0003](docs/adr/0003-sse-vs-websockets-for-streaming.md)
and [ADR-0007](docs/adr/0007-no-langgraph-direct-react-loop.md).

## Operations

Redis backs request idempotency and per-IP and per-organization rate limits.
The API exposes liveness and dependency readiness probes. Readiness checks
Postgres, Redis and Kafka and returns HTTP 503 with failed dependency names
when a probe fails. The checks use fixed dependency names in problem details.
The repository includes OpenTelemetry and local Grafana, Prometheus, Loki
and Tempo configuration. Production SLOs, end-to-end trace isolation and alerting
require separate validation.

## Non-goals

Billing, model training and hosting, a general-purpose workflow engine, a
dedicated vector database and federated cross-workspace search are outside
the current scope.
