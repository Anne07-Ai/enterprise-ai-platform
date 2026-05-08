# Architecture

This document is the long-form companion to the README. It describes what the platform does and, more importantly, where its boundaries are: what it commits to doing well, what it deliberately punts on, and how the pieces are wired so the system stays observable and changeable as it grows.

## System context

In the box: a web app, an HTTP API, a queue, a relational store with vector search, an object store, four async workers, an LLM gateway, and an observability spine. Outside the box: identity providers (the platform federates with the customer's IdP via OIDC and never owns passwords), the LLM provider (Anthropic by default; the gateway abstracts this), the customer's existing storage if they bring their own bucket, and the customer's own dashboards consuming usage events.

A user opens the Next.js app, authenticates via the customer IdP, and lands inside a workspace that belongs to one organization. From there they upload documents, chat over those documents with citations, and (later) compose multi-step agents that call registered tools. The API gateway is the only inbound surface; everything LLM-touching is dispatched asynchronously.

## Bounded contexts

The API is a modular monolith — single deployable, one Python process per replica — with clear module boundaries enforced by package structure and a no-cross-import lint rule. Modules expose typed Pydantic interfaces to one another and never reach into each other's tables.

- **identity**: organizations, users, sessions, OIDC integration. Owns `orgs`, `users`, `memberships`, `api_keys`. Issues JWTs that carry `org_id`, `user_id`, and a short-lived audience claim.
- **workspaces**: a workspace is the unit of access control inside an organization. Owns `workspaces`, `workspace_members`, role definitions.
- **documents**: uploads, parsed chunks, embedding state, retrieval. Owns `documents`, `chunks`, `embeddings` (pgvector column lives on `chunks`).
- **conversations**: chat sessions, messages, streaming tokens, citation records. Owns `conversations`, `messages`, `message_citations`.
- **agents**: agent definitions, tool registry, agent runs, run steps. Owns `agents`, `agent_tools`, `agent_runs`, `agent_run_steps`. The agent runtime is a separate worker, not in-process with the API.
- **usage**: every LLM call, every embedding job, every retrieval emits a usage event. Owns `usage_events`, `usage_aggregates_daily`. Read-only API surface — writes come from Kafka consumers, never from HTTP handlers.

The carve-outs from the monolith are workers, not services in the SOA sense — they share schemas and migrations but run independently because their scaling and failure profiles diverge from the API's: ingestion is CPU-bound on PDF parsing, embedding is GPU-or-batch-API-bound, agent runtime is long-lived and stateful per run, and the LLM gateway needs aggressive provider-aware retry without blocking HTTP threads.

## Data flow — RAG ingestion

A user uploads a PDF. The API streams the bytes to MinIO, writes a `documents` row with `status='uploaded'`, emits `documents.uploaded.v1` to Redpanda, and returns a job handle in under 100 ms. The HTTP handler does no parsing, no chunking, and definitely no embedding.

The ingestion worker consumes `documents.uploaded.v1`, fetches the object from MinIO, parses it (Unstructured for layout, with fallbacks per content type), produces semantic chunks with overlap tuned per document type, writes `chunks` rows with `embedding IS NULL`, updates the document to `status='parsed'`, and emits `documents.parsed.v1`.

The embedding worker consumes `documents.parsed.v1`, batches chunks into provider-appropriate batches (Voyage by default, configurable per workspace), calls the embedding provider with retry and circuit-breaker, writes vectors back into the `chunks.embedding` column, updates the document to `status='ready'`, and emits `documents.embedded.v1` for downstream consumers (e.g. the eval harness re-runs gold-set retrieval after every embedded batch).

Failures at every stage land in a `.dlq` topic with the original payload, error class, retry count, and span context. The DLQ is consumed by an operator UI (later phase) and by alerts on DLQ depth.

See [`docs/diagrams/data-flow-rag.mmd`](docs/diagrams/data-flow-rag.mmd) for the sequence rendered.

## Data flow — chat with streaming

A user posts a message in a conversation. The API persists the user message, performs hybrid retrieval (vector + BM25 via `pg_trgm`) scoped by RLS to the workspace, packs a context window with deduplication, opens an SSE stream to the client, and streams tokens from the LLM gateway as they arrive. Citations are emitted as discrete SSE events alongside token events so the UI can attach them to the rendered text. When the stream completes the API persists the assistant message, the citation set, and emits a `conversations.message.v1` event for usage accounting.

SSE was chosen over WebSockets because the protocol is unidirectional and stateless on the server, surviving CDNs and corporate proxies that mangle `Upgrade` headers. The bidirectional case — agent runs that stream intermediate steps and accept user interrupts — keeps WebSockets in reserve. See [ADR-0003](docs/adr/0003-sse-vs-websockets-for-streaming.md).

## Tenant isolation

Every tenant-scoped table carries an `org_id` column, an index on `(org_id, ...)` matching its hot query patterns, and a Postgres row-level security policy that filters on a session-local GUC: `current_setting('app.current_org_id')::uuid`. The API connection pool sets that GUC at the start of each request via `SET LOCAL app.current_org_id = '<uuid>'` inside a transaction; if the GUC is unset, the policy returns zero rows, so a forgotten setter fails closed. The app layer also carries explicit `WHERE org_id = ?` filters in queries — defense in depth — and a CI lint rejects raw SQL without an `org_id` predicate. A schema-per-tenant or DB-per-tenant model was rejected on operational grounds: migrations, backups, and connection pooling all become quadratic in tenant count. See [ADR-0004](docs/adr/0004-tenant-isolation-with-postgres-rls.md).

## Async-first principle

HTTP handlers complete in under 100 ms or they hand off to a queue. The rule is mechanical: any code path that calls an LLM, embeds text, parses a document, or runs an agent step does not run inside the request thread. Synchronous-feeling endpoints (the chat completion stream, agent run polling) are implemented with SSE and short polls against state that workers maintain. This keeps the API horizontally scalable on cheap CPU replicas, pushes the expensive infrastructure to dedicated worker fleets, and makes timeouts and retries first-class concerns instead of afterthoughts.

## Observability

OpenTelemetry traces propagate from the browser (via `traceparent` headers issued by the Next.js fetch wrapper) through the API, into Kafka headers on every produced message, out to workers that resume the trace on consume, and into LLM provider calls. The collector exports traces to Tempo, metrics to Prometheus, and logs to Loki. Grafana is provisioned with a derived field that turns any `trace_id` in a log line into a clickable jump to the corresponding trace, which is the single most valuable debugging affordance in the entire stack. Every queue boundary records consumer lag and DLQ depth as RED-style metrics.

## Failure modes

- **LLM provider down or rate-limited**: the LLM gateway runs a per-provider circuit breaker with exponential backoff and falls back to a configured secondary provider for completions. Embedding calls retry against a DLQ if all providers fail, so the retry can be replayed cleanly when the provider recovers.
- **Kafka lag**: each consumer group exposes lag as a Prometheus metric; alerts fire at sustained lag > N seconds. Workers autoscale on lag, not CPU.
- **Postgres failover**: Postgres is deployed with streaming replication; the connection pool is configured for fast reconnect, and the API treats a connection error as a 503 with retry-after rather than swallowing it.
- **Runaway tenant**: per-org rate limits at the API gate (token-bucket in Redis), per-org concurrency caps in the LLM gateway, and per-org Kafka quota at Redpanda. Usage events feed a quota service (later phase) that can suspend a tenant from new LLM calls without taking the whole platform down.
- **Poison message**: every consumer is wrapped with a poison-message handler that captures the payload, error, and trace into a DLQ topic and continues. No single bad upload halts ingestion for the workspace.
- **Embedding model swap**: chunks carry an `embedding_model_version` column; retrieval queries filter by version, and a migration job re-embeds in the background without blocking writes.

## Non-goals

To keep this project focused, it explicitly does not attempt:

- **Billing or subscription management.** Usage events are emitted; metering and invoicing are downstream consumer concerns and out of scope.
- **Fine-tuning, model training, or hosting custom models.** The platform consumes hosted LLMs via the provider abstraction. Hosting GPU inference is a separate problem with its own ops surface.
- **A general-purpose workflow engine.** Agents here are AI-centric — tool calls plus retrieval — not Airflow or Temporal replacements.
- **A vector database product.** pgvector is a deliberate choice, not a temporary one. If a tenant outgrows it, the migration is to a managed vector DB at that workspace's level, not platform-wide.
- **A unified search product.** Document retrieval is scoped to a workspace; cross-workspace federation, connector marketplaces, and "search everything" are Glean's product, not this one's.
- **Full enterprise SSO administration UI.** The platform federates via OIDC and trusts the customer's IdP for user lifecycle.

These non-goals are not "yet" items. They are deliberate boundaries that keep the surface area defensible.
