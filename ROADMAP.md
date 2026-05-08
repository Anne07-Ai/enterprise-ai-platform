# Roadmap

This is an honest list of what exists today and what remains. Phases are sequential, not concurrent — each builds on the last and ends in something demonstrable.

## Phase 1 — Foundation (current)

Status: **Done**.

- Repository structure and module placeholders.
- README, ARCHITECTURE, CONTRIBUTING, SECURITY, ROADMAP, LICENSE.
- Five ADRs covering the load-bearing technical choices.
- System-context and RAG data-flow diagrams (Mermaid source + rendered PNG).
- Local infrastructure plane via Docker Compose: Postgres+pgvector, Redis, Redpanda, MinIO, OTel collector, Prometheus, Loki, Tempo, Grafana.
- `make` interface for the day-to-day: up, down, ps, logs, healthcheck, topics, psql, redis-cli.
- Healthcheck script and Kafka topic seeding script.
- `.github` with CODEOWNERS, PR template, issue templates, and a workflows placeholder.

Demo at end of phase: `make up && make healthcheck` is all green; Grafana shows the three datasources connected; Redpanda console lists the seeded v1 topics.

## Phase 2 — Identity and tenancy

- Postgres migrations for `orgs`, `users`, `memberships`, `workspaces`, `workspace_members`, `api_keys`, `audit_events`.
- RLS policies on every tenant-scoped table; CI lint that rejects new tables without an `org_id` column and policy.
- OIDC integration via `authlib`; JWT issuance with `org_id` and `user_id` claims.
- API skeleton in `apps/api/` (FastAPI, dependency-injected DB session that sets `app.current_org_id`, request-scoped tenant context).
- Web app skeleton in `apps/web/` (Next.js App Router, NextAuth federating to the API's OIDC).
- Integration tests that prove cross-tenant queries return zero rows even when `WHERE org_id` is omitted.

Demo at end of phase: two orgs, two users, login flow works, hitting another org's endpoint returns 404.

## Phase 3 — Document ingestion and retrieval

- Document upload endpoint, MinIO integration, signed URLs.
- Ingestion worker with Unstructured-based parsing and configurable chunking.
- Embedding worker with provider abstraction (Voyage default, OpenAI/Cohere pluggable).
- Hybrid retrieval (pgvector + `pg_trgm`) with reranking.
- Eval harness with a small gold set, scored on retrieval precision and recall.

Demo: upload a PDF, ask a question, get a grounded answer with citations.

## Phase 4 — Streaming chat with citations

- Conversation and message persistence.
- SSE streaming endpoint, citation events interleaved with token events.
- Web UI with streaming markdown render and inline citation chips.
- Usage event emission per LLM call, per retrieval call, per embedding batch.

Demo: a fluid chat experience, citations are clickable and open the source document at the cited chunk.

## Phase 5 — Agents and tool registry

- Agent definitions, tool registry, tool execution sandbox.
- Agent runtime worker with run state machine and step-by-step OTel spans.
- Bidirectional UI for agent runs (this is where the WebSocket from ADR-0003 earns its keep).
- Tool definitions for HTTP fetch, document retrieval, SQL on a sandboxed read replica.

Demo: a multi-step agent answers a question that requires tool calls and the user sees each step stream in.

## Phase 6 — Production readiness

- Helm charts under `infra/helm/` for every service.
- Terraform under `infra/terraform/` for the cloud baseline (managed Postgres, MSK, S3, EKS).
- CI: lint + type + test + build + image push, gated branch protection.
- CD: argo-style or GitOps deploys per environment.
- SLOs published, dashboards alerting on error budget burn.
- Runbooks for the failure modes listed in ARCHITECTURE.md.

Demo: a live deployment to a staging environment, dashboards showing trace continuity from the browser to the LLM provider.

## Out of scope

The non-goals listed in ARCHITECTURE.md remain non-goals. This roadmap will not silently absorb a billing system, a fine-tuning platform, or a federated search product.
