# Enterprise AI Workflow Platform

A multi-tenant platform for building and running production AI workflows over private organizational knowledge — document ingestion with retrieval-augmented chat, structured tool-using agents, per-tenant isolation, usage accounting, and end-to-end observability. Built for engineering teams that need a self-hostable substrate for AI features without rebuilding the same retrieval, streaming, and tenancy plumbing for every internal product.

## Why this exists

ChatGPT Enterprise and Glean solve a slice of this problem as closed SaaS, but offer no extension points for custom agents, tool definitions, or domain-specific evaluation. This repository is the open substrate underneath: an opinionated, observable, multi-tenant runtime that an engineering org can fork and extend, with the boring-but-critical parts — RLS-backed tenancy, async LLM execution, citation-faithful RAG, OTel traces across the whole pipeline — already correct.

## Architecture

![System context](docs/diagrams/system-context.png)

Source: [`docs/diagrams/system-context.mmd`](docs/diagrams/system-context.mmd). Render instructions in [`docs/diagrams/README.md`](docs/diagrams/README.md).

A long-form treatment of bounded contexts, data flows, and failure modes lives in [ARCHITECTURE.md](ARCHITECTURE.md).

## Tech stack at a glance

| Layer          | Choice                                                          | Why                                                            |
| -------------- | --------------------------------------------------------------- | -------------------------------------------------------------- |
| Edge           | Next.js 14 (App Router), Tailwind, shadcn/ui                    | Streaming UI, RSC, sane defaults                               |
| API            | FastAPI (Python 3.12), Pydantic v2, modular monolith            | One deploy, clear boundaries, fast iteration                   |
| AI             | Anthropic (Claude) default, provider abstraction for OpenAI/Bedrock | Vendor flexibility, regulatory choice, cost routing            |
| Data           | Postgres 16 + pgvector, Redis 7, MinIO (S3-compatible)          | One transactional store for metadata + embeddings              |
| Async          | Redpanda (Kafka API), versioned topics with DLQs                | Kafka semantics without ZooKeeper, fast dev boot               |
| Infra          | Docker Compose (dev), Helm + Terraform (prod, later phases)     | Local parity with production primitives                        |
| Observability  | OpenTelemetry → Prometheus, Loki, Tempo, surfaced in Grafana    | One trace from browser → API → Kafka → worker → LLM provider   |

## Quickstart

```bash
git clone <this-repo> && cd enterprise-ai-platform
cp .env.example .env
make up && make healthcheck
```

That boots the full local infrastructure plane (Postgres+pgvector, Redis, Redpanda, MinIO, OTel collector, Prometheus, Loki, Tempo, Grafana) and verifies every service is healthy.

## Project status

Phase 1 — **infrastructure and documentation only**. The platform's application code (FastAPI gateway, Next.js web app, ingestion/embedding/agent workers) lands in subsequent phases. What exists today:

| Area                                | Status      |
| ----------------------------------- | ----------- |
| Repository structure & docs         | Done        |
| ADRs (5)                            | Done        |
| Local infra via Docker Compose      | Done        |
| API service (FastAPI)               | Roadmap     |
| Web app (Next.js)                   | Roadmap     |
| Ingestion / embedding / agent workers | Roadmap   |
| Tenant model + RLS migrations       | Roadmap     |
| RAG retrieval pipeline              | Roadmap     |
| Agent runtime + tool registry       | Roadmap     |
| Eval harness                        | Roadmap     |
| CI/CD, Helm charts, Terraform       | Roadmap     |

Full plan in [ROADMAP.md](ROADMAP.md).

## Architecture deep-dive

See [ARCHITECTURE.md](ARCHITECTURE.md) for bounded contexts, the RAG ingestion data flow, the chat-with-streaming data flow, the tenant isolation strategy, and the failure-mode catalog.

## Decisions

Each ADR captures a load-bearing technical choice and the alternatives rejected.

- [ADR-0001 — Modular monolith over microservices](docs/adr/0001-modular-monolith-vs-microservices.md): one API deploy with clear module boundaries; only carve out services with distinct scaling or failure profiles.
- [ADR-0002 — pgvector over a dedicated vector database](docs/adr/0002-pgvector-vs-dedicated-vector-db.md): one less system to operate, transactional consistency between metadata and embeddings, sufficient up to ~10M chunks.
- [ADR-0003 — SSE over WebSockets for token streaming](docs/adr/0003-sse-vs-websockets-for-streaming.md): unidirectional, proxy-friendly, stateless on the server; WebSockets reserved for bidirectional agent UI.
- [ADR-0004 — Postgres RLS plus app-layer guards for tenancy](docs/adr/0004-tenant-isolation-with-postgres-rls.md): defense in depth, one accidental missing `WHERE` clause does not cause a breach.
- [ADR-0005 — LLM provider abstraction, Anthropic-first](docs/adr/0005-llm-provider-abstraction-anthropic-first.md): thin interface in `apps/api/app/ai/providers/`, swap providers without changing call sites.

## License

[Apache 2.0](LICENSE). © 2026 Lakshmi Anne. Contributions welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).
