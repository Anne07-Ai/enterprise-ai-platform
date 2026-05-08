# ADR-0001: Modular monolith over microservices

## Status

Accepted — 2026-05-08

## Context

The platform spans several distinct concerns: identity and tenancy, document ingestion, embedding generation, retrieval, chat with streaming, agent execution, and usage accounting. A naive read of "we have multiple concerns, therefore we need multiple services" would land us in microservices on day one. The team is small (single-digit engineers in the foreseeable future), the system has a single primary database, and the scaling profiles of most modules are similar — request/response over HTTP with bursty but moderate load.

The cost of an early microservices split is well documented: each service edge becomes a place where schemas have to be kept in sync, network failures need handling, end-to-end debugging requires distributed tracing to even start, and data that was a `JOIN` becomes an N+1 fan-out across services. The "distributed monolith" is the failure mode where teams pay all of that cost without the benefits, because the services are still tightly coupled at the data level.

The benefit of microservices is real but specific: independent scaling for components with genuinely different load profiles, and independent failure domains for components whose blast radius we want to contain. That benefit is worth paying for where it applies and worth refusing where it doesn't.

## Decision

The API plane is a single modular monolith — one FastAPI deployment, one Python process per replica, with module boundaries enforced by package structure (`apps/api/app/<module>/`) and a no-cross-import lint rule. We carve out separate deployments only for components with distinct scaling or failure profiles: the LLM gateway (provider-aware retry, circuit breakers, latency-sensitive), the ingestion worker (CPU-bound on PDF parsing), the embedding worker (provider-batch-bound), and the agent runtime (long-lived, stateful per run).

The Postgres database is shared across the API and workers because the data is shared and the alternative — eventual consistency between the API's view of a document and the ingestion worker's — would force us to engineer around our own boundary.

## Alternatives Considered

**Microservices from day one (one service per bounded context).** Rejected because the contexts are not actually independent at the data level: a chat request reads from `conversations`, `messages`, `documents`, `chunks`, and writes a `usage_event`. Splitting these into services replaces a transactional `SELECT` with a fan-out of HTTP calls, none of which can roll back together. The team is also too small to absorb the per-service overhead (CI, deployment, on-call rotation, schema sync) that microservices demand.

**Pure monolith (API and all workers in one process).** Rejected because the workers' failure profiles diverge from the API's. An OOM in PDF parsing should not kill the request handler. A stuck embedding job should not consume API request threads. A long-running agent run cannot live inside an HTTP request lifetime. These are not philosophical concerns, they are operational requirements.

**Service-per-table (extreme microservices).** Rejected for the same reasons as the day-one microservices option, amplified.

**Serverless functions per endpoint.** Rejected because cold starts and the lack of long-lived connections are catastrophic for a system that holds Postgres pools, OTel exporter buffers, and Kafka producers. The dev loop is also worse: hot-reload across functions is friction we don't want.

## Consequences

Positive: one place to look for application logic, one set of migrations, one CI pipeline for the API, transactional consistency by default, and trace continuity is automatic inside the process. New endpoints take an afternoon, not a sprint.

Negative: the deploy unit is larger than any single team would change in a week; we have to pay attention to module boundary discipline because the compiler won't enforce it; scaling the API replicates the entire process even for endpoints that don't need it. The first two are mitigated by the import lint and module ownership in `CODEOWNERS`. The third is mitigated by the fact that FastAPI's per-replica memory footprint is small enough that horizontal scaling is cheap.

Neutral: the workers are separate deployments, so we already have a heterogeneous topology — we are not actually a "pure" monolith and never claim to be. The vocabulary "modular monolith for the API plane, separate deployments for distinct workloads" is precise.

## References

- [Shopify on the modular monolith](https://shopify.engineering/deconstructing-monolith-designing-software-maximizes-developer-productivity)
- [Don't start with microservices, monoliths are your friend — Arnold Galovics](https://arnoldgalovics.com/microservices-in-production/)
- [MonolithFirst — Martin Fowler](https://martinfowler.com/bliki/MonolithFirst.html)
- [The Distributed Monolith — Jonathan Tower](https://www.youtube.com/watch?v=p2GlRToY5HI)
