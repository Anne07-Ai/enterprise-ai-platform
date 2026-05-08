# Security policy

## Reporting a vulnerability

If you find a security issue, please report it privately rather than filing a public issue. Email `security@example.com` (replace with the maintainer's contact when forking) with:

- A description of the issue and the impact you believe it has.
- Reproduction steps, ideally as a minimal script or curl invocation.
- Affected component (api, web, ingestion worker, embedding worker, agent runtime, infra).
- Affected version or commit SHA.

You will receive an acknowledgement within three business days. A fix or mitigation timeline will follow within ten business days, depending on severity. Coordinated disclosure is the default; we will credit reporters who want it.

## Scope

In scope:

- The application code under `apps/` and `workers/`.
- The infrastructure configuration under `infra/`, `observability/`, and `docker-compose.yml`.
- Dependencies declared in `pyproject.toml`, `package.json`, and lockfiles.

Out of scope:

- Issues that require a malicious operator with shell access to a production node.
- Denial-of-service via raw resource exhaustion that would affect any comparable system.
- Issues in third-party services (Anthropic, OpenAI, AWS) themselves; report those upstream.

## Threat model

The platform is multi-tenant. The primary security objective is that no tenant can read or write another tenant's data, observe another tenant's traces, or exhaust another tenant's quota. Secondary objectives are to prevent secret exfiltration via prompt injection, unsafe tool calls, or LLM-mediated data egress.

Concrete protections:

- **Tenant isolation** — Postgres row-level security plus app-layer `org_id` predicates plus a CI lint that rejects raw SQL missing the predicate. See [ADR-0004](docs/adr/0004-tenant-isolation-with-postgres-rls.md).
- **Trace isolation** — `org_id` is a span attribute on every span; Grafana datasources filter by it for non-admin viewers.
- **Quota** — per-org rate limits at the API gate, per-org concurrency caps in the LLM gateway, per-org Kafka quota in Redpanda.
- **Tool sandboxing** (later phases) — tool calls run in a worker with no inbound network and explicit allowlists for outbound calls.
- **Prompt injection mitigations** — system prompts are versioned and signed; user-controlled content is delimited and never executed as instructions; tool calls require an LLM-emitted intent matching a registered tool name.
- **Secret hygiene** — secrets are env-only, never committed, surfaced via the orchestrator's secret manager in production.

## Local development credentials

Every credential in `.env.example` is a placeholder. Do not deploy this stack to a public network without rotating every value first. The defaults are intentionally obvious so a misconfiguration is loud.

## Dependency management

Dependencies are pinned in lockfiles (`uv.lock`, `pnpm-lock.yaml`). Renovate or Dependabot (configured in a later phase) opens PRs against these lockfiles weekly; security advisories are merged within seven days of a fix being available.
