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

Current controls include organization-scoped request transactions, Postgres
row policies and explicit application predicates where implemented.
PostgreSQL role configuration is part of the isolation boundary.
See [Architecture](ARCHITECTURE.md) for the role contract and
[Known Issues](KNOWN_ISSUES.md) for its verification status.

Redis-backed request limits cover IP and organization buckets. The chat
loop exposes registered document tools and bounds the number of tool
iterations. Tool sandboxing, comprehensive tenant trace isolation and
provider-side quota controls remain production hardening work.

## Local development credentials

Every credential in `.env.example` is a placeholder. Do not deploy this stack to a public network without rotating every value first. The defaults are intentionally obvious so a misconfiguration is loud.

## Dependency management

The API dependencies are pinned in `apps/api/uv.lock`. Automated dependency
updates are planned.
