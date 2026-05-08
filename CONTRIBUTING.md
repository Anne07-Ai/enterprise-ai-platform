# Contributing

Thanks for considering a contribution. This project is built in public as a portfolio of production-grade engineering choices, so contributions that sharpen the technical content — better ADRs, clearer diagrams, infrastructure tightening, real bug fixes — are very welcome. Drive-by code-style reformatting and AI-generated PRs without a real bug or feature behind them are not.

## Ground rules

- File an issue before opening a non-trivial PR. Aligning on direction beats discovering rework at review time.
- One concern per PR. Mixing a refactor with a feature with a docs change makes review harder than it needs to be.
- New behavior needs a test. New infrastructure needs a healthcheck. New documentation needs to be linted clean.

## Local setup

```bash
git clone <this-repo> && cd enterprise-ai-platform
cp .env.example .env
make up
make healthcheck
```

`make help` lists every available target.

## Branching and commit style

Branches are short-lived and named with a type prefix that mirrors the commit convention: `feat/agent-runtime-poc`, `fix/dlq-replay`, `docs/adr-0006-eval-harness`, `infra/loki-retention`.

Commits follow [Conventional Commits](https://www.conventionalcommits.org). The types in active use:

- `feat:` — user-visible behavior change.
- `fix:` — bug fix.
- `docs:` — documentation only.
- `infra:` — Compose, Helm, Terraform, observability config.
- `chore:` — tooling, dependencies, no behavior change.
- `refactor:` — internal restructuring with no behavior change.
- `test:` — tests only.

The pre-commit hook checks the message format. Squash-merge is the default; the squashed commit message is the PR title and must follow the convention.

## Code style and quality gates

- Python: `ruff` for lint, `ruff format` for formatting, `mypy --strict` for types. Pre-commit runs all three.
- TypeScript: `eslint`, `prettier`, `tsc --noEmit`. Pre-commit runs all three.
- Shell: every script starts with `set -euo pipefail` and passes `shellcheck`.
- YAML: 2-space indent, no trailing whitespace, ends with a newline. `yamllint` enforces.
- Markdown: passes `markdownlint` with default rules. Headings are sentence case.
- Mermaid diagrams: render with `mmdc` before pushing; the PNG is checked in alongside the `.mmd` source.

## Documentation expectations

- Every load-bearing decision is an ADR under `docs/adr/`. Use the existing format. Status is `Proposed` until merged, then `Accepted`.
- Every long-running operational procedure (failover, DLQ replay, schema migration) is a runbook under `docs/runbooks/`.
- README and ARCHITECTURE updates accompany any change that alters externally visible behavior or module boundaries.

## Testing expectations

- Unit tests are colocated with code under `__tests__/` (TS) or `tests/` (Py).
- Integration tests live under `apps/api/tests/integration/` and run against the Compose stack.
- The eval harness lives under `workers/eval-runtime/` (later phase) and runs on a fixed gold set.

## Reporting security issues

Do not file a public issue. See [SECURITY.md](SECURITY.md) for the disclosure path.

## Code of conduct

Be kind, be specific, be technical. Disagreement is welcome; personal attacks are not. Maintainers reserve the right to lock or remove threads that lose this signal.
