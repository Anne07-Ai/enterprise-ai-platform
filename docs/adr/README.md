# Architecture Decision Records

This directory captures load-bearing technical decisions in append-only form. The premise: every choice has alternatives, and the value of an ADR is in being honest about which alternatives were considered, why they were rejected, and what the chosen path costs us.

## Format

Every ADR follows the same structure:

```
# ADR-NNNN: <Title>

## Status
Accepted — YYYY-MM-DD

## Context
The forces at play. What made this decision necessary.

## Decision
The decision in 1–2 sentences, then justification.

## Alternatives Considered
Each alternative with a specific reason for rejection — not "didn't fit our needs" but the actual constraint it failed.

## Consequences
Positive: ...
Negative: ...
Neutral: ...

## References
Links to docs, papers, blog posts that informed the decision.
```

## Lifecycle

- **Proposed** — open for discussion, typically as a draft PR.
- **Accepted** — merged. From this point the ADR is immutable.
- **Superseded by ADR-NNNN** — a newer ADR replaces this decision. The original is left in place; the supersession is recorded in both.
- **Deprecated** — the decision is no longer in force but no replacement was needed (the feature was removed).

## Index

| ID    | Title                                                        | Status   |
| ----- | ------------------------------------------------------------ | -------- |
| 0001  | [Modular monolith vs microservices](0001-modular-monolith-vs-microservices.md) | Accepted |
| 0002  | [pgvector vs dedicated vector DB](0002-pgvector-vs-dedicated-vector-db.md)     | Accepted |
| 0003  | [SSE vs WebSockets for streaming](0003-sse-vs-websockets-for-streaming.md)     | Accepted |
| 0004  | [Tenant isolation with Postgres RLS](0004-tenant-isolation-with-postgres-rls.md) | Accepted |
| 0005  | [LLM provider abstraction, Anthropic-first](0005-llm-provider-abstraction-anthropic-first.md) | Accepted |

## Adding a new ADR

1. Copy an existing ADR as a template.
2. Increment the number.
3. Open as a PR with status `Proposed`.
4. Discuss in PR comments, not in the ADR text itself.
5. On merge, flip status to `Accepted` with the merge date.
