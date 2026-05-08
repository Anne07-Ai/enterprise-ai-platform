# ADR-0004: Tenant isolation with Postgres row-level security and app-layer guards

## Status

Accepted — 2026-05-08

## Context

The platform is multi-tenant from day one: every document, conversation, agent, and usage event belongs to exactly one organization, and cross-tenant reads or writes constitute a data breach. A single missing `WHERE org_id = ?` predicate in any of dozens of query sites is enough to leak. "Be careful in code review" is not a control.

Three structural strategies exist for multi-tenancy in Postgres. **Database-per-tenant** gives strong isolation at high operational cost: a thousand tenants is a thousand databases to migrate, back up, and connection-pool. **Schema-per-tenant** is operationally a little easier but still scales schemas with tenant count, and `pg_dump`, `psql \dt`, and migration tools all become tenant-aware. **Shared schema with `org_id` column** is operationally trivial but pushes the entire correctness burden into application code — exactly the place we cannot afford a single mistake.

Postgres ships with row-level security: a policy is attached to a table, the policy is evaluated for every row read or written, and rows that fail the policy are simply not visible. The policy itself is SQL, so it can reference session-local state. The combination of a shared schema with an `org_id` column and an RLS policy that filters on a session-local GUC gives us the operational simplicity of the shared-schema approach with a hard isolation control at the database layer.

## Decision

Adopt shared-schema multi-tenancy with two layers of isolation:

1. **Postgres row-level security** on every tenant-scoped table. The policy filters on `current_setting('app.current_org_id', true)::uuid = org_id`. If the GUC is unset, `current_setting('app.current_org_id', true)` returns `NULL`, the comparison is `NULL = org_id`, the predicate is `NULL`, and the row is excluded. The policy fails closed.

2. **App-layer tenant guards** in every query. The data-access layer accepts the tenant id as a parameter and includes `WHERE org_id = :org_id` in every SQL statement. A CI lint based on `sqlfluff` plus a custom rule rejects any new SQL statement against a tenant-scoped table that does not name `org_id` in its `WHERE` or `RETURNING` clause.

The setter is wired into the API request lifecycle. Each request acquires a connection from the pool, opens a transaction, runs `SET LOCAL app.current_org_id = '<uuid>'` from the verified JWT claim, executes the handler, and commits. `SET LOCAL` is scoped to the transaction, so connection pool reuse cannot leak the GUC to the next request. Workers do the same at the start of each message processing block.

Migrations carry the policy with the table: a single migration creates the table, the index on `(org_id, ...)`, the RLS enable, the policy, and the `FORCE ROW LEVEL SECURITY` so even the table owner is subject to the policy. A schema-validation test asserts every tenant-scoped table has both an `org_id` column and a policy.

## Alternatives Considered

**Database-per-tenant.** Rejected because the tenant count is expected to grow into the thousands. A thousand databases is a thousand migration jobs per release, a thousand connection pool slots minimum across the API replicas, a thousand backup jobs, and a thousand things to monitor. Cross-tenant reporting also becomes a federation problem rather than a SQL problem. The isolation guarantee is the strongest, but the operational tax is too steep for the threat model.

**Schema-per-tenant.** Rejected for largely the same reasons as database-per-tenant, with the additional friction that `search_path` manipulation in connection pools is a known footgun and that ORM tooling (Alembic, in our case) treats schemas as a foreign concept.

**App-layer tenant guards only (no RLS).** Rejected because the failure mode is silent: a missing `WHERE` clause does not raise an error, it returns the wrong data. We have a CI lint, but lints catch bugs only in the patterns they know about; ad hoc SQL, JOINs through views, and migrations that backfill data are all places where a lint can miss. RLS is a backstop that does not depend on every developer remembering to type the predicate.

**RLS only (no app-layer guards).** Rejected because we want defense in depth. The app-layer guard catches mistakes RLS would let through, namely an aggregation that joins two tenant tables but only references `org_id` from one — RLS protects each table independently, but a naive query writer might assume the join covers both. The lint that requires `org_id` in queries forces the writer to think about it. RLS plus app-layer is two layers, each catching a different class of mistake.

**Tenant-id as part of every primary key.** Considered as an additional structural protection. Rejected because the cost (compound keys everywhere, FK complexity) outweighed the benefit on top of RLS plus app-layer guards.

## Consequences

Positive: a missed `WHERE` clause is no longer a breach. Cross-tenant queries written by accident return zero rows; cross-tenant writes raise an RLS violation. The DB-layer audit trail is straightforward — every write carries `org_id`, every connection sets `app.current_org_id` at transaction start, and connection pool logs make the binding visible. Operational simplicity matches the shared-schema model: one DB, one set of migrations, one backup, one connection pool.

Negative: every connection acquisition pays the cost of one extra round trip (`SET LOCAL`) at the start of each request. The cost is microseconds against a local Postgres and is irrelevant in practice, but it is a non-zero overhead and is worth being honest about. Triggers, materialized views, and `SECURITY DEFINER` functions all need explicit RLS-aware authoring or they punch through the policy. We document this and review every such object specifically.

Neutral: the policy itself is a piece of SQL that has to be tested. We have a fixture-based test that asserts the policy is enforced for every tenant-scoped table by inserting rows for two tenants and asserting that a session bound to tenant A cannot see tenant B's rows in any of the supported access patterns (SELECT, UPDATE, DELETE, INSERT-with-FK).

## References

- [Postgres documentation — Row Security Policies](https://www.postgresql.org/docs/16/ddl-rowsecurity.html)
- [Crunchy Data — Multi-tenant data isolation with PostgreSQL Row-Level Security](https://www.crunchydata.com/blog/row-level-security-for-tenants-in-postgres)
- [Aurora PostgreSQL multi-tenancy patterns — AWS](https://docs.aws.amazon.com/prescriptive-guidance/latest/saas-multitenant-data-isolation/postgresql-rls.html)
- [Supabase RLS guide](https://supabase.com/docs/guides/database/postgres/row-level-security)
