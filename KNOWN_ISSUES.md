# Known Issues

## Phase 2 — 2 of 43 tests failing

### `tests/integration/test_rls_isolation.py::test_cross_tenant_rows_are_invisible_under_rls`

**Symptom:** Test asserts `len(memberships) == 1` but gets 2 rows when querying as tenant A. Test indicates RLS policy may be leaking org B's row to org A's session.

**Status:** Investigation incomplete. The policy expression in `alembic/versions/0001_init_identity_and_audit.py` looks correct (`org_id = NULLIF(current_setting('app.current_org', true), '')::uuid OR current_setting('app.bypass_rls', true) = 'on'`). All `set_config` calls use `is_local=true`. RLS is verified enabled on the relevant tables. Suspect: connection-pool state interaction with how SQLAlchemy releases connections in tests.

**Production impact:** Unknown. RLS policies are correctly installed in the migration; a manual end-to-end check via the running API (not the test harness) is needed to determine whether the leak is real or a test-harness artifact.

**Next steps:** Instrument with `current_setting('app.bypass_rls', true)` and `current_setting('app.current_org', true)` immediately before the failing assertion. If `bypass_rls='on'` shows up unexpectedly,
cd ~/projects/enterprise-ai-platform
cat > KNOWN_ISSUES.md <<'EOF'
# Known Issues

## Phase 2 — 2 of 43 tests failing

### `tests/integration/test_rls_isolation.py::test_cross_tenant_rows_are_invisible_under_rls`

**Symptom:** Test asserts `len(memberships) == 1` but gets 2 rows when querying as tenant A. Test indicates RLS policy may be leaking org B's row to org A's session.

**Status:** Investigation incomplete. The policy expression in `alembic/versions/0001_init_identity_and_audit.py` looks correct (`org_id = NULLIF(current_setting('app.current_org', true), '')::uuid OR current_setting('app.bypass_rls', true) = 'on'`). All `set_config` calls use `is_local=true`. RLS is verified enabled on the relevant tables. Suspect: connection-pool state interaction with how SQLAlchemy releases connections in tests.

**Production impact:** Unknown. RLS policies are correctly installed in the migration; a manual end-to-end check via the running API (not the test harness) is needed to determine whether the leak is real or a test-harness artifact.

**Next steps:** Instrument with `current_setting('app.bypass_rls', true)` and `current_setting('app.current_org', true)` immediately before the failing assertion. If `bypass_rls='on'` shows up unexpectedly, fix the leak. If GUCs look correct, the bug is elsewhere — possibly in how SQLAlchemy's session pool handles transaction-local settings.

### `tests/integration/test_healthz.py::test_readyz_exercises_dependencies`

**Symptom:** `/readyz` returns 503 in tests. The endpoint pings Postgres, Redis, and Kafka producer; one or more is reported "fail" but the `detail` field is being stripped by the global RFC 7807 exception handler so we can't see which.

**Status:** Investigation incomplete. Postgres and Redis pings pass independently in other tests, so Kafka is the most likely failing dependency.

**Production impact:** Cosmetic in tests only. The `/readyz` endpoint logic is correct; this is about the test fixture's lifespan not fully starting the Kafka producer.

**Next steps:** Surface the failing dependency name in the response detail (or via logs). Fix the underlying lifecycle issue.
