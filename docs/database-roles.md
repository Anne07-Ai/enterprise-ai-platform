# PostgreSQL login separation

The API, ingestion and embedding workers, outbox publisher, and demo seed
use the application runtime login. Use a separate login for schema
migrations and provisioning. The runtime login must have no superuser or
`BYPASSRLS` privilege, including through memberships, and must not own the
tables. The API checks bypass privileges when it opens a physical database
connection. Restart connection pools when changing database roles.

## Local Compose

1. Start the local infrastructure with `make up`. Run `make api-migrate`
   with `EAIP_DATABASE_URL` set to the local bootstrap login for this command
   only. Keep the schema-owner URL out of the API environment.

   ```bash
   EAIP_DATABASE_URL="$EAIP_LOCAL_MIGRATION_URL" make api-migrate
   ```

2. Connect to the local database as its bootstrap user with `make psql`.
   On first setup create the runtime login, then set its password using
   the psql prompt:

   ```sql
   CREATE ROLE eaip_runtime LOGIN NOSUPERUSER NOBYPASSRLS
       NOCREATEROLE NOCREATEDB NOREPLICATION;
   \password eaip_runtime
   ```

3. From the repository root, with `POSTGRES_USER` and `POSTGRES_DB` set
   to the local stack's values, apply the grants:

   ```bash
   docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
     -v ON_ERROR_STOP=1 -v runtime_role=eaip_runtime < scripts/postgres-runtime-grants.sql
   ```

4. Update the host `EAIP_DATABASE_URL` to use `eaip_runtime` and set
   `POSTGRES_RUNTIME_PASSWORD` for the Compose API profile. Use a distinct,
   URL-safe local runtime password. Start the API and workers and run the demo
   seed with the runtime URL.

Existing local volumes can use the same procedure. Keep the original schema
owner for migrations and grant access to the new runtime login. Avoid blanket
grants on all tables. Direct queries to audit partition children do not
inherit the parent table's RLS policy. Use the partitioned `audit_log` parent.

The application still uses an explicit transaction-local `app.bypass_rls`
setting for trusted identity lookup and seed paths. This is an internal
service boundary. It cannot defend against arbitrary SQL execution by an
attacker using the application login. The internal outbox publisher uses the same transaction-local bypass for its
cross-tenant queue scan. Ordinary request transactions remain tenant scoped.

## Integration tests

The Testcontainers fixture migrates and cleans a disposable `eaip_test`
database as its owner. It creates a separate runtime login for application
queries and applies the same grants file. For an externally provisioned test
stack, use `EAIP_SKIP_CONTAINERS=1`, `EAIP_ENVIRONMENT=test`,
`EAIP_DATABASE_URL` for the restricted runtime login, and
`EAIP_TEST_ADMIN_DATABASE_URL` for its migration and cleanup login. Both
URLs must target the same disposable database with a name beginning
`eaip_test`. Provision the runtime role and grants in advance. Integration
tests truncate data and must only run against this isolated test database.


See [PostgreSQL 16 row security](https://www.postgresql.org/docs/16/ddl-rowsecurity.html)
for the superuser, `BYPASSRLS`, and table ownership rules.
