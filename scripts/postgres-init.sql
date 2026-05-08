-- Postgres initialization for the eaip stack.
-- Mounted into the official entrypoint as /docker-entrypoint-initdb.d/00-extensions.sql
-- and executed exactly once on first volume creation.

-- Required for vector search (ADR-0002).
CREATE EXTENSION IF NOT EXISTS vector;

-- Required for trigram-based fuzzy / hybrid text search.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Required for gen_random_uuid() and encryption helpers used in identity / audit tables.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Case-insensitive text type used for emails and tenant slugs.
CREATE EXTENSION IF NOT EXISTS citext;

-- Application-level GUC used by the RLS policies in ADR-0004. Declaring it here
-- means a fresh database does not error on the first SET LOCAL the API issues.
DO $$
BEGIN
  EXECUTE format('ALTER DATABASE %I SET app.current_org_id TO %L', current_database(), '');
END
$$;
