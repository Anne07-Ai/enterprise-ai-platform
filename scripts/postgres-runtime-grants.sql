-- Run after migrations as the schema owner. Set runtime_role to a distinct
-- existing login without superuser, BYPASSRLS, or elevated role memberships.
GRANT USAGE ON SCHEMA public TO :"runtime_role";
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE
    public.organizations, public.users, public.memberships, public.api_keys,
    public.refresh_tokens, public.documents, public.document_chunks
    TO :"runtime_role";
GRANT SELECT, INSERT, UPDATE ON TABLE public.outbox TO :"runtime_role";
GRANT SELECT, INSERT ON TABLE public.consumer_inbox TO :"runtime_role";
-- Use the RLS-protected partitioned parent. Never grant access to its children.
GRANT SELECT, INSERT ON TABLE public.audit_log TO :"runtime_role";
