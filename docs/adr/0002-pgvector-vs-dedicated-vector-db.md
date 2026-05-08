# ADR-0002: pgvector over a dedicated vector database

## Status

Accepted — 2026-05-08

## Context

The platform's core RAG path needs vector search over document chunks: at query time we run an approximate-nearest-neighbor search over an embedding space, scoped by tenant and workspace, and combine the results with keyword search. The chunks themselves carry a substantial amount of relational metadata — document id, page number, span offsets, chunk type, embedding model version, ingestion timestamp, RLS-enforced tenant id — and that metadata is queried alongside the vector in nearly every retrieval call.

The default industry framing pushes us toward a dedicated vector database (Pinecone, Weaviate, Qdrant, Milvus). The promise is purpose-built indexes, separate scaling, and a managed control plane. The cost is a second source of truth: every chunk now lives in two stores, every write is a distributed transaction we have to invent, and every query that needs both a vector match and a metadata filter pays for an extra hop or duplicates the metadata into the vector store.

The cost calculation is dominated by tenant scale. At a few thousand chunks per tenant and tenants in the low thousands, we are nowhere near the regime where a dedicated vector DB is necessary. At roughly ten million chunks (per the published pgvector HNSW benchmarks against IVFFlat on commodity Postgres hardware) is where pgvector latency starts to lose ground noticeably, and that is also approximately where the memory footprint of the HNSW graph starts to compete seriously with the rest of the working set on a single Postgres instance.

## Decision

Use pgvector inside the same Postgres database that holds the rest of the relational schema. The `chunks` table carries an `embedding vector(1536)` column with an HNSW index parametrized for our dimensionality and distance metric (cosine, since we use embedding models normalized to unit length). Hybrid retrieval combines pgvector ANN search with `pg_trgm` full-text scoring inside a single SQL query.

We commit to pgvector through approximately the ten-million-chunk regime per Postgres instance. Past that, the migration path is a per-workspace cutover to a managed vector DB, with the metadata staying in Postgres and the vector store referenced by foreign id.

## Alternatives Considered

**Pinecone.** Rejected for two reasons. First, the operational simplicity is real but the cost curve at moderate scale is steep, and we would be paying for managed-service margin on workloads that fit comfortably in our existing Postgres. Second, vendor lock-in: Pinecone's API is proprietary, the data model is theirs, and a migration off Pinecone is non-trivial because the index is opaque. Acceptable for a startup that wants to ship in a week, not for a platform we expect to maintain for years.

**Weaviate.** Rejected primarily on operational overhead. Weaviate is a full database to operate, with its own consistency model, its own backup story, its own upgrade path, and its own failure modes that the team would need to learn. The benefit — schema-aware vector search with built-in modules — does not outweigh adding a second database tier on day one.

**Qdrant.** Rejected for the same operational reason as Weaviate, with the additional consideration that the Qdrant filter language is its own system to learn and we would need to reimplement our tenant filters there in addition to in Postgres. The defense-in-depth tenancy story (see [ADR-0004](0004-tenant-isolation-with-postgres-rls.md)) becomes worse, not better, when filters live in two places.

**Milvus.** Rejected on operational scale: Milvus is genuinely excellent at the high end (hundreds of millions of vectors, GPU-accelerated indexes) and genuinely terrible to run at the low end. We are firmly at the low end and will be for the foreseeable future.

**Elasticsearch with dense_vector / OpenSearch k-NN.** Rejected because adopting Elasticsearch for vector search would make Elasticsearch a second primary database, with all the same operational cost as Weaviate or Qdrant, and we would still need Postgres for relational metadata. Two databases versus one.

**SQLite + sqlite-vss for development.** Considered briefly because of the appeal of a zero-dependency dev experience. Rejected because the production target is Postgres, and dev/prod parity matters more than a slightly faster `make up`. We use the same pgvector-enabled Postgres in dev and prod.

## Consequences

Positive: one database, one backup story, one migration system, one connection pool, one set of access controls. Hybrid queries that mix vector similarity with relational predicates run in a single transaction with a single planner. RLS automatically scopes vector search to the tenant. Embedding writes happen in the same transaction that updates document state, so we never have a moment where the metadata says "ready" but the vector store hasn't seen the embeddings.

Negative: at very high scale (low tens of millions of chunks per instance) HNSW build times grow, query latency rises, and the index's memory footprint competes with the rest of the working set. Vacuum and bloat on a vector-heavy table need attention. We also lose access to the latest research-y vector index types as they ship in dedicated systems first.

Neutral: pgvector is improving rapidly. Index types we wanted six months ago (HNSW with proper concurrent build, IVFFlat with predicate pushdown) have shipped. We expect to keep getting closer to dedicated vector DB performance, not further from it.

## References

- [pgvector — Andrew Kane](https://github.com/pgvector/pgvector)
- [HNSW: Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs — Malkov & Yashunin, 2018](https://arxiv.org/abs/1603.09320)
- [Supabase vector benchmarks — pgvector vs Pinecone vs Qdrant](https://supabase.com/blog/pgvector-performance)
- [Crunchy Data on pgvector at scale](https://www.crunchydata.com/blog/topic/ai-llm)
