# Limitations

pg-koldstore keeps PostgreSQL authoritative for hot rows and stores flushed rows
as Parquet plus manifest metadata. That boundary is important: cold row values
can be preserved in cold files, but PostgreSQL-owned indexes remain attached to
rows that still live inside PostgreSQL.

## Custom and Extension Indexes

PostgreSQL indexes do not move to cold storage. When a flush writes eligible
rows to cold files and removes those rows from the hot table, PostgreSQL removes
their index entries too.

This applies to built-in indexes, custom indexes, and extension-owned indexes.
Kalam does not automatically translate those indexes into object-storage
indexes over Parquet files.

## pgvector

pgvector indexes such as HNSW and IVFFlat speed vector similarity search over
rows in a PostgreSQL table. IVFFlat splits vectors into lists and searches
nearby lists; HNSW builds a graph for approximate nearest-neighbor search. Both
index entries point to rows that are still resident in PostgreSQL.

When Kalam flushes old rows to cold storage:

```text
PostgreSQL hot table: row removed
pgvector index: row removed from index
Cold Parquet: row values retained in cold storage
```

The result is intentionally strict:

- Hot rows remain searchable through pgvector.
- Cold rows are not part of pgvector HNSW or IVFFlat indexes after they are
  flushed.
- Vector columns require explicit Kalam type support before they can be flushed
  safely; v0.1 does not yet include pgvector's `vector` type in the supported
  type matrix.

For v1 behavior, vector search should be treated as hot-only unless a
Kalam-managed cold-vector mode is explicitly enabled.

## ParadeDB and BM25

ParadeDB and BM25-style indexes follow the same boundary. They index data that
is resident in PostgreSQL. They do not automatically index Kalam's external
Parquet cold files.

Kalam's current product promise is ordinary PostgreSQL app tables that can
retain history cheaply, not that every PostgreSQL extension index follows rows
into object storage.

## Supported Search Modes

### Hot-only search

This is the default and safest v1 behavior for pgvector queries:

```sql
SELECT *
FROM documents
ORDER BY embedding <-> $query_embedding
LIMIT 20;
```

That query searches only rows still hot in PostgreSQL. It is a good fit for
recent messages, recent memories, active user documents, and fresh
recommendations. It is not a complete search over archived cold history.

### Cold exact scan

For narrow filters with a small amount of cold data, Kalam can later support
exact vector scans by reading candidate Parquet segments, computing distances in
Rust, and merging cold top-k results with hot pgvector results.

This can work for user-scoped queries such as:

```sql
WHERE user_id = 'u_123'
  AND created_at > now() - interval '1 year'
```

It is not appropriate for global semantic search over all users or millions of
cold vectors.

### Cold vector side index

The future path is a Kalam-managed cold-vector engine. On flush, Kalam can write
a segment-level sidecar vector index next to each Parquet segment:

```text
s3://bucket/kalam/documents/user_id=u1/segment-001.parquet
s3://bucket/kalam/documents/user_id=u1/segment-001.usearch
s3://bucket/kalam/documents/user_id=u1/segment-001.manifest.json
```

Cold vector search would then:

1. Use the manifest to choose candidate cold segments.
2. Search the segment-level sidecar index.
3. Fetch matching rows from Parquet.
4. Merge cold results with hot pgvector results.

USearch is the current preferred candidate for this file-backed custom vector
index. Other embedded index implementations may be evaluated later, but the
important design rule is that cold vector indexes are Kalam-owned files, not
pgvector indexes moved out of PostgreSQL.

## Design Rule

Cold vector search should start segment-based, not as one giant global cold
index. A practical layout is per table, per user or tenant, and per time
segment:

```text
documents/user_id=123/year=2026/month=01/segment-0001.parquet
documents/user_id=123/year=2026/month=01/segment-0001.usearch
```

That keeps rebuilds, compaction, deletes, and tenant-scoped search manageable.

## Behavior Summary

| Feature | Hot rows | Cold rows |
|---------|----------|-----------|
| Normal SQL select | Yes | Yes, through Kalam cold reader |
| PostgreSQL custom indexes | Yes | No |
| pgvector index search | Yes | No |
| ParadeDB/BM25 index search | Yes | No, unless separately indexed |
| Vector column value | Yes | Planned with explicit type support |
| Exact vector scan | Yes | Possible for narrow scans, slower |
| Approximate vector search | Yes, through pgvector | Future Kalam sidecar index |
