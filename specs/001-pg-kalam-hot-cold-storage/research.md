# Research: pg-koldstore Hot/Cold Storage

**Branch**: `001-pg-koldstore-hot-cold-storage`
**Date**: 2026-07-03
**Status**: Complete - planning decisions updated after architecture review

## 1. Unified SELECT Architecture: KoldstoreMergeScan

### Decision

Use PostgreSQL Custom Scan for managed-table SELECT. `KoldstoreMergeScan` wraps the best hot heap child path and merges it with cold Parquet streams.

### Rationale

- A managed table is a logical hot+cold relation. A heap-only path is incomplete after flush.
- PostgreSQL Custom Scan is the documented extension point for custom scan paths, plans, and executor state.
- FDWs and views do not preserve the single table name and hot index behavior cleanly enough for this product.

### Important Boundary

KoldstoreMergeScan handles SELECT only. It is not a table access method and it does not give PostgreSQL `ModifyTable` a `ctid` for cold rows.

References:

- https://www.postgresql.org/docs/current/custom-scan-path.html
- https://www.postgresql.org/docs/current/custom-scan-plan.html
- https://www.postgresql.org/docs/current/custom-scan-execution.html

## 2. No `CREATE TABLE ... USING koldstore` in MVP

### Decision

Use normal PostgreSQL `CREATE TABLE`, then `koldstore.migrate_table(...)`.

### Rationale

In PostgreSQL, `CREATE TABLE ... USING method` selects a table access method. pg-koldstore does not replace heap storage; it uses heap plus external cold storage and a Custom Scan read path. Advertising `USING koldstore` would be an architectural lie unless pg-koldstore implemented a full table AM.

### API Outcome

```sql
CREATE TABLE app.items (...);
SELECT koldstore.migrate_table('app.items', 'shared', 'local-minio');
```

Reference:

- https://www.postgresql.org/docs/current/tableam.html
- https://www.postgresql.org/docs/current/sql-createtable.html

## 3. Hot Storage Model

### Decision

The PostgreSQL heap keeps one row per logical primary key. The application primary key remains the heap primary key.

### Rationale

The user requirement is near-native hot DML performance. Rewriting the primary key to `(pk, _seq)` and appending a new heap row for every update would:

- duplicate hot rows
- break native uniqueness expectations
- require more cleanup
- make hot point lookups less native

KalamDB's RocksDB model is append-versioned by sequence. pg-koldstore should learn from its merge/manifest/commit ideas, but not copy that physical hot layout into PostgreSQL heap.

## 4. Commit Sequence

### Decision

Add `_commit_seq bigint` and use it as the durable change-feed cursor. `_seq` remains a row/effect version id.

### Rationale

KalamDB's transaction docs show a single commit seal point where committed rows are stamped with `commit_seq`; Parquet is a later flush path, not the transaction target. pg-koldstore should follow that separation: PostgreSQL heap is the commit target; cold Parquet is later flush.

PostgreSQL sequences are nontransactional and can be allocated in an order different from final commit order. Therefore `_seq` cannot be the external `changes_since` watermark.

Implementation implication:

- the first managed write in a transaction acquires a transaction-scoped pg-koldstore commit-order lock
- `_commit_seq` is allocated while holding that lock and stamped on changed rows/events as part of normal DML
- the lock is held until transaction end, so a later managed transaction cannot commit ahead of an earlier allocated `_commit_seq`
- rollbacks leave harmless `_commit_seq` gaps because the row/event writes roll back
- `changes_since` uses `_commit_seq`

References:

- `../kalamdb/docs/architecture/transactions.md`
- `../kalamdb/backend/crates/kalamdb-transactions/src/commit_sequence.rs`
- https://www.postgresql.org/docs/current/functions-sequence.html

## 5. Cold Reader: Direct Arrow/Parquet, not DataFusion MVP

### Decision

Implement the MVP cold reader in `koldstore-parquet` using Arrow/Parquet and `object_store` directly. Do not add a full DataFusion dependency in MVP.

### Rationale

KalamDB's existing Parquet reader already uses the key pieces directly:

- object-store backed async reader
- projection mask
- selected row groups
- `_seq` range pruning
- PK bloom pruning
- footer/statistics first, then column chunks

That is exactly what pg-koldstore needs for MVP. PostgreSQL remains the SQL planner and executor for joins, aggregates, RLS expression evaluation, and snapshots.

DataFusion can be introduced behind a trait later if benchmarks prove it is worth the binary size and dependency cost.

References:

- `../kalamdb/backend/crates/kalamdb-filestore/src/parquet/reader.rs`
- https://parquet.apache.org/docs/file-format/bloomfilter/
- https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html

## 6. Predicate Pushdown Safety

### Decision

Only proven-safe predicates can prune cold data before merge:

- PK equality / IN
- scope equality
- `_seq` ranges
- `_commit_seq` ranges
- immutable/stat-only columns explicitly marked safe

Mutable app-column filters are residual after merge.

### Rationale

Filtering cold rows by mutable app columns before winner selection can produce wrong answers. A newer hot row may fail the predicate while an older cold row passes; the older row must not leak as the winner. Correctness requires resolving the latest row per PK first, then applying mutable filters.

## 7. DML Semantics

### Decision

Hot-row DML stays native. Cold-only updates require explicit hydration or explicit `lookup_cold => true`. Cold-only deletes can write tombstones from PK metadata because deletes only need the PK.

### Rationale

A partial UPDATE of a cold-only row needs the old cold tuple to reconstruct unchanged columns. Reading Parquet during every DML would violate the near-native hot-path requirement. DELETE is different: a PK-only tombstone is enough to mask old cold rows.

MVP APIs:

- `koldstore.hydrate_pk(table, pk)`
- `koldstore.update_row(table, pk, patch, lookup_cold => true)`
- `koldstore.delete_row(table, pk)`

Standard SQL cold-only UPDATE is out of MVP.

## 8. Tombstone Retention

### Decision

Hot tombstones exist only when a cold segment may contain an older row for the PK. They are not a general append-only history mechanism.

### Rationale

KalamDB's manifest docs already keep latest tombstones hot to mask older cold segments and let compaction later decide removal. pg-koldstore should copy that rule but with one hot row per PK.

Reference:

- `../kalamdb/docs/architecture/manifest.md`

## 9. Manifest and Object-Store Publish

### Decision

The manifest commit is the cold visibility boundary. Do not rely on portable atomic rename.

### Rationale

KalamDB docs describe temp file then rename. That is fine as a local-filesystem mental model, but object-store rename is not portable:

- S3 general-purpose buckets effectively copy/delete for rename-like workflows.
- Some backends support conditional writes or generation checks.
- The Rust `object_store` abstraction cannot guarantee atomic rename everywhere.

pg-koldstore must write temp/final objects using backend-safe publish logic, validate final object metadata, then commit `manifest.json`. Recovery cleans orphan temp files and final files not referenced by the manifest.

References:

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/copy-object.html
- https://docs.aws.amazon.com/AmazonS3/latest/API/API_RenameObject.html
- https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html

## 10. Background Worker

### Decision

Ship a built-in flush scheduler, but document the PostgreSQL preload requirement.

### Rationale

Persistent background workers are registered at server start. That means operators need:

```conf
shared_preload_libraries = 'koldstore'
```

Without preload, the extension still works for SQL-managed flushes, and pg_cron can call `koldstore.flush_pending()` if installed separately.

Reference:

- https://www.postgresql.org/docs/current/bgworker.html

## 11. Security, COPY, and Tooling

### Decision

Security and tooling behavior must be explicit:

- user-scoped reads/writes require `koldstore.user_id`
- RLS/security quals must be enforced on cold rows or fail closed
- `COPY FROM` user-scoped managed tables is rejected under RLS; use INSERT/staging
- `COPY (SELECT ...) TO` is the documented merged export path
- pg_dump is not a full backup for cold objects
- `koldstore_exec('EXPORT TABLE ...')` is the full logical transfer path

References:

- https://www.postgresql.org/docs/current/ddl-rowsecurity.html
- https://www.postgresql.org/docs/current/sql-copy.html

## 12. Constraint Model

### Decision

Native UNIQUE and FK checks are hot-only. Migration rejects risky FK combinations by default when flush is enabled.

### Rationale

PostgreSQL indexes and FK checks do not see object-store Parquet. Keeping the app primary key on the hot heap preserves hot performance but does not create global hot+cold constraints.

## 13. Crate Layout

### Decision

Use six pure/support crates plus the pgrx extension:

- `koldstore-core`
- `koldstore-manifest`
- `koldstore-storage`
- `koldstore-parquet`
- `koldstore-merge`
- `koldstore-catalog`
- `pg_koldstore`

No `koldstore-cold` DataFusion crate in MVP. Parquet read/write/pruning lives in `koldstore-parquet`.

Reference:

- [contracts/crate-layout.md](./contracts/crate-layout.md)

## Resolved Unknowns

| Unknown | Resolution |
|---------|------------|
| SELECT merge architecture | KoldstoreMergeScan Custom Scan. |
| Greenfield DDL | Normal `CREATE TABLE` plus `koldstore.migrate_table`. |
| Hot physical layout | One heap row per PK. |
| Commit-order cursor | `_commit_seq`, not `_seq`. |
| Cold reader | Direct Arrow/Parquet/object_store. |
| Cold-only UPDATE | Explicit hydrate/update API. |
| Cold-only DELETE | Explicit tombstone API using local PK hints; standard SQL only when exact metadata supports it. |
| Tombstone purpose | Mask older cold rows only. |
| Manifest publish | Manifest is visibility boundary; no portable atomic rename assumption. |
| Background worker | Built-in with `shared_preload_libraries`; pg_cron optional. |
