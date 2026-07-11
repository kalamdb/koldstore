# Implementation Plan: pg-koldstore Hot/Cold Storage Extension

**Branch**: `001-pg-koldstore-hot-cold-storage` | **Date**: 2026-07-03 | **Spec**: [spec.md](./spec.md)

## Summary

pg-koldstore is a PostgreSQL extension for managed hot/cold tables. Hot storage remains a normal PostgreSQL heap with one row per primary key. Cold storage is kalamdb-compatible Parquet on object storage. `KoldstoreMergeScan` is the SELECT path that merges hot and cold rows; DML stays near native and uses local cold metadata instead of object-store reads.

Greenfield and existing tables use normal `CREATE TABLE`, then `koldstore.migrate_table(...)`. The spec does not use `CREATE TABLE ... USING koldstore` because that would require a custom PostgreSQL table access method.

## Technical Context

| Area | Decision |
|------|----------|
| Language | Rust/pgrx for extension glue; small C shim for Custom Scan API. |
| PostgreSQL | 15+ minimum; target 16/17 first. |
| Hot storage | PostgreSQL heap, app primary key preserved. |
| Cold storage | Object-store Parquet and kalamdb-compatible manifest. |
| Cold reader | Direct Arrow/Parquet/object_store reader in `koldstore-parquet`; no full DataFusion in MVP. |
| Commit ordering | `_commit_seq` allocated under a transaction-scoped pg-koldstore commit-order lock; `changes_since` uses `_commit_seq`. |
| Scheduling | Built-in background worker when `shared_preload_libraries` includes `koldstore`; SQL/pg_cron fallback. |
| Testing | Rust unit, pgrx/pg_regress, PostgreSQL + MinIO integration, manifest/Parquet golden files. |

## Project Structure

```text
Cargo.toml
koldstore.control
sql/
crates/
  koldstore-core/
  koldstore-manifest/
  koldstore-storage/
  koldstore-parquet/
  koldstore-merge/
  koldstore-catalog/
  pg_koldstore/
    src/
      hooks/
      merge_scan/
      flush/
      migrate/
      sql/
      ffi/
    native/
tests/
docs/
```

See [contracts/crate-layout.md](./contracts/crate-layout.md).

## Architecture

```text
Application SQL
   |
   +-- SELECT --------------------> KoldstoreMergeScan
   |                                  hot heap child
   |                                  + cold Parquet streams
   |                                  + koldstore-merge resolver
   |
   +-- INSERT/UPDATE/DELETE ------> native hot heap path
                                      + pg-koldstore DML hooks
                                      + local cold PK hints
                                      + row_events

Flush worker
   -> scan hot rows
   -> write Parquet
   -> publish manifest
   -> clean hot rows/tombstones only after commit
```

## Complexity Tracking

| Item | Why needed | Boundary |
|------|------------|----------|
| Custom Scan Provider | Managed table SELECT must include cold rows. | SELECT only; no custom table AM. |
| C shim | PostgreSQL Custom Scan structs are low-level. | Isolated in `pg_koldstore/native`. |
| Commit sequence lock | Strict commit-order `_commit_seq` cannot be derived from nontransactional sequences. | Only transactions that mutate managed tables. |
| Local cold PK hints | Avoid object-store reads during DML while preserving tombstone correctness. | Exact hints required for exact rowcount claims. |
| Row event log | Hot heap has one row per PK, so change history cannot live in duplicate heap rows. | Internal retention-managed table. |

DataFusion is not in MVP. The direct Parquet reader is enough for projection, footer stats, bloom pruning, and object-store range reads; this is also closer to the kalamdb reader implementation.

## Implementation Phases

### Phase A - Foundation

Extension scaffold, schemas, catalog tables, GUCs, `koldstore_version()`, `koldstore_user_id()`, `SNOWFLAKE_ID()`, storage registration, type matrix.

### Phase B - Migration and Demigration

`koldstore.migrate_table`, system column add/backfill, PK preservation, FK/unique validation, schema registry, demigration rehydrate path.

### Phase C - KoldstoreMergeScan Hot-Only

Planner hook, CustomPath/CustomScan shim, hot child execution, managed-table heap-only path blocking.

### Phase D - DML Hooks and Commit Sequencing

System column guards, hot INSERT/UPDATE/DELETE stamping, transaction-scoped `_commit_seq`, `koldstore.row_events`, simple cold PK hint lookups, `koldstore.delete_row`, `koldstore.hydrate_pk`.

### Phase E - Cold Path

Parquet writer/reader, object-store publish protocol, manifest model, `koldstore.segments`, `koldstore.cold_pk_hints`, safe qualifier pruning.

### Phase F - Flush and Recovery

Background worker, `system.jobs`, flush policies, manifest sync states, hot cleanup, orphan temp/final recovery.

### Phase G - Security and Operations

RLS enforcement, `COPY`/pg_dump documentation, backup/validate/recover SQL, `koldstore_exec` export/import, DROP TABLE cleanup, FILE type.

## MVP Scope

In MVP:

- normal `CREATE TABLE` plus `koldstore.migrate_table`
- demigration with default rehydrate
- one hot row per PK
- `_seq`, `_commit_seq`, `_deleted`
- KoldstoreMergeScan for SELECT
- direct Arrow/Parquet cold reader
- hot-row standard SQL DML
- explicit cold-only update/delete APIs
- local cold PK hints
- row-event based `changes_since`
- kalamdb-compatible manifest and Parquet files
- built-in worker with preload requirement documented

Out of MVP:

- custom table access method
- `CREATE TABLE ... USING koldstore`
- full DataFusion cold engine
- transparent standard SQL UPDATE of cold-only rows
- global hot+cold UNIQUE/FK enforcement
- parallel Custom Scan
- aggregate/join pushdown into cold reader
- vector indexes and stream tables

## Spec Alignment

| Topic | Decision |
|-------|----------|
| KoldstoreMergeScan | Correct SELECT architecture. |
| Table AM / `USING koldstore` | Removed from MVP; use migrate/manage command. |
| DataFusion | Not needed for MVP; direct Parquet reader first. |
| Hot model | One row per PK; no append-only heap duplicates. |
| Tombstones | Only when cold may contain older PK. |
| Change feed | `_commit_seq` and `koldstore.row_events`, not `_seq` scans. |
| Cold DML | Avoid object-store checks on normal DML; explicit APIs for cold lookup/hydrate. |

## Generated Artifacts

| Artifact | Path |
|----------|------|
| Research | [research.md](./research.md) |
| Data model | [data-model.md](./data-model.md) |
| SQL API contract | [contracts/sql-api.md](./contracts/sql-api.md) |
| KoldstoreMergeScan contract | [contracts/koldstore-merge-scan.md](./contracts/koldstore-merge-scan.md) |
| DML contract | [contracts/dml-rewrite.md](./contracts/dml-rewrite.md) |
| Migration contract | [contracts/migration-and-columns.md](./contracts/migration-and-columns.md) |
| Crate layout | [contracts/crate-layout.md](./contracts/crate-layout.md) |
| Test plan | [contracts/test-plan.md](./contracts/test-plan.md) |

## References

- PostgreSQL Custom Scan API: https://www.postgresql.org/docs/current/custom-scan-path.html
- PostgreSQL Table Access Methods: https://www.postgresql.org/docs/current/tableam.html
- PostgreSQL Background Workers: https://www.postgresql.org/docs/current/bgworker.html
- PostgreSQL sequence functions and nontransactional behavior: https://www.postgresql.org/docs/current/functions-sequence.html
- kalamdb transactions: `../kalamdb/docs/architecture/transactions.md`
- kalamdb manifest and flush: `../kalamdb/docs/architecture/manifest.md`
- kalamdb direct Parquet reader: `../kalamdb/backend/crates/kalamdb-filestore/src/parquet/reader.rs`
