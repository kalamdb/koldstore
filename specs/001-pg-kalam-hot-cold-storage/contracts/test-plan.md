# Test Plan: pg-koldstore Extension

**Version**: 0.3.0 (planning)
**Branch**: `001-pg-koldstore-hot-cold-storage`

Mandatory verification gates before MVP release.

## Test Layers

| Layer | Tool | Scope |
|-------|------|-------|
| Unit | `cargo test` | Merge resolver, commit sequence, manifest publish, Parquet pruning, PK hints. |
| SQL regression | `cargo pgrx test` / pg_regress | DDL, GUCs, planner, DML, errors. |
| Integration | PostgreSQL + MinIO | Migrate -> flush -> query -> DML -> demigrate. |
| Compatibility | Golden files | manifest.json and Parquet readable by kalamdb-compatible tooling. |

## P1 - Blocking

### Extension Lifecycle

- [ ] `CREATE EXTENSION koldstore` creates `koldstore` and `system` schemas.
- [ ] `koldstore_version()` returns a non-empty version.
- [ ] Internal GUCs cannot be set by application roles.
- [ ] Built-in worker test documents and validates `shared_preload_libraries = 'koldstore'`.

### Table Management

- [ ] Normal `CREATE TABLE` followed by `koldstore.migrate_table` succeeds.
- [ ] `CREATE TABLE ... USING koldstore` is not required and is not documented as supported.
- [ ] Migration rejects a table without primary key.
- [ ] Migration preserves app primary key; it does not create `(pk, _seq)` primary key.
- [ ] Migration adds `_seq`, `_commit_seq`, `_deleted`.
- [ ] User table migration accepts explicit `scope_column` or adds `_user_id`.
- [ ] Migration rejects unsupported types with clear detail.
- [ ] FK tables with flush enabled are rejected by default unless explicit hot-only FK option is passed.

### Hot DML

- [ ] INSERT creates exactly one hot row per PK.
- [ ] UPDATE of hot row updates in place and keeps one hot row per PK.
- [ ] DELETE of hot row with no cold hint physically deletes the row.
- [ ] DELETE of hot row with cold hint leaves one tombstone.
- [ ] Reinsert/revive after tombstone keeps one hot row and `_deleted=false`.
- [ ] Normal hot DML path does not perform object-store reads.
- [ ] Direct user writes to `_seq`, `_commit_seq`, `_deleted` are rejected.

### Commit Sequence and Events

- [ ] `_commit_seq` is stamped on committed managed mutations.
- [ ] Rollback produces no row event.
- [ ] Concurrent transactions commit with monotonic `_commit_seq` order under the commit lock.
- [ ] `koldstore.changes_since(table, since_commit_seq)` orders by `commit_seq`.
- [ ] Event retention gap returns explicit error.

### KoldstoreMergeScan

- [ ] `EXPLAIN` shows `Custom Scan (KoldstoreMergeScan)` on managed SELECT.
- [ ] Heap-only final scan path is unavailable for managed read.
- [ ] Hot winner beats older cold row.
- [ ] Tombstone hides older cold row.
- [ ] Object store down + cold required -> ERROR.
- [ ] Mutable app-column predicate is residual after merge.
- [ ] PK/scope/_seq/_commit_seq predicates prune safely.

### Flush and Manifest

- [ ] DML marks `koldstore.manifest` scope `pending_write`.
- [ ] Normal DML does not rewrite object-store `manifest.json`.
- [ ] Flush writes Parquet final object and commits manifest as visibility boundary.
- [ ] No test assumes portable atomic rename.
- [ ] Interrupted flush leaves no manifest reference to corrupt/unvalidated objects.
- [ ] `koldstore.cold_segments` contains min/max `_seq` and `_commit_seq`.
- [ ] Cold PK hints are updated after successful flush.
- [ ] Hot cleanup runs only after manifest commit.
- [ ] Tombstones retained while older cold rows may exist.

### Cold DML APIs

- [ ] `koldstore.hydrate_pk` brings a cold-only row to one hot row.
- [ ] `koldstore.update_row(..., lookup_cold => true)` updates cold-only row by opting into cold lookup.
- [ ] Standard SQL cold-only UPDATE affects 0 rows in MVP.
- [ ] `koldstore.delete_row` writes PK-only tombstone using local metadata.
- [ ] Standard SQL cold-only DELETE only enabled for simple PK predicates with exact local metadata.

### Security

- [ ] User-scoped SELECT without `koldstore.user_id` fails.
- [ ] User-scoped DML without `koldstore.user_id` fails.
- [ ] Cross-scope reads/writes are denied.
- [ ] Cold path enforces RLS/security quals or fails closed.

### Demigration

- [ ] Default demigration rehydrates cold-only rows into heap.
- [ ] Demigrated table no longer uses KoldstoreMergeScan.
- [ ] DML hooks are inactive after demigration.
- [ ] Cold objects are retained by default.
- [ ] `drop_cold => true` removes table object prefix after rehydrate succeeds.

## P2 - MVP Quality

### Pruning and Performance

- [ ] Direct Parquet reader supports projection.
- [ ] Row-group pruning uses footer stats and bloom metadata.
- [ ] PK point lookup skips at least 90% of row groups in test fixture.
- [ ] Hot-row DML benchmark is within target threshold of equivalent heap table.

### Tooling

- [ ] `COPY (SELECT * FROM managed_table) TO` exports merged logical rows.
- [ ] `COPY FROM` shared table stamps system columns correctly.
- [ ] `COPY FROM` user-scoped table is rejected or routed through documented staging path.
- [ ] `pg_dump` limitation is documented.
- [ ] `koldstore_exec('EXPORT TABLE ...')` includes manifest and Parquet artifacts.

### Operations

- [ ] `koldstore.table_status`, `koldstore.backup_manifest`, `koldstore.validate_cold_storage` work.
- [ ] `system.jobs` shows flush status and errors.
- [ ] Storage credential rotation works without rewriting cold objects.
- [ ] Orphan temp/final object recovery is idempotent.

### Schema Evolution

- [ ] `ALTER TABLE ADD COLUMN` increments schema version.
- [ ] Older Parquet segments read with NULL/default coercion where supported.
- [ ] Unsupported type evolution is rejected.

## Optional

- [ ] pg_cron can call `SELECT koldstore.flush_pending()` when installed separately.
- [ ] Extension still works without pg_cron.
- [ ] DataFusion experiment behind trait can be benchmarked without changing public contracts.

## CI Gates

| Gate | Command | Threshold |
|------|---------|-----------|
| Rust unit | `cargo test` | 100% pass. |
| Extension SQL | `cargo pgrx test` | 100% P1 pass. |
| Integration | `tests/integration/run.sh` | Greenfield, migrate, flush, query, DML, demigrate. |
| Compatibility | golden manifest/Parquet checks | No format regressions. |

## Failure Triage

| Failure | Likely module |
|---------|---------------|
| Heap-only managed SELECT | `merge_scan/planner.rs` |
| Duplicate hot PK | `dml` / migration constraint preservation |
| `_seq` used as changelog cursor | `row_events` / SQL API |
| Cold read on hot DML | `dml` / PK hint logic |
| Predicate filters wrong row | `koldstore-merge` qual classification |
| Orphan object visible | manifest publish/recovery |
| Cold-only rows missing after demigration | `migrate/rehydrate.rs` |
