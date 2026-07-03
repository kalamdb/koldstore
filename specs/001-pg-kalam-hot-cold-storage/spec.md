# Feature Specification: koldstore Hot/Cold Storage Extension

**Feature Branch**: `001-pg-koldstore-hot-cold-storage`
**Created**: 2026-07-02
**Updated**: 2026-07-03
**Status**: Draft

## Input

**koldstore** (k for kalamdb, cold storage) is the PostgreSQL extension name. Install with `CREATE EXTENSION koldstore`; public SQL lives in the `koldstore` schema (`koldstore.migrate_table`, etc.).

Implement a PostgreSQL extension for hot/cold storage. Hot tier is PostgreSQL heap. Cold tier is object storage with kalamdb-compatible Parquet and manifest layout. Existing or greenfield PostgreSQL tables can be migrated into managed shared or user-scoped tables. Queries keep using the original table name. Flush archives eligible rows to cold storage. The extension avoids RocksDB, Raft, and an external SQL planner.

## Clarifications

### 2026-07-03 Architecture Corrections

- `CREATE TABLE ... USING koldstore` is removed from MVP because PostgreSQL `USING` means a table access method. pg-koldstore uses normal heap storage plus Custom Scan, not a custom table AM.
- Greenfield workflow is normal `CREATE TABLE`, then `koldstore.migrate_table(...)`.
- Hot heap MUST keep at most one row per logical primary key. The application primary key is preserved.
- `_seq` is a row/effect version id. `_commit_seq` is the durable commit-order cursor used by `changes_since`.
- Change history lives in `koldstore.row_events`, not duplicate hot heap rows.
- Full DataFusion is not required for MVP cold storage. Use a direct Arrow/Parquet/object_store reader with projection, footer stats, row-group pruning, and bloom checks.
- Tombstones exist only to mask older cold rows when cold may contain the PK.
- Normal DML MUST NOT read object storage. Cold-only UPDATE requires explicit hydrate/update API. Cold-only DELETE uses local cold PK hints or exact local metadata.
- Demigration defaults to rehydrating the current logical hot+cold state into a regular heap table.

## User Scenarios & Testing

### User Story 1 - Manage a Greenfield Table (P1)

An application developer creates a normal PostgreSQL table and enables pg-koldstore management with a single SQL function.

**Independent Test**: Create a table with PostgreSQL types and `DEFAULT SNOWFLAKE_ID()`, call `koldstore.migrate_table`, insert rows, and verify system columns and metadata.

**Acceptance Scenarios**:

1. Given a normal table with a primary key, when `koldstore.migrate_table(..., table_type => 'shared')` runs, then pg-koldstore adds `_seq`, `_commit_seq`, `_deleted`, registers metadata, and keeps the original primary key.
2. Given a normal table intended for per-user isolation, when `koldstore.migrate_table(..., table_type => 'user', scope_column => 'user_id')` runs, then reads and writes require `koldstore.user_id`.
3. Given a table uses `DEFAULT SNOWFLAKE_ID()`, when rows are inserted, then primary keys are generated without requiring application-side IDs.

### User Story 2 - Migrate an Existing Table (P1)

An administrator converts an existing PostgreSQL table to a managed hot/cold table without changing the application-facing table name.

**Independent Test**: Migrate a table with rows and primary key, then read/update/delete hot rows through normal SQL.

**Acceptance Scenarios**:

1. Migration is rejected without a primary key.
2. Migration preserves the primary key and existing hot indexes.
3. Migration records supported types, indexes, PK, scope, flush policy, and storage binding in `system.schemas`.
4. Migration rejects risky FK configurations by default when flush is enabled, unless the operator explicitly accepts hot-only FK semantics.

### User Story 3 - Flush to Cold Storage (P1)

An administrator configures a flush policy. Eligible hot rows are written to object storage while queries still return complete logical results.

**Independent Test**: Insert rows, run `koldstore.flush_table`, verify Parquet and manifest artifacts, then query the table.

**Acceptance Scenarios**:

1. Normal DML marks the affected scope `pending_write` but does not rewrite `manifest.json`.
2. Flush writes Parquet through a backend-safe publish protocol and commits visibility through `manifest.json`.
3. `koldstore.cold_segments` and local cold PK hints are updated after manifest commit.
4. Hot live rows are removed only after Parquet and manifest commit succeed.
5. Hot tombstones are retained while any cold segment may contain an older version of the same PK.

### User Story 4 - Query Hot and Cold Transparently (P2)

An application issues normal SELECT queries and receives the logical current table contents across hot and cold storage.

**Independent Test**: Flush rows to cold, update a hot row, delete a cold row via explicit API, and verify merged SELECT results.

**Acceptance Scenarios**:

1. `EXPLAIN` shows `Custom Scan (KoldstoreMergeScan)` for managed table reads.
2. Heap-only paths are not selectable as final scan paths for managed reads.
3. A hot row with newer `_seq` wins over older cold rows for the same PK.
4. A hot tombstone hides older cold rows.
5. Mutable app-column filters are applied after winner resolution unless proven safe for pruning.
6. If cold segments are required and object storage is unavailable, the query fails with ERROR rather than returning partial hot-only results.

### User Story 5 - Near-Native Hot DML (P1)

Applications update recently active rows with standard SQL while avoiding object-store reads on the write path.

**Independent Test**: Insert, update, and delete hot rows while tracing that no cold object reads occur.

**Acceptance Scenarios**:

1. The hot heap has at most one row per primary key after any sequence of INSERT/UPDATE/DELETE/revive.
2. Hot UPDATE mutates the one hot row in place and advances `_seq` / `_commit_seq`.
3. Hot DELETE physically deletes the row when no cold segment may contain that PK.
4. Hot DELETE converts the row to a tombstone when cold may contain that PK.
5. Direct writes to `_seq`, `_commit_seq`, and `_deleted` are rejected for application roles.

### User Story 6 - Cold-Only DML APIs (P2)

An operator or application can update or delete rows that live only in cold storage without forcing every normal DML statement to scan cold storage.

**Independent Test**: Flush a row cold-only, call `koldstore.delete_row`, verify default SELECT hides it and `changes_since` reports a delete event.

**Acceptance Scenarios**:

1. `koldstore.hydrate_pk(table, pk)` reads one cold row and brings it back to hot storage.
2. `koldstore.update_row(..., lookup_cold => true)` updates a cold-only row only when the caller opts into cold lookup.
3. `koldstore.delete_row(table, pk)` writes a PK-only tombstone using local cold metadata without scanning Parquet on the default path.
4. Standard SQL cold-only UPDATE is out of MVP.
5. Standard SQL cold-only DELETE is supported only for simple PK predicates when exact local metadata preserves SQL rowcount semantics.

### User Story 7 - User-Scoped Security (P2)

Multi-tenant applications use user-scoped tables and every query or mutation is restricted to the active session scope.

**Independent Test**: Set `koldstore.user_id` to user A and verify user B rows are not visible or writable; unset scope and verify fail-closed behavior.

**Acceptance Scenarios**:

1. Missing `koldstore.user_id` fails before reading hot or cold rows.
2. Scope filters are applied to cold path selection before object-store reads.
3. RLS/security quals are enforced on cold rows or planning fails closed.

### User Story 8 - Change Feed (P2)

A future realtime service can consume committed changes without relying on duplicate heap rows.

**Independent Test**: Insert, update, delete, and revive a PK; call `koldstore.changes_since` using `_commit_seq` cursor.

**Acceptance Scenarios**:

1. Every managed mutation writes a `koldstore.row_events` entry in the same transaction.
2. Events are returned ordered by `_commit_seq`.
3. Delete events include PK and `_deleted = true`.
4. If event retention has expired, `changes_since` returns a gap error.

### User Story 9 - Demigrate (P2)

An administrator exits pg-koldstore management and returns to a regular PostgreSQL table.

**Independent Test**: Demigrate a table with cold rows and verify normal scans show the current logical data.

**Acceptance Scenarios**:

1. Default demigration rehydrates current hot+cold logical rows into the heap.
2. `KoldstoreMergeScan`, DML hooks, flush jobs, and managed metadata are disabled after demigration.
3. Cold artifacts are retained by default and deleted only with `drop_cold => true`.
4. `rehydrate => false` is explicit archive-detach mode and warns that cold-only rows will not be visible.

### User Story 10 - Operability and Backup (P3)

Operators inspect status, validate cold storage, and export/import managed tables.

**Acceptance Scenarios**:

1. `koldstore.table_status`, `koldstore.backup_manifest`, `koldstore.validate_cold_storage`, and `koldstore.recover_segments` expose useful state.
2. Documentation states PostgreSQL base backup/PITR does not include object-store Parquet or FILE blobs.
3. `koldstore_exec('EXPORT TABLE ...')` creates a kalamdb-compatible transfer archive.
4. `DROP TABLE` removes object-storage artifacts according to configured drop policy.

## Edge Cases

- Migrating without a primary key: reject.
- Migrating with unsupported data types: reject with type matrix detail.
- Migrating with inbound/outbound FKs and flush enabled: reject by default unless explicitly allowed.
- Concurrent writes to same PK: PostgreSQL row locking and primary key constraints protect the hot row; `_commit_seq` is allocated after acquiring the transaction-scoped pg-koldstore commit-order lock.
- Transaction allocates `_seq` then rolls back: no committed row/event; `_seq` gaps are allowed.
- Object store unavailable during flush: job enters retry/error; hot data remains authoritative.
- Object store unavailable during SELECT requiring cold: ERROR.
- Temp/final Parquet exists without manifest entry after crash: recovery treats it as orphan and cleans or quarantines it.
- Standard SQL cold-only UPDATE: affects 0 rows in MVP; use hydrate/update API.
- Cold-only DELETE with only may-contain metadata: explicit API may write idempotent tombstone; exact SQL rowcount requires exact metadata.
- Reinsert after hot tombstone: revive/update the one tombstone row; do not create a duplicate heap row.
- Mutable app-column filter on cold row: residual after merge, not unsafe pre-merge pruning.
- `COPY FROM` user-scoped managed table: rejected under RLS; use staging/INSERT.
- `COPY TO` and pg_dump: full logical export requires `COPY (SELECT ...)` plus cold object backup or `koldstore_exec EXPORT`.
- Logical replication: native PostgreSQL replication does not replicate object-store bytes; full support is post-MVP.

## Functional Requirements

### Core Model

- **FR-001**: System MUST provide a PostgreSQL extension that manages normal heap tables as shared or user-scoped hot/cold tables through `koldstore.migrate_table`.
- **FR-002**: System MUST NOT require or advertise `CREATE TABLE ... USING koldstore` in MVP.
- **FR-003**: Managed tables MUST include `_seq bigint`, `_commit_seq bigint`, and `_deleted boolean not null default false`.
- **FR-004**: Managed hot heap tables MUST preserve the application primary key and keep at most one hot row per PK.
- **FR-005**: User-scoped tables MUST require a scope column and `koldstore.user_id`.
- **FR-006**: Direct application writes to system columns MUST be rejected.

### Migration and Demigration

- **FR-010**: Migration MUST validate primary key, type support, constraints, scope, storage, and flush policy.
- **FR-011**: Migration MUST preserve hot indexes and record cold stats/bloom candidates.
- **FR-012**: Demigration MUST rehydrate current logical hot+cold data by default.
- **FR-013**: Demigration MUST retain cold artifacts unless `drop_cold => true`.

### Cold Storage and Manifest

- **FR-020**: Cold data MUST be stored as kalamdb-compatible Parquet segments on object storage.
- **FR-021**: `manifest.json` MUST be the object-store visibility boundary for committed cold segments.
- **FR-022**: Publish MUST NOT rely on portable atomic rename; backend-safe temp/final/manifest commit and recovery are required.
- **FR-023**: DML MUST mark local manifest state `pending_write` without rewriting object-store manifest.
- **FR-024**: Flush MUST update `koldstore.cold_segments`, local cold PK hints, and `koldstore.manifest` after manifest commit.

### Query

- **FR-030**: Managed SELECT MUST use KoldstoreMergeScan.
- **FR-031**: KoldstoreMergeScan MUST merge hot and cold rows by PK and hide tombstone winners.
- **FR-032**: Cold pruning MUST be limited to safe predicates; mutable app-column quals are residual after merge.
- **FR-033**: Cold required + object storage unavailable MUST fail with ERROR.
- **FR-034**: The cold reader MUST use direct Arrow/Parquet/object_store capabilities in MVP.

### DML

- **FR-040**: Hot INSERT/UPDATE/DELETE MUST avoid object-store reads on the normal path.
- **FR-041**: Hot UPDATE MUST update in place and advance `_seq` / `_commit_seq`.
- **FR-042**: DELETE MUST keep a tombstone only when cold may contain the PK.
- **FR-043**: Cold-only UPDATE MUST require explicit hydrate/update API in MVP.
- **FR-044**: Cold-only DELETE MUST be available through `koldstore.delete_row` using local cold metadata.

### Commit Sequence and Change Feed

- **FR-050**: `_commit_seq` MUST be the external change-feed cursor.
- **FR-051**: `changes_since` MUST read `koldstore.row_events` ordered by `_commit_seq`.
- **FR-052**: `_seq` gaps are allowed and MUST NOT be treated as missing commits.
- **FR-053**: Event retention gaps MUST be reported explicitly.

### Security and Tooling

- **FR-060**: User-scoped tables MUST fail closed without `koldstore.user_id`.
- **FR-061**: Cold-side RLS/security enforcement MUST be equivalent to hot-side enforcement or fail closed.
- **FR-062**: Internal GUCs MUST be protected from normal application roles.
- **FR-063**: COPY, pg_dump, PITR, and logical replication limitations MUST be documented and tested where supported.

### Operations

- **FR-070**: Built-in flush scheduling MUST document `shared_preload_libraries = 'koldstore'`; SQL/pg_cron fallback MUST be available.
- **FR-071**: Operators MUST be able to inspect table status, jobs, manifests, and cold validation through SQL.
- **FR-072**: Object-store backup/recovery MUST be explicit; PostgreSQL backup alone is not a full managed-table backup.

## Key Entities

- **Managed Table**: Normal PostgreSQL heap table with pg-koldstore metadata and system columns.
- **KoldstoreMergeScan**: Custom Scan SELECT path.
- **Cold Segment**: Immutable Parquet object plus PostgreSQL visibility metadata.
- **Manifest**: Object-store JSON source of truth for cold segment visibility.
- **Cold PK Hint**: Local metadata used to avoid cold DML object-store reads.
- **Row Event**: Internal committed change-feed record ordered by `_commit_seq`.
- **Storage Registration**: Object-store backend and path templates.
- **Background Job**: Flush/recovery/validation work item.

## Success Criteria

- **SC-001**: Migrate a 1M-row table in a maintenance window while preserving table name and primary key.
- **SC-002**: Hot-row point INSERT/UPDATE/DELETE performs within 10% of equivalent heap table behavior in benchmark scenarios that do not require cold lookup.
- **SC-003**: After flush, 100% of tested PK lookups return the same logical current row as before flush.
- **SC-004**: No test leaves duplicate hot heap rows for a primary key.
- **SC-005**: `changes_since` returns committed events in `_commit_seq` order.
- **SC-006**: Parquet pruning skips at least 90% of row groups on PK point lookup test data.
- **SC-007**: Cold object outage never returns partial hot-only results when cold is required.
- **SC-008**: Demigration with default options leaves a regular heap table containing current logical rows.
- **SC-009**: Produced manifests and Parquet files are readable by kalamdb-compatible tooling.

## Assumptions

- PostgreSQL 15+ is the minimum target.
- Operators can configure object storage and backup outside PostgreSQL PITR.
- Primary keys are required.
- Composite primary keys are supported.
- Strict `_commit_seq` requires serializing pg-koldstore managed commits at the selected commit domain.
- Full transparent SQL UPDATE of cold-only rows is not required for MVP.

## Out of Scope

- Custom PostgreSQL table access method.
- `CREATE TABLE ... USING koldstore`.
- RocksDB or Raft.
- Full DataFusion dependency or DataFusion as SQL planner.
- Global hot+cold UNIQUE/FK enforcement.
- Transparent standard SQL UPDATE of cold-only rows.
- Parallel Custom Scan and aggregate/join pushdown into cold reader.
- Stream tables, vector indexes, and full realtime subscription transport.
