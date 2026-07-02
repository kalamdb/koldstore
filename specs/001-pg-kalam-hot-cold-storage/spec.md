# Feature Specification: pg-kalam Hot/Cold Storage Extension

**Feature Branch**: `001-pg-kalam-hot-cold-storage`

**Created**: 2026-07-02

**Status**: Draft

**Input**: User description: "Implement a PostgreSQL extension that supports hot/cold storage: hot tier is PostgreSQL, cold tier is object storage. Users migrate existing tables to managed shared or user-scoped tables with system columns (_seq, _deleted, optional scope identifier). Periodic flush archives older rows to cold storage using the same manifest, Parquet layout, and flush options as kalamdb. Metadata lives in kalam.storage, kalam.manifest, system.jobs, and system.schemas. Includes a FILE datatype for direct object-store uploads. Secured with row-level policies; user-scoped queries always enforce scope. Lightweight, PostgreSQL-native—no RocksDB, external query planner, or Raft."

## Clarifications

### Session 2026-07-02

- Q: What cold-read engine should the spec authorize for MVP? → A: DataFusion permitted only as the cold Parquet scan engine inside unified hot+cold merge reads; PostgreSQL remains the outer SQL planner.
- Q: How should MVP handle UPDATE/DELETE on rows that exist only in cold storage (no hot heap version)? → A: Hot rows use standard SQL DML; cold-only rows require extension functions (e.g., `kalam.update()`, `kalam.delete()`) in MVP.
- Q: What should MVP guarantee for native PostgreSQL UNIQUE indexes and FOREIGN KEY constraints on managed tables? → A: Hot heap only — native UNIQUE/FK apply to hot rows; document that they do not cover cold-only rows; no global enforcement in MVP.
- Q: When object storage is unavailable during a SELECT that requires cold Parquet segments, what should happen? → A: Fail the query with ERROR — no partial hot-only results when cold data is needed.
- Q: Who is responsible for cold storage backup and recovery in MVP? → A: Operator-managed via pg-kalam — extension exposes backup manifest, validate, and recover SQL; cold objects are outside PostgreSQL PITR.

### Session 2026-07-03

- Product requirements: cold-row deletes append hot tombstones; `CREATE TABLE ... USING kalamdb WITH (...)` DDL; migrate-anytime; kalamdb-compatible flush/manifest; index-derived Parquet bloom/stats; footer-only Parquet pruning; `kalam_version()`/`kalam_user_id()`/`kalam.user_id`; minimal DataFusion/binary; cross-table joins; auto schema catalog on install+ALTER; DROP removes storage; export/import via `kalam_exec`.
- Realtime readiness: every INSERT/UPDATE/DELETE appends a new row with a new monotonic `_seq`; tombstone (`_deleted=true`) versions remain visible to change-feed consumers so delete events can be pushed to realtime subscribers.

## User Scenarios & Testing *(mandatory)*

### User Story 0 - Create a Kalam Table with Native PostgreSQL DDL (Priority: P1)

An application developer creates a new pg-kalam table using familiar PostgreSQL column types and kalamdb-style `USING kalamdb WITH (...)` options—no separate migration step required for greenfield tables.

**Why this priority**: Easy table creation is the primary developer entry point alongside migration.

**Independent Test**: Run `CREATE TABLE ... USING kalamdb WITH (type = 'shared', ...)` with `BIGINT`, `TEXT`, `TIMESTAMP`, and `DEFAULT SNOWFLAKE_ID()`; insert rows; verify system columns and flush policy are applied.

**Acceptance Scenarios**:

1. **Given** a developer runs `CREATE TABLE app.shared_items (...) USING kalamdb WITH (type = 'shared', flush_policy = 'rows:1000,interval:60')`, **When** the statement completes, **Then** the table is Kalam-managed with `_seq`, `_deleted`, registered storage binding, and standard PostgreSQL datatypes.
2. **Given** a developer runs `CREATE TABLE app.profiles (...) USING kalamdb WITH (type = 'user')`, **When** inserts occur with `SET kalam.user_id = 'user-alice'`, **Then** rows are scope-isolated and the table requires session user context.
3. **Given** a column uses `DEFAULT SNOWFLAKE_ID()`, **When** a row is inserted without explicit PK, **Then** a monotonic snowflake ID is generated compatible with kalamdb semantics.

---

### User Story 1 - Migrate a Table to Hot/Cold Storage (Priority: P1)

A database administrator has an existing PostgreSQL table with application data. They want to convert it into a pg-kalam managed table at any point in the table's lifetime so that recent rows stay queryable in PostgreSQL (hot) while older rows are archived automatically to cheaper object storage (cold), without changing how applications refer to the table name.

**Why this priority**: Without migration, no other feature delivers value. This is the entry point for every adoption path.

**Independent Test**: Migrate a plain table with primary key and sample rows, insert new rows, and verify reads return all rows with correct values and system columns present.

**Acceptance Scenarios**:

1. **Given** an existing PostgreSQL table with a defined primary key, **When** the administrator migrates it as a **shared** pg-kalam table, **Then** the table gains system columns `_seq` and `_deleted`, remains queryable by name, and accepts standard SQL inserts/updates/deletes for rows with hot heap versions.
2. **Given** an existing PostgreSQL table, **When** the administrator migrates it as a **user-scoped** pg-kalam table, **Then** the table additionally gains a scope identifier column (initially `user_id`) and enforces isolation per scope on reads and writes.
3. **Given** a migrated table with no flush policy configured, **When** rows are inserted, **Then** all rows remain in hot storage only and no cold archive is created.
4. **Given** a plain PostgreSQL table with existing indexes, **When** migrated to a Kalam table, **Then** indexed columns are recorded for Parquet bloom filters and min/max column stats to enable cold-read pruning without full-file scans.

---

### User Story 2 - Automatic Archive to Cold Storage (Priority: P1)

An administrator configures a migrated table with flush options (row count threshold, time interval, or combined). As data ages or accumulates, eligible rows are written to object storage as Parquet segments and indexed in a manifest, reducing hot-tier storage cost while keeping data queryable.

**Why this priority**: Cold storage is the core cost and capacity benefit of pg-kalam. Flush must work reliably for the product to justify migration.

**Independent Test**: Configure flush policy on a shared table, insert enough rows to trigger flush, and verify manifest and Parquet artifacts appear in object storage while queries still return complete results.

**Acceptance Scenarios**:

1. **Given** a shared table with `FLUSH_POLICY 'rows:10000'`, **When** hot row count exceeds the threshold, **Then** a background job archives a batch to cold storage, writes `batch-N.parquet.tmp` then renames atomically, rewrites `manifest.json` listing all committed segments (kalamdb-compatible), updates `kalam.manifest` sync state (`syncing` → `in_sync`), and removes flushed rows from hot storage (preserving tombstones per retention rules).
2. **Given** a user-scoped table with active scopes A and B, **When** flush runs, **Then** each scope gets its own manifest path and Parquet batches under the configured storage template.
3. **Given** a flush in progress, **When** the process is interrupted, **Then** recovery leaves no corrupt committed segments (temporary files are not promoted; manifest sync state reflects error or retry).

---

### User Story 3 - Unified Query Across Hot and Cold (Priority: P2)

An application developer queries a pg-kalam table with standard SQL. The system transparently merges hot PostgreSQL rows with cold Parquet segments, resolving row versions by primary key and highest `_seq`, and hiding soft-deleted rows unless retention requires otherwise.

**Why this priority**: Transparent querying is required for drop-in adoption; without it, applications would need dual-query logic.

**Independent Test**: Insert rows, flush a subset to cold, query by primary key and range filters, and verify merged results match pre-flush expectations.

**Acceptance Scenarios**:

1. **Given** a row updated in hot storage after an older version was flushed to cold, **When** queried by primary key, **Then** the result reflects the hot version (highest `_seq`).
2. **Given** a soft-deleted row (`_deleted = true`), **When** queried by default, **Then** the row is not returned unless explicitly requested by supported retention or audit modes.
3. **Given** a row exists only in cold storage, **When** deleted via `kalam.delete()` or standard SQL delete on a hot tombstone path, **Then** a new hot tombstone row (`_deleted = true`, new higher `_seq`) is appended; the tombstone is the winning version for default merged reads but remains visible to change-feed queries for realtime delete propagation.
4. **Given** segment metadata with sequence ranges and Parquet footer stats/bloom filters, **When** a query filters on primary key or `_seq` bounds, **Then** irrelevant segments and row groups are skipped using manifest min/max and Parquet footer metadata without reading full files.
5. **Given** any INSERT, UPDATE, or DELETE on a managed table, **When** the transaction commits, **Then** a new version row is appended with a strictly increasing `_seq` (never in-place `_seq` reuse), enabling `_seq`-ordered change feeds for future realtime.

---

### User Story 4 - Secure User-Scoped Access (Priority: P2)

A multi-tenant application uses user-scoped pg-kalam tables. Every session querying or mutating data must operate within an explicit scope (e.g., `user_id`). Row-level security policies prevent cross-scope reads and writes even if application SQL omits scope filters.

**Why this priority**: Security failure in multi-tenant mode is unacceptable; scope enforcement must be automatic and default-deny.

**Independent Test**: Set session scope to user A, attempt to read/write user B's rows, and verify denial; repeat with correct scope and verify success.

**Acceptance Scenarios**:

1. **Given** a user-scoped table and a session with `SET kalam.user_id = 'user-alice'`, **When** selecting all rows, **Then** only rows for that scope are visible and `SELECT kalam_user_id()` returns the active scope.
2. **Given** a session without scope set on a user-scoped table, **When** any query or DML is attempted, **Then** the operation is rejected with a clear error.
3. **Given** a shared table with configured access level, **When** a role without permission queries the table, **Then** access is denied per table access policy.

---

### User Story 5 - FILE Column Type for Object Storage (Priority: P3)

An application stores documents and media. Columns declared as FILE accept uploads that land directly in object storage (shared or scope-specific path), storing a structured reference in the row while bytes live outside PostgreSQL.

**Why this priority**: FILE support completes parity with kalamdb's media workflow but is not required for core tabular hot/cold migration.

**Independent Test**: Insert a FILE value on a shared and a user-scoped table, verify blob in storage and JSON reference in row, download by reference.

**Acceptance Scenarios**:

1. **Given** a shared table with a FILE column, **When** a file is uploaded, **Then** the blob is stored under the table's shared storage path and the row contains a reference with id, name, size, mime, and checksum metadata.
2. **Given** a user-scoped table, **When** a scoped user uploads a file, **Then** the blob path includes the scope identifier and is isolated from other scopes.
3. **Given** manifest subfolder rotation limits, **When** file count in a subfolder exceeds the configured maximum, **Then** new files use the next subfolder and manifest `files` state is updated.

---

### User Story 6 - Operate and Observe via System Catalog (Priority: P3)

An operator registers object storage backends, inspects manifest sync state, and monitors background flush/compaction jobs through SQL-accessible system tables without direct object-store console access.

**Why this priority**: Operability supports production adoption but depends on core migration and flush working first.

**Independent Test**: Register storage, migrate table, trigger flush, query system tables for job status and manifest sync state.

**Acceptance Scenarios**:

1. **Given** a new S3-compatible storage registration in `kalam.storage`, **When** a table is bound to that storage, **Then** cold artifacts use the configured path templates for shared or user tables.
2. **Given** a pending flush, **When** the operator queries `system.jobs`, **Then** job type, status, parameters, and error details are visible.
3. **Given** manifest cache rows in `kalam.manifest`, **When** queried, **Then** sync state (`in_sync`, `pending_write`, `syncing`, `stale`, `error`) reflects current hot/cold consistency.
4. **Given** a managed table with cold segments on object storage, **When** the operator runs backup manifest, validate, or recover SQL functions, **Then** cold artifact paths and consistency checks are returned without requiring object-store console access, and documentation states cold data is outside PostgreSQL base backup/PITR.

---

### User Story 7 - Export and Import Table Data (Priority: P2)

An operator or developer exports a managed table's hot+cold data for backup or migration and imports it into another Kalam table using kalamdb-compatible transfer semantics via `kalam_exec`.

**Why this priority**: Parity with kalamdb table transfer workflows for portability and disaster recovery beyond manifest SQL.

**Independent Test**: Export a flushed table via `kalam_exec`, import into a new table, verify row counts and PK resolution match.

**Acceptance Scenarios**:

1. **Given** a managed table with cold segments, **When** export is requested via `kalam_exec`, **Then** Parquet segments and manifest metadata are packaged kalamdb-compatibly.
2. **Given** an export archive from kalamdb or pg-kalam, **When** import runs via `kalam_exec`, **Then** segments are written with new ids/paths and manifest is persisted atomically.
3. **Given** `DROP TABLE` on a managed table, **When** drop completes, **Then** associated object-storage prefix (manifest, segments, FILE blobs) is removed per table options.

---

### User Story 8 - Change-Feed Visibility for Future Realtime (Priority: P2)

A realtime service will subscribe to table changes (inserts, updates, deletes) on top of Kalam tables. Every mutation must produce a new `_seq`, and delete events must remain observable as tombstone versions so subscribers can propagate removals.

**Why this priority**: Realtime is a planned follow-on; the storage model must not hide delete events or reuse sequence numbers in ways that break changelogs.

**Independent Test**: Insert, update, and delete rows; query change-feed surface ordered by `_seq`; verify all three event types appear including tombstones with `_deleted = true`.

**Acceptance Scenarios**:

1. **Given** a row is updated, **When** the update commits, **Then** a new hot version row exists with a higher `_seq` than the prior version.
2. **Given** a row is deleted, **When** the delete commits, **Then** a tombstone row exists with `_deleted = true` and a new `_seq`, and change-feed queries return it as a delete event.
3. **Given** default application `SELECT` (merged logical view), **When** querying by primary key after delete, **Then** the row is not returned; change-feed mode still exposes the tombstone version for realtime consumers.

---

### Edge Cases

- What happens when migrating a table without a primary key? Migration is rejected with guidance to add a primary key first (required for version resolution and flush deduplication).
- What happens when object storage is temporarily unavailable during flush? Job enters error/retry state; hot data remains intact; queries that require cold segments fail with ERROR; queries satisfiable from hot rows alone continue to succeed; cold segments already committed remain readable once storage is restored.
- What happens when two sessions update the same primary key concurrently? Each write gets a distinct `_seq`; readers see the highest committed `_seq` after transaction visibility rules.
- What happens when flush policy triggers but hot row count is below minimum batch size? System may defer flush until threshold met or coalesce with time-based trigger per combined policy semantics.
- What happens when a user-scoped table row lacks scope identifier on insert? Insert is rejected; scope must be set at session level and applied automatically.
- What happens when storage credentials rotate? Operator updates `kalam.storage` credentials; subsequent flush and FILE operations use new credentials without rewriting existing cold artifacts.
- What happens when schema evolves (add column)? New schema version recorded in `system.schemas`; new Parquet segments carry `schema_version`; readers merge columns with null defaults for older segments.
- What happens when standard SQL `UPDATE`/`DELETE` targets a row that exists only in cold storage? Operation MUST fail with a clear error directing callers to extension functions (`kalam.update()`, `kalam.delete()`) that append new hot versions or tombstones.
- What happens when a native UNIQUE index or FOREIGN KEY exists on a managed table after flush? Constraints enforce hot heap rows only; cold-only rows are outside native constraint scope in MVP—operators MUST NOT assume global hot+cold uniqueness or referential integrity from native PostgreSQL constraints alone.
- What happens when object storage is unavailable during a SELECT that requires cold Parquet segments? Query MUST fail with ERROR; system MUST NOT return partial hot-only results when merged cold data is required for correctness.
- What happens during disaster recovery when only PostgreSQL base backup/PITR is restored? Hot data and catalog metadata are restored from PostgreSQL; cold Parquet objects and FILE blobs MUST be recovered separately using pg-kalam backup/validate/recover operations and object-store procedures—PostgreSQL PITR alone is insufficient for full logical table recovery.
- What happens when a row in cold storage is deleted? System appends a hot tombstone row (`_deleted = true`) with a new `_seq` that wins merge resolution over all older hot and cold versions for that primary key.
- What happens when `DROP TABLE` is run on a managed table? Table metadata, catalog rows, and associated object-storage artifacts (manifest, Parquet segments, FILE blobs) are removed per configured drop policy.
- What happens when a developer alters a managed table (`ALTER TABLE ADD COLUMN`)? Schema version increments in `system.schemas`; extension event hook records change; new flushes carry updated `schema_version`; older Parquet segments remain readable with null defaults for new columns.
- What happens when a realtime consumer polls changes since `_seq` N? Change-feed queries return all version rows (including tombstones) with `_seq > N` in monotonic order; tombstones carry `_deleted = true` so subscribers can emit delete events.

## Requirements *(mandatory)*

### Functional Requirements

#### Core Storage Model

- **FR-001**: System MUST provide a PostgreSQL extension that registers managed **shared** and **user-scoped** table types via `CREATE TABLE ... USING kalamdb WITH (...)` and via in-place migration.
- **FR-001a**: `CREATE TABLE` for Kalam tables MUST accept standard PostgreSQL datatypes (`BIGINT`, `TEXT`, `TIMESTAMP`, etc.) and kalamdb-style options: `type` (`shared` | `user`), `flush_policy`, `compression`, `storage_id`.
- **FR-001b**: System MUST provide `SNOWFLAKE_ID()` as a default expression for primary keys, compatible with kalamdb snowflake semantics.
- **FR-002**: Every managed table MUST include system columns `_seq` (monotonic version identifier per appended row) and `_deleted` (soft-delete / tombstone flag).
- **FR-002a**: Every INSERT, UPDATE, and DELETE MUST append a new version row with a strictly increasing `_seq`; `_seq` MUST NOT be updated in place on existing rows.
- **FR-003**: User-scoped tables MUST include a public scope identifier column (initially `user_id`, extensible to `tenant_id`, `entity_id`, `device_id`, or equivalent without redesigning the storage model).
- **FR-004**: Shared and user-scoped tables MUST share the same logical row and flush model; the only structural difference is the optional scope identifier on user-scoped tables.
- **FR-005**: Hot storage MUST be the PostgreSQL heap for active rows; cold storage MUST be object storage (filesystem, S3-compatible, GCS, Azure) registered in `kalam.storage`.

#### Migration

- **FR-006**: System MUST allow migrating an existing PostgreSQL table in-place to a pg-kalam managed table at any point in the table's lifetime while preserving the table name visible to applications.
- **FR-007**: Migration MUST require a primary key on the source table and MUST copy existing rows into the managed model with generated `_seq` values.
- **FR-008**: Migration MUST record table definition, columns, options, storage binding, table type, and indexed columns in `system.schemas`.
- **FR-008a**: On migration, system MUST read existing PostgreSQL indexes and map indexed columns to Parquet bloom-filter columns and min/max `column_stats` metadata for cold-read pruning.

#### Cold Storage & Manifest (kalamdb-compatible)

- **FR-009**: Cold data MUST be stored as Parquet segment files (`batch-N.parquet`) written atomically via temporary file then rename.
- **FR-010**: Each storage scope MUST maintain a `manifest.json` on object storage that is the source of truth for cold segments, sequence ranges, file subfolder state, and metadata—using the same structure and field semantics as kalamdb. On flush, manifest MUST be rewritten listing **all** committed segments (not delta-only).
- **FR-011**: Shared tables MUST use one manifest per table; user-scoped tables MUST use one manifest per (table, scope identifier) pair.
- **FR-012**: `kalam.manifest` MUST cache manifest metadata locally (etag, sync state, last refreshed) for introspection and flush scheduling; it is not the source of truth over object-store `manifest.json`.
- **FR-013**: Path layout for shared and user tables MUST follow configurable templates from `kalam.storage` (e.g., `{namespace}/{tableName}/manifest.json` and `{namespace}/{tableName}/{userId}/manifest.json`).

#### Flush & Archive

- **FR-014**: Tables MUST support per-table flush policy options equivalent to kalamdb: row limit, time interval, and combined (either trigger fires), specified at table creation or migration.
- **FR-015**: Tables without flush policy MUST remain hot-only indefinitely.
- **FR-016**: On DML, system MUST mark the affected manifest scope as `pending_write` for flush discovery; normal DML MUST NOT rewrite `manifest.json`.
- **FR-017**: Background scheduler MUST scan pending scopes on a configurable interval and enqueue flush jobs in `system.jobs`.
- **FR-018**: Flush jobs MUST: mark manifest `syncing`; deduplicate rows by primary key keeping highest `_seq`; write Parquet to `batch-N.parquet.tmp` then atomic rename to `batch-N.parquet`; append segment with `column_stats` and PK bloom filters; rewrite `manifest.json` with all committed segments; update `kalam.manifest` to `in_sync`; then remove flushed rows from hot storage while honoring deleted-row retention settings.
- **FR-019**: Flush MUST support compression options (none, snappy, zstd) and deleted-row retention hours consistent with kalamdb table options.

#### Query & Versioning

- **FR-020**: Queries against managed tables MUST merge hot PostgreSQL rows with cold Parquet segments transparently via a single unified read path (no heap-only scan that omits cold data). Managed tables MUST support joins with other Kalam tables and normal PostgreSQL tables in the same query plan.
- **FR-021**: **Default merged read** (application `SELECT`): version resolution uses primary key plus highest `_seq`; tombstone rows (`_deleted = true`) MUST be excluded from the logical result. A tombstone MUST override all older live versions for the same primary key.
- **FR-021a**: **Change-feed read** (realtime readiness): tombstone versions MUST remain queryable and visible when consuming changes by `_seq` order (e.g., `kalam.changes_since(seq)` or `SET kalam.changelog = on`), so delete events can be pushed to subscribers.
- **FR-021b**: Flush MUST retain tombstone rows through the configured deleted-row retention window so cold archives preserve delete events for `_seq`-ordered replay.
- **FR-022**: Cold read path MUST prune Parquet segments using manifest `min_seq`/`max_seq` and per-column `column_stats` min/max when query bounds allow.
- **FR-022a**: Cold Parquet reads MUST use DataFusion only as an internal scan engine (projection, filter, row-group pruning, bloom-filter pruning); PostgreSQL MUST remain the outer SQL planner for joins, aggregates, permissions, and transaction snapshots. DataFusion dependency MUST be minimized to required crates/features only (no full SQL planner, no unused execution modules).
- **FR-022b**: When a query requires cold Parquet segments and object storage is unavailable, the query MUST fail with ERROR; partial hot-only results MUST NOT be returned in that case.
- **FR-022c**: Cold reads MUST use Parquet footer/metadata reads (statistics, bloom filters, page index) to prune row groups without full-file scans—following kalamdb `parquet/reader.rs` patterns.

#### DML & Versioning Writes

- **FR-037**: DML on managed tables MUST follow append-versioned semantics: `UPDATE` inserts a new hot version row with a new `_seq`; `DELETE` inserts a hot tombstone row (`_deleted = true`) with a new `_seq`; in-place heap updates MUST NOT be the source of truth.
- **FR-038**: Standard SQL `UPDATE` and `DELETE` MUST work for rows with a current hot heap version.
- **FR-039**: Standard SQL `UPDATE` and `DELETE` on rows that exist only in cold storage MUST be rejected with a clear error in MVP.
- **FR-040**: System MUST provide extension functions (e.g., `kalam.update()`, `kalam.delete()`) that accept a primary-key identifier and append the corresponding hot version or tombstone for cold-only rows. `kalam.delete()` on a cold-only row MUST append a hot tombstone that suppresses all prior versions in merge reads.

#### Constraints

- **FR-041**: Native PostgreSQL UNIQUE indexes and FOREIGN KEY constraints on managed tables MUST apply to hot heap rows only in MVP.
- **FR-042**: System MUST document that native UNIQUE/FK constraints do not enforce uniqueness or referential integrity across cold Parquet segments or merged logical rows.
- **FR-043**: Global hot+cold uniqueness or referential integrity enforcement MUST NOT be claimed in MVP; post-MVP global enforcement is out of scope unless explicitly added in a future release.

#### Security & Access Control

- **FR-023**: User-scoped tables MUST require an explicit session user identifier (`SET kalam.user_id = '...'`) before any query or DML; missing user context MUST fail closed.
- **FR-023a**: System MUST provide `kalam_user_id()` returning the current session user identifier and `kalam_version()` returning the extension version.
- **FR-024**: Row-level security policies MUST enforce scope isolation on user-scoped tables so cross-scope access is denied by default.
- **FR-025**: Shared tables MUST support configurable access levels enforced via PostgreSQL role and policy mechanisms.
- **FR-026**: Storage credentials in `kalam.storage` MUST be accessible only to privileged roles; DML on system catalog tables MUST be restricted to administrative roles.

#### FILE Datatype

- **FR-027**: System MUST provide a FILE column type that stores a structured reference (id, subfolder, name, size, mime, checksum, shard) in-row while persisting bytes in object storage.
- **FR-028**: FILE uploads MUST route to shared or scope-specific storage paths matching the hosting table type.
- **FR-029**: FILE subfolder rotation and counts MUST be tracked in manifest `files` state consistent with kalamdb.

#### System Catalog

- **FR-030**: `kalam.storage` MUST register storage backends with type, base path, credentials, config, and path templates.
- **FR-031**: `system.schemas` MUST version table definitions including columns, options, storage binding, table type, access level, indexed columns, and bloom/stats column mapping. On `CREATE EXTENSION`, system catalog tables MUST be created automatically; DDL/event hooks MUST record `ALTER TABLE` changes on managed tables without manual catalog updates.
- **FR-031a**: `DROP TABLE` on a managed table MUST remove associated object-storage artifacts (manifest, Parquet segments, FILE blobs) in addition to PostgreSQL catalog entries.
- **FR-032**: `system.jobs` MUST track background work (flush, compact, cleanup, backup, restore) with status, parameters, idempotency keys, retries, and error traces.
- **FR-033**: Operators MUST be able to inspect sync state and manifest cache entries via SQL on `kalam.manifest`.

#### Operability & Safety

- **FR-034**: System MUST be decomposable into modular, independently testable responsibilities (metadata catalog, hot row access, cold I/O, flush orchestration, security enforcement, FILE handling) with boundaries defined during planning.
- **FR-035**: Extension MUST follow PostgreSQL extension best practices: safe upgrade paths, minimal shared-memory footprint, crash-safe background workers, and privilege separation.
- **FR-036**: System MUST NOT depend on RocksDB, Raft, or an external SQL/query planner; coordination MUST use PostgreSQL-native mechanisms (catalog tables, background workers, advisory locks). DataFusion MAY be used solely as the internal cold Parquet scan engine inside unified hot+cold merge reads—not as the primary query planner.
- **FR-044**: System MUST document that PostgreSQL base backup and PITR do not include cold Parquet segments or FILE blobs on object storage.
- **FR-045**: System MUST expose operator SQL functions to export backup manifests, validate cold storage consistency, and recover/reconcile cold segment metadata (e.g., `kalam.backup_manifest()`, `kalam.validate_cold_storage()`, `kalam.recover_segments()`).
- **FR-046**: Cold storage backup and recovery MUST be operator-managed via pg-kalam tooling and object-store procedures in MVP; the system MUST NOT imply that PostgreSQL backup alone preserves full managed-table data.
- **FR-047**: Extension binary MUST minimize size: include only required DataFusion/Parquet modules for cold scan; avoid unused codecs, SQL parsers, or analytics subsystems.
- **FR-048**: System MUST provide `kalam_exec(sql text)` for kalamdb-compatible administrative commands including table export and import.
- **FR-049**: System MUST provide a change-feed query surface (e.g., `kalam.changes_since(table, seq)` or session mode `kalam.changelog = on`) that returns all appended versions including tombstones ordered by `_seq`, without PK merge collapsing delete events away.

### Key Entities

- **Managed Table (Shared)**: Application table with `_seq`, `_deleted`, optional flush policy; single manifest; hot rows in PostgreSQL; cold rows in Parquet segments.
- **Managed Table (User-Scoped)**: Same as shared plus mandatory scope identifier; manifest per scope; RLS enforced per session scope.
- **Scope Identifier**: Optional partition key (`user_id` initially) generalizable to tenant/entity/device identifiers; drives manifest path, RLS, and FILE storage isolation.
- **Storage Registration (`kalam.storage`)**: Named object-store backend with path templates and credentials for cold and FILE data.
- **Table Schema Registry (`system.schemas`)**: Versioned definition of managed tables, columns, options, and storage routing.
- **Manifest (cold)**: JSON document on object storage listing Parquet segments, sequence bounds, file subfolder state, and vector index placeholders compatible with kalamdb.
- **Manifest Cache (`kalam.manifest`)**: Local PostgreSQL cache of manifest metadata and sync state for scheduling and observability.
- **Background Job (`system.jobs`)**: Durable record of flush and maintenance work with idempotency and retry metadata.
- **Parquet Segment**: Immutable cold batch file with row data sorted by `_seq`, segment stats, and schema version.
- **FILE Reference**: JSON metadata stored in-row pointing to blob location in object storage; blob stored outside Parquet.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Administrators can migrate an existing table with up to 1 million rows to pg-kalam shared or user-scoped mode in a single maintenance window without application query rewrites (same table name).
- **SC-002**: After flush, queries return merged hot+cold results identical to pre-flush logical content for 100% of tested primary-key lookups in acceptance test suites.
- **SC-003**: User-scoped tables reject 100% of cross-scope access attempts in security test scenarios when `kalam.user_id` is set or missing appropriately.
- **SC-004**: Configured row-based flush policy archives batches within 2 flush scheduler cycles (default check interval 60 seconds) after threshold is exceeded under normal load.
- **SC-005**: Cold storage artifacts (manifest.json and batch Parquet files) produced by pg-kalam are readable by kalamdb-compatible tooling without format conversion.
- **SC-006**: Hot-only tables (no flush policy) introduce no measurable cold-storage I/O during steady-state operation.
- **SC-007**: Operators can determine manifest sync state and last job outcome for any managed table via SQL on system catalog tables without object-store console access.
- **SC-008**: FILE uploads up to 100 MB complete with retrievable reference metadata and verified checksum in acceptance tests.
- **SC-009**: Operators can export a backup manifest, validate cold storage, and reconcile segment metadata for any managed table via pg-kalam SQL functions; documentation clearly states cold objects are outside PostgreSQL PITR.
- **SC-010**: `CREATE TABLE ... USING kalamdb WITH (...)` completes with standard PostgreSQL types and `SNOWFLAKE_ID()` defaults without auxiliary setup steps beyond extension install and storage registration.
- **SC-011**: Cold-row delete via `kalam.delete()` suppresses the row in 100% of default merged SELECT tests; the tombstone version is visible in 100% of change-feed queries with `_deleted = true` and higher `_seq`.
- **SC-013**: Sequential INSERT → UPDATE → DELETE on the same PK produces three distinct monotonic `_seq` values queryable via change-feed surface.
- **SC-012**: Parquet pruning tests skip ≥90% of row groups on PK-point lookups using footer bloom/stats without reading full segment files.

## Assumptions

- Target users are PostgreSQL administrators and application developers already running PostgreSQL 15+ (exact minimum version determined during planning).
- Object storage backends and path templates match kalamdb conventions for interoperability of cold artifacts.
- Primary keys exist on all migrated tables; composite primary keys are supported.
- Snowflake-style or equivalent monotonic `_seq` generation is acceptable for versioning (same semantics as kalamdb).
- Updates and deletes are append-versioned: each mutation appends a new row with a new `_seq` (never in-place). Tombstones remain visible to change-feed consumers for future realtime delete propagation.
- Native PostgreSQL UNIQUE indexes and FOREIGN KEY constraints on managed tables apply to hot heap rows only; they do not cover cold-only rows in MVP.
- Session user context for user-scoped tables is set via `SET kalam.user_id = '...'` and read via `kalam_user_id()`.
- Parquet write path follows kalamdb: PK columns get bloom filters (row_count ≥ 1024); PK + `_seq` columns get min/max stats in manifest `column_stats`.
- Flush and manifest write semantics mirror kalamdb: Parquet temp+rename first, then full `manifest.json` rewrite with all committed segments; `kalam.manifest` tracks `pending_write` → `syncing` → `in_sync`.
- Background flush workers run inside PostgreSQL (e.g., background worker or scheduled job pattern)—no external orchestrator required for MVP.
- Stream tables and vector index cold paths are out of scope for initial release unless already present in migrated kalamdb manifests.
- Compaction of small Parquet segments is optional post-MVP; flush is required for MVP.
- Existing kalamdb flush policy syntax (`rows:N`, `interval:seconds`, combined) is reused verbatim for operator familiarity.
- Multi-node PostgreSQL (Patroni/replicas) is supported read-only on replicas; flush workers run on primary only.
- Cold Parquet segments and FILE blobs on object storage are outside PostgreSQL base backup/PITR; operators use pg-kalam backup/validate/recover SQL plus object-store backup procedures for full recovery.

## Out of Scope (Initial Release)

- Embedded RocksDB or alternative hot store outside PostgreSQL heap
- Transparent SQL `UPDATE`/`DELETE` on rows that exist only in cold storage (use `kalam.update()` / `kalam.delete()` in MVP)
- Global hot+cold UNIQUE or FOREIGN KEY enforcement across merged logical rows
- DataFusion or any external engine as the primary SQL/query planner (joins, aggregates, permissions)
- Using DataFusion for analytics workloads beyond cold Parquet segment scans inside unified merge reads
- Raft or distributed leader election for job coordination
- Real-time streaming table type (full subscription transport is post-MVP; change-feed `_seq` visibility is in MVP)
- Automatic cross-region replication of cold storage
