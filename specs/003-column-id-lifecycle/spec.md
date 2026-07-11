# Feature Specification: Catalog-Owned Column Identity, Schema Versions, and Segment Lifecycle

**Feature Branch**: `003-column-id-lifecycle`

**Created**: 2026-07-11

**Status**: Draft

**Input**: User description: "Add stable column_id (copy KalamDB design; KoldStore will replace KalamDB). Use column_id in catalog and segments. Support ALTER TABLE rename/add/drop/alter like KalamDB. Add explicit file lifecycle states. Rethink catalog organization so catalog-related work lives in one catalog home with easy schema-version access. Rename cold files to `segment-0017.parquet` (not `batch-...`). Use `column_id` in Parquet. After write, derive column stats from the written Parquet footer instead of filling stats twice. On DML, maintain in-memory row counters keyed by (table, optional scope); pre-flush creates pending segments from counters that reached threshold; flush drains pending segments (scoped by scope value for user tables). One unified counter/pending-segment mechanism for User (scoped) and Shared (unscoped) tables, aligned with KalamDB."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - One catalog home with easy schema versions (Priority: P1)

As a developer and operator working on managed tables, I need all catalog concerns—schema registry versions, logical columns, cold segment records, and related lookups—owned in one catalog place with straightforward access to active and historical schema versions.

**Why this priority**: Today catalog responsibility is split across schema models, migrate writes, flush segment writes, and a narrow catalog read crate. Without a single catalog home and easy version access, column-id evolution, ALTER, and segment metadata will keep drifting.

**Independent Test**: From the catalog surface alone, load the active schema version for a managed table, load a prior version by number, list columns with stable IDs for both versions, and resolve a cold segment’s schema version to its column set—without callers needing to assemble that from multiple unrelated owners.

**Acceptance Scenarios**:

1. **Given** a managed table with multiple schema versions, **When** a caller asks for the active version, **Then** the catalog returns that version’s full logical column set (including `column_id`, name, type, activity) in one access path.
2. **Given** a historical schema version number, **When** a caller requests that version, **Then** the catalog returns the exact column definitions that were active then.
3. **Given** catalog-related reads and writes for schemas, columns, and cold segments, **When** ownership is reviewed, **Then** those concerns are concentrated in the catalog ownership boundary (not scattered across unrelated feature owners), with a clear, easy API for versioned schema access.

---

### User Story 2 - Stable column identity in catalog and cold files (Priority: P1)

As an operator, I need every managed column to have a permanent `column_id` used by the catalog, segment stats, and cold file field identity so renames and older files stay aligned—matching KalamDB’s `column_id` / `next_column_id` model that KoldStore will replace.

**Why this priority**: Name-only identity breaks rename and long-lived cold data. KalamDB already treats `column_id` as permanent and keys segment stats by it; KoldStore must adopt the same design.

**Independent Test**: Manage a table, flush, rename a non-PK column, flush again, query old and new rows by the new name; confirm `column_id` unchanged, cold files carry that identity, and catalog/segment stats are keyed by `column_id`. Drop then add another column and confirm IDs are never reused.

**Acceptance Scenarios**:

1. **Given** a managed table, **When** columns are registered, **Then** each application column receives a permanent numeric `column_id` and the table tracks the next unused ID.
2. **Given** a rename, **When** schema metadata updates, **Then** `column_id` stays the same and only the display name changes.
3. **Given** a drop then later add, **When** the new column is assigned an ID, **Then** it does not reuse any previously assigned ID for that table.
4. **Given** newly written cold files, **When** inspected for field identity, **Then** each application column field carries the matching `column_id` (not name-only identity).
5. **Given** catalog and segment column statistics, **When** pruning or inspecting bounds, **Then** stats are keyed by `column_id` so rename does not orphan them.

---

### User Story 3 - KalamDB-aligned ALTER evolution (Priority: P1)

As an application owner, I want add, rename, drop, and compatible alter-column changes on managed PostgreSQL tables to work like KalamDB: new schema versions, stable IDs, readable older cold data, and clear failure for unsafe changes.

**Why this priority**: Production schemas change continuously. KoldStore must observe PostgreSQL ALTER outcomes and apply the same logical evolution rules KalamDB already uses.

**Independent Test**: On a managed table with cold rows, run ADD, RENAME, DROP (non-PK), and compatible type change; after each, verify version advance, reads of pre- and post-change rows, and that incompatible/PK changes fail closed without hot prune.

**Acceptance Scenarios**:

1. **Given** existing cold rows, **When** a column is added, **Then** a new schema version records a new `column_id`, older files fill via the column’s initial backfill default (or NULL), and insert defaults may later change without rewriting history.
2. **Given** cold data under an old name, **When** the column is renamed, **Then** the same `column_id` keeps historical values readable under the new name without rewriting cold files.
3. **Given** a non-PK column, **When** it is dropped, **Then** its `column_id` becomes inactive and is never reused; live queries stop exposing it.
4. **Given** a compatible type promotion, **When** applied, **Then** the same `column_id` records the new type and cold reads apply compatibility rules.
5. **Given** incompatible type change, unsupported type, or primary-key reshape, **When** refresh/flush runs, **Then** the change fails with a clear error and hot data is not pruned.

---

### User Story 4 - Deterministic `segment-NNNN` cold file names (Priority: P2)

As an operator inspecting object storage, I want cold segment files named `segment-0017.parquet` (zero-padded segment number) instead of `batch-...`, so paths are stable, sortable, and clearly segment-scoped.

**Why this priority**: Current `batch-{n}.parquet` naming is easy to confuse with flush job batches and does not match the desired long-term object layout.

**Independent Test**: Flush several segments and confirm object paths use `segment-0001.parquet`, `segment-0002.parquet`, … with monotonic numbers per scope prefix; manifests and catalog `object_path` values match those names.

**Acceptance Scenarios**:

1. **Given** a successful flush that publishes a new cold file, **When** the object path is recorded, **Then** the filename matches `segment-{NNNN}.parquet` with a zero-padded numeric segment sequence (for example `segment-0017.parquet`).
2. **Given** multiple segments in one scope, **When** listed lexicographically, **Then** order matches numeric segment order.
3. **Given** catalog and manifest entries, **When** compared to object storage, **Then** stored paths use the new naming scheme for newly written files.

---

### User Story 5 - Single-source column stats after Parquet write (Priority: P1)

As a maintainer of the flush path, I need catalog `column_stats` to come from the written cold file’s footer statistics after encode finishes—not from a second parallel min/max tracker during encode—so stats, `column_id`, and file contents cannot disagree.

**Why this priority**: Today encode-time bound tracking and Parquet chunk statistics both compute similar min/max. That duplication is wasteful and can drift. The accepted product decision is: Parquet write owns statistics; catalog publishes the aggregated footer-derived bounds (keyed by `column_id`) without opening every file again at query prune time.

**Independent Test**: Flush a multi-row-group segment; assert catalog/manifest column stats equal footer-derived aggregates for indexed/`column_id` columns; assert encode path no longer maintains a separate per-cell bounds map for catalog publish; assert prune still uses catalog stats without opening every candidate file.

**Acceptance Scenarios**:

1. **Given** a flush about to publish catalog stats, **When** encode completes, **Then** segment-level min/max for required columns are derived from the written file’s footer statistics (using the in-memory bytes already held for validate/publish—not a separate post-publish download).
2. **Given** those footer-derived stats, **When** stored in catalog/manifest, **Then** they are keyed by `column_id` and remain the authority for segment-level prune-before-open.
3. **Given** the encode path, **When** building Arrow batches for write, **Then** it does not maintain a duplicate catalog bounds accumulator solely to republish the same min/max the footer already contains.
4. **Given** types that need domain-preserving catalog JSON (for example timestamptz-style values), **When** footer physical stats are converted, **Then** conversion is type-aware and never publishes bounds that could falsely exclude a segment; unsupported/inexact cases omit that column or fail flush for required columns.
5. **Given** row-group pruning inside an opened file, **When** scanning, **Then** footer/bloom statistics remain the authority inside the file, consistent with catalog segment bounds.

---

### User Story 6 - Explicit cold-file lifecycle states (Priority: P2)

As an operator running flush, compaction, and recovery, I need every cold file/segment to move through explicit lifecycle states so crash recovery and retention are unambiguous and plug into existing durable jobs.

**Why this priority**: Coarse statuses are not enough for pending pre-flush reservations, staged publish, supersession, retention delete, and orphan cleanup alongside leases/phases/checkpoints.

**Independent Test**: Create pending segments via pre-flush, interrupt mid-write and recover (`pending`/`staged` → `published` or retryable recovery). Compact a segment (`published` → `superseded` → `deleting` → `deleted`). Reconcile an unreferenced object as `orphaned` via durable jobs.

**Acceptance Scenarios**:

1. **Given** a pre-flush that reserves work for a scope (or the whole shared table), **When** the catalog row is created before any object write, **Then** the segment is `pending`.
2. **Given** a flush writing a temp cold object for a pending segment, **When** validation completes but publish has not, **Then** the segment is `staged`.
3. **Given** successful manifest/snapshot publish and verified cold file, **When** commit succeeds, **Then** the segment is `published` (completed) and query-visible, and only then are corresponding hot/mirror rows removed.
4. **Given** replacement by compaction, **When** the new file is published, **Then** the old file becomes `superseded`.
5. **Given** retention eligibility, **When** cleanup runs, **Then** the file moves `deleting` → `deleted` idempotently under job leases/retries.
6. **Given** an unreferenced object after crash/partial failure (and no valid owning lease), **When** reconciliation runs, **Then** it is `orphaned` and cleaned through durable jobs—not ad-hoc deletes.
7. **Given** a crash or failed flush mid-transition, **When** jobs resume, **Then** the segment remains recoverable and retryable without losing rows or creating duplicate visible rows.

---

### User Story 7 - In-memory scope counters and pending-segment flush initiation (Priority: P1)

As an operator of managed User and Shared tables, I need DML to bump lightweight in-memory row counters (not catalog segment rows), and flush to start from a pre-flush step that turns threshold-reaching counters into pending segments—then the existing flush write/verify/publish/hot-prune path drains those pending segments (scoped when a scope column exists).

**Why this priority**: Creating or updating catalog segment rows on every insert is too expensive. KalamDB already uses a pending-write / per-scope flush initiation pattern; KoldStore must unify User (scoped) and Shared (unscoped) onto one counter → pending-segment → flush mechanism, with manual flush as the current initiator.

**Independent Test**: Insert into a scoped user table across multiple scope values and into a shared table; confirm counters advance in memory only (no per-insert segment rows). Invoke flush; pre-flush creates pending segments only for keys at/above threshold (and for force/manual policy as configured); flush drains pending segments, writing Parquet per scope for user tables and one shared stream for shared tables; crash mid-flush leaves pending/staged work retryable without duplicates.

**Acceptance Scenarios**:

1. **Given** a managed user-scoped table (`user_id` / `tenant_id` / `device_id` / configured scope column), **When** a row is inserted (and mirrored), **Then** an in-memory counter for key `(table, Some(scope_value))` increments and no catalog segment row is created or updated for that insert alone.
2. **Given** a managed shared table (no scope column), **When** a row is inserted (and mirrored), **Then** an in-memory counter for key `(table, None)` increments using the same counter mechanism as scoped tables.
3. **Given** existing separate scoped vs unscoped flush/counter paths, **When** this feature lands, **Then** there is one unified mechanism for User and Shared table types (extend or delete duplicates—no parallel designs).
4. **Given** the operator initiates flush for a table (manual flush is the supported initiator today), **When** the pre-flush job runs, **Then** it gathers all in-memory counter keys for that table id, and for each key that has reached the configured segment threshold it creates a `pending` segment row representing a snapshot/range of hot rows ready for cold storage.
5. **Given** pending segments exist after pre-flush, **When** the flush job runs, **Then** it scans pending segments and, for each, writes rows to Parquet/object storage (filtering by scope value for scoped tables), verifies the cold file, advances status to completed/`published`, and only then removes the corresponding rows from PostgreSQL hot/mirror storage—reusing the proven write/verify/prune path, with pending segments as the initiator input.
6. **Given** a flush failure or PostgreSQL crash after pending creation or during write, **When** work is retried, **Then** segments remain recoverable without losing hot rows or creating duplicate query-visible cold rows.
7. **Given** multiple scopes (or multiple threshold slices) ready at once, **When** pre-flush/flush run, **Then** multiple segments may be `pending` or flushing concurrently without corrupting counters or visibility.

**Workflow (normative)**:

```text
DML in PostgreSQL
  → mirror the row
  → increment in-memory map key: (table_id, Optional<scope_value>)
Operator chooses to flush a table
  → pre-flush job gathers keys for that table_id
  → creates pending segment rows for keys at/above segment threshold
  → flush job loads pending segments
  → for each pending segment: write cold file (scoped when scope present)
       → verify → mark published/completed → then prune hot/mirror rows
```

---

### Edge Cases

- Rename that collides with an existing live column name fails before schema refresh commits.
- Drop or reshape of primary-key columns remains unsupported and fails closed.
- Concurrent ALTER during flush: no segment published under a mismatched schema version.
- Pre-cutover cold formats (`batch-*.parquet`, name-keyed stats, files without field identity) are unsupported; recreate managed tables after upgrade.
- Orphan detection must not classify a still-leased staged flush object as orphaned.
- Footer stats missing or inexact for a required prune column fail flush or omit that key—never false-exclude.
- Multi–row-group files aggregate min-of-mins / max-of-maxs correctly, including null-only groups.
- Catalog version API must not return a mix of columns from different versions for one request.
- Counter map must not create durable segment rows on DML; only pre-flush materializes pending segments.
- Backend restart may lose in-memory counters: flush/pre-flush MUST remain correct by rebuilding or reconciling from mirror/hot state when counters are cold/missing (no silent under-flush that drops durability guarantees).
- Concurrent pending segments for different scopes of the same user table must not block each other incorrectly; shared-table `(table, None)` uses the same queue semantics.
- Force flush / explicit operator flush may create pending segments for below-threshold counters when the product policy requires draining all hot work for that table.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST concentrate managed-table catalog ownership (schema versions, logical columns with `column_id`, cold segment metadata, and versioned schema access) in the catalog ownership boundary so callers have one obvious place for catalog reads/writes related to these concerns.
- **FR-002**: Catalog MUST provide easy access to the active schema version and to any historical schema version by version number for a managed table.
- **FR-003**: Every managed application column MUST have a permanent numeric `column_id` that never changes for that logical column’s lifetime.
- **FR-004**: Each managed table MUST maintain a monotonically increasing next-column-id allocator so new columns get fresh IDs and dropped IDs are never reused.
- **FR-005**: Column display names MUST be mutable metadata on a `column_id`, not the primary identity.
- **FR-006**: Newly written cold files MUST embed stable field identity equal to each application column’s `column_id`.
- **FR-007**: Cold readers MUST resolve fields by `column_id` / field identity only; pre-cutover files without field identity are unsupported.
- **FR-008**: Catalog and segment `column_stats` MUST be keyed by `column_id`.
- **FR-009**: After cold-file encode finishes, catalog/manifest segment `column_stats` MUST be derived from that file’s footer statistics (from bytes already held for validate/publish), not from a parallel encode-time bounds tracker.
- **FR-010**: Encode MUST NOT maintain a duplicate per-cell catalog bounds map whose only purpose is to republish min/max already present in footer statistics.
- **FR-011**: Footer-to-catalog conversion MUST be type-aware and MUST NOT publish bounds that can falsely exclude a segment; unsupported/inexact stats omit that column or fail flush for required columns.
- **FR-012**: Segment-level prune-before-open MUST continue to use catalog/manifest stats only (no open-every-file to read footers for prune).
- **FR-013**: System MUST detect and apply PostgreSQL-driven managed-table changes for add column, drop non-PK column, rename column, and compatible alter-column type changes using KalamDB-aligned logical rules (`column_id` stability, version advance, dual defaults where applicable).
- **FR-014**: On add column, system MUST record an initial backfill default for older files separately from any later-changed insert default.
- **FR-015**: On rename, system MUST keep `column_id` unchanged and leave existing cold files untouched.
- **FR-016**: On drop, system MUST mark `column_id` inactive and never reuse it.
- **FR-017**: Incompatible type changes, unsupported types, and primary-key membership changes MUST fail closed without pruning hot rows.
- **FR-018**: Newly published cold object filenames MUST use `segment-{NNNN}.parquet` with a zero-padded numeric segment sequence (example: `segment-0017.parquet`), not `batch-{n}.parquet`.
- **FR-019**: Catalog and manifest `object_path` values for new segments MUST match the `segment-{NNNN}.parquet` naming scheme.
- **FR-020**: Every cold file/segment MUST carry an explicit lifecycle state: `pending`, `staged`, `published`, `superseded`, `deleting`, `deleted`, or `orphaned`.
- **FR-021**: Lifecycle transitions MUST follow pre-flush reservation → `pending` → write temp → `staged` → validate → publish → `published` → (optional) `superseded` → retention → `deleting` → `deleted`, with `orphaned` for unreferenced leftovers, integrated with existing durable jobs (leases, phases, checkpoints, retries).
- **FR-022**: Only `published` files for the relevant snapshot/manifest generation are query-visible.
- **FR-023**: Column identity, ALTER semantics, and `column_id`-keyed segment stats MUST stay aligned with KalamDB’s stable design so KoldStore can replace that path without a second incompatible scheme.
- **FR-024**: Operators MUST be able to observe schema version, column ids/names, segment paths, lifecycle state, and column stats from catalog-facing metadata.
- **FR-025**: On DML into a managed table, after the row is mirrored, the system MUST increment an in-memory row counter keyed by `(table_id, Optional<scope_value>)` and MUST NOT create or update a durable catalog segment row for that insert alone.
- **FR-026**: User-scoped tables MUST key counters by the configured scope column value; shared tables MUST use the same counter mechanism with an empty/absent scope (`Optional::None`)—one unified path for User and Shared table types.
- **FR-027**: Existing divergent scoped vs unscoped counter/flush initiation logic MUST be extended or removed so only one mechanism remains (no duplicate parallel designs).
- **FR-028**: When flush is initiated for a table (manual flush is the supported initiator in this phase), a pre-flush job MUST gather in-memory counter keys for that table and create `pending` segment rows for keys that have reached the configured segment threshold (and for explicit force/drain policy when applicable).
- **FR-029**: Each `pending` segment MUST represent a snapshot or range of hot/mirror rows ready to move to cold storage for that table and optional scope.
- **FR-030**: The flush job MUST scan `pending` segments and, for each, write Parquet/object storage (scoped by scope value when present), verify successful cold write, mark the segment completed/`published`, and only then remove corresponding rows from PostgreSQL hot/mirror storage—reusing the current proven write/verify/prune path with pending segments as the initiator input.
- **FR-031**: If flush fails or PostgreSQL crashes, affected segments MUST remain recoverable and retryable without losing rows or creating duplicate query-visible cold rows.
- **FR-032**: Multiple segments for the same table MAY be `pending` or flushing concurrently (including different scopes).
- **FR-033**: In-memory counters are advisory for thresholding; after process restart, pre-flush/flush MUST reconcile from durable mirror/hot state so correctness does not depend on surviving memory alone.

### Key Entities

- **Catalog (ownership boundary)**: Single home for managed schema versions, logical columns, cold segment records, and easy versioned schema access.
- **Logical Column**: Permanent `column_id`, mutable name, type, nullability, activity, initial backfill default, insert default, schema-version lineage.
- **Schema Version**: Point-in-time column set and primary-key shape; addressable as active or by version number.
- **Scope Counter Key**: `(table_id, Optional<scope_value>)` — present scope for User tables, absent for Shared tables; same map/mechanism for both.
- **In-Memory Row Counter**: Process-local count of mirrored DML effects per Scope Counter Key; never alone durable; used to decide pending-segment creation at pre-flush.
- **Pending Segment**: Catalog segment row created by pre-flush for a key at/above segment threshold; describes the hot/mirror row snapshot/range to flush; not query-visible until published.
- **Cold Segment File**: Object named `segment-{NNNN}.parquet`, carrying field identities, footer statistics, schema version at write, lifecycle state, and catalog stats keyed by `column_id`.
- **Footer-Derived Column Stats**: Segment-level min/max aggregated from written-file footer statistics after encode; published into catalog/manifest for prune-before-open.
- **File Lifecycle State**: `pending` | `staged` | `published` | `superseded` | `deleting` | `deleted` | `orphaned`.
- **Pre-Flush Job**: Gather table counter keys, materialize `pending` segments for threshold (or force) policy.
- **Flush Job**: Drain `pending` segments through write → verify → publish → hot/mirror prune; durable leases/phases/checkpoints.
- **Durable Job Context**: Existing flush/compaction/GC-style work owning lifecycle transitions via lease/phase/checkpoint/retry.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Callers can retrieve active and historical schema versions (including full `column_id` column sets) through the catalog access path in under one lookup step per version request in automated tests.
- **SC-002**: After non-PK rename with prior cold data, 100% of sampled historical values for that logical column remain readable under the new name without rewriting old cold files.
- **SC-003**: Across at least 20 add→drop→add cycles, newly added columns never reuse a prior `column_id`.
- **SC-004**: Supported ALTER flows (add, rename, drop non-PK, compatible type change) each produce a usable new schema version and successful reads of pre- and post-change rows in end-to-end tests.
- **SC-005**: Unsupported/incompatible schema changes fail before hot prune or cold publish in 100% of tested negative cases.
- **SC-006**: 100% of newly flushed cold objects in tests use `segment-{NNNN}.parquet` naming; no new writes use `batch-*.parquet`.
- **SC-007**: For flushed multi-row-group segments, catalog `column_stats` for required columns match footer-derived aggregates in automated checks, and the encode path has no separate catalog bounds accumulator for those columns.
- **SC-008**: Interrupted flush recovery never leaves unpublished files query-visible; publish is exactly-once or cleaned as pending/staged/orphaned and retryable without row loss or duplicate visible cold rows.
- **SC-009**: Superseded files stop being query-visible when their replacement publishes; physical deletion occurs only via `deleting` → `deleted` after retention.
- **SC-010**: A migration checklist can map KalamDB table column identities to KoldStore catalog entries one-to-one on `column_id`.
- **SC-011**: Under sustained DML, catalog segment row count does not increase on every insert; pending segments appear only after pre-flush for threshold-reaching (or force-drained) counter keys.
- **SC-012**: For a user-scoped table with N distinct scope values each above threshold, one flush cycle creates and completes pending segments such that each scope’s eligible hot rows are flushed under that scope (no cross-scope mixing in a scoped segment).
- **SC-013**: Shared and user-scoped tables use the same counter and pending-segment APIs in tests (only the optional scope key differs); duplicate parallel initiation paths are absent.

## Assumptions

- Iterative refinements stay in `specs/003-column-id-lifecycle` (do not split into a new feature folder for the same work).
- **Hard cutover**: no backward compatibility, dual codecs, or keep-alive of pre-cutover naming/status/stats-key schemes; delete superseded code in the same change.
- Prefer copying stable KalamDB designs (`column_id`, `next_column_id`, ALTER rules, `column_id`-keyed stats, pending-write / per-scope flush initiation) over inventing parallel schemes.
- Footer-derived catalog stats: keep catalog prune-before-open; source write-time catalog bounds from written-file footer statistics; delete duplicate encode-time bounds tracking.
- PostgreSQL remains DDL authority; KoldStore observes catalog changes and refreshes versioned registry metadata.
- Rename detection correlates PostgreSQL `attnum` to KoldStore `column_id`.
- Zero-padding width for `segment-{NNNN}` defaults to at least four digits (`0017`).
- Dual defaults (`initial_default` vs insert default) follow the DuckLake/KalamDB lesson.
- File lifecycle plugs into existing durable jobs rather than replacing them.
- Primary-key changes and full time-travel APIs remain out of scope except fail-closed behavior.
- Exact DuckLake SQL schema parity is not required.
- Catalog ownership = versioned schema access + cold segments in one catalog boundary; type-matrix leaf may remain separate without a second registry API.
- Manual flush is the supported initiator in this phase; background scheduling may later call the same pre-flush → pending → flush path.
- Segment threshold is a configured policy (rows per scope/table); exact default may match existing flush policy knobs or a dedicated segment threshold—one clear setting, not two competing counters.
- Proven flush write/verify/publish/hot-prune behavior is retained; this feature changes **initiation** (counters + pending segments), not the successful cold visibility boundary.
