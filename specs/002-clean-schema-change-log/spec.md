# Feature Specification: Clean Schema Change-Log Mirrors

**Feature Branch**: `main`

**Created**: 2026-07-05

**Updated**: 2026-07-05

**Status**: Draft

**Input**: User description: "Refactor pg-koldstore so enabling a table no longer adds internal columns to the user's table by default. Use one per-table latest-state change-log mirror in the koldstore schema, preserve primary-key shape and delete state through flush, evaluate default row-limit and duration flush policies from mirror state, migrate changes_since to the mirror model, retire old row-events and system-column paths that are no longer needed, and remove KoldStore artifacts safely on demigration."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Enable KoldStore Without Polluting Business Schema (Priority: P1)

An application owner enables pg-koldstore on an existing or new business table and keeps the table's application-facing schema unchanged.

**Why this priority**: Clean schema is the central promise of this refactor. Users must be able to adopt hot/cold storage without adding KoldStore columns to business tables, generated clients, ORMs, dumps, or application queries.

**Independent Test**: Capture the table definition before enablement, enable pg-koldstore, and compare the resulting table columns, primary key, constraints, and indexes visible to application roles.

**Acceptance Scenarios**:

1. **Given** `public.messages` has an application primary key and no KoldStore columns, **When** pg-koldstore is enabled for the table, **Then** `public.messages` has the same application columns as before enablement and does not gain `_seq`, `_deleted`, `_commit_seq`, `_user_id`, or any other internal KoldStore column by default.
2. **Given** the enabled table has a single-column primary key, **When** enablement completes, **Then** a matching change-log mirror exists in the `koldstore` schema with that primary key column using the same PostgreSQL data type and primary-key-required configuration.
3. **Given** the enabled table has a composite primary key, **When** enablement completes, **Then** the change-log mirror contains all primary key columns in the same primary-key order with the same PostgreSQL data types and primary-key-required configuration.
4. **Given** the source table has no primary key, **When** pg-koldstore enablement is requested, **Then** the request is rejected without changing the user table or creating partial KoldStore artifacts.

---

### User Story 2 - Track Latest Hot State in a Per-Table Mirror (Priority: P1)

An application continues using normal INSERT, UPDATE, DELETE, and reinsert flows while pg-koldstore records the latest change state outside the business table.

**Why this priority**: The change-log mirror replaces internal user-table columns as the default hot replay and flush coordination state. It must be correct before flush, merge, and demigration can be safe.

**Independent Test**: Enable a table, run INSERT, UPDATE, DELETE, and reinsert operations for the same primary key, and verify the mirror has exactly one latest-state row for that key after each operation.

**Acceptance Scenarios**:

1. **Given** an enabled table, **When** a row is inserted, **Then** the mirror contains the primary key, `op = 1`, a new sequence value, a change timestamp, and no duplicate mirror row for that key.
2. **Given** a mirrored inserted row, **When** the base row is updated, **Then** the mirror row for the same primary key is updated in place with `op = 2` and a newer sequence value.
3. **Given** a mirrored live row, **When** the base row is deleted, **Then** the mirror keeps one tombstone row with `op = 3` and a newer sequence value.
4. **Given** a primary key currently has a mirror tombstone, **When** the same primary key is inserted again later, **Then** the existing mirror row changes back to `op = 1` with a newer sequence value.
5. **Given** multiple transactions touch the same primary key, **When** they commit, **Then** the mirror reflects the latest committed state and never exposes more than one mirror row for that key.

---

### User Story 3 - Enable Populated Tables Safely (Priority: P1)

An administrator enables pg-koldstore on a table that already contains rows and expects those rows to become managed without first being moved to cold storage or changing the user table schema.

**Why this priority**: Existing-table migration is the adoption path most likely to expose data loss or noisy schema changes. Registration should make rows manageable first, then let normal flush policy decide when to move them cold.

**Independent Test**: Create a populated table, enable pg-koldstore, verify every pre-existing primary key has a live mirror entry, perform concurrent or follow-up DML during migration, and verify newer DML is not overwritten by the initial mirror backfill.

**Acceptance Scenarios**:

1. **Given** a table already has rows, **When** pg-koldstore enablement starts, **Then** the table-specific mirror and change capture are established before rows are eligible for flush cleanup.
2. **Given** pre-existing rows are present, **When** mirror initialization runs, **Then** each existing primary key receives a live mirror entry with `op = 1` and a sequence value while the base row remains in the user table.
3. **Given** a row is updated or deleted while populated-table initialization is in progress, **When** the initial mirror backfill reaches that primary key, **Then** the backfill must not overwrite the newer committed mirror state.
4. **Given** mirror initialization has not completed, **When** a flush is requested, **Then** flush must skip the table or scope until the mirror has a complete, safe cutoff.
5. **Given** mirror initialization succeeds, **When** normal flush policy later runs, **Then** eligible rows may be flushed from oldest to newest using the recorded mirror sequence state.

---

### User Story 4 - Flush Clean-Schema State to Cold Storage (Priority: P1)

An operator flushes eligible hot rows to cold storage while deletes and reinserts remain logically correct even though the business table has no internal tombstone columns.

**Why this priority**: Flush is where clean-schema metadata must become durable cold-state metadata. If delete markers are not preserved in cold storage, flushed rows can reappear incorrectly after hot cleanup.

**Independent Test**: Insert, update, delete, and reinsert rows; run a flush; verify cold artifacts contain base-table columns plus KoldStore change metadata, hot cleanup occurs only after a durable cold commit, and logical reads do not resurrect deleted rows.

**Acceptance Scenarios**:

1. **Given** eligible live mirror rows, **When** flush succeeds, **Then** cold storage receives rows containing the base-table columns and the corresponding sequence, operation, change timestamp, and delete state derived from the mirror.
2. **Given** eligible mirror tombstones, **When** flush succeeds, **Then** cold storage receives delete-marker records that include the primary key and enough KoldStore metadata to mask older cold rows without requiring row payload storage in the mirror.
3. **Given** flush commits cold artifacts successfully, **When** hot cleanup runs, **Then** matching base-table rows and mirror rows are removed only for changes included in the committed cold artifact and no longer needed for hot replay.
4. **Given** flush fails before the cold visibility boundary is committed, **When** cleanup is considered, **Then** base-table rows and mirror rows remain authoritative and are not removed.
5. **Given** a deleted key is reinserted after a tombstone was flushed, **When** queries resolve hot and cold state, **Then** the newer insert wins and the old delete marker does not hide the reinserted row.
6. **Given** a table uses the default row-limit policy such as `flush_policy => 'rows:1000'`, **When** pending mirror entries exceed the configured hot-row limit, **Then** pg-koldstore selects the oldest pending mirror entries by `seq` and flushes only that stable eligible set.
7. **Given** a table uses a duration policy such as `flush_policy => 'duration:1d'`, **When** mirror entries have `changed_at` older than the configured duration, **Then** pg-koldstore flushes those older rows and leaves newer mirror entries hot.

---

### User Story 5 - Disable KoldStore Safely (Priority: P1)

An administrator removes pg-koldstore management from a table and is left with a normal business table and no table-specific KoldStore artifacts.

**Why this priority**: A clean-schema default must include a clean exit path. Demigration must not leave triggers, mirror tables, metadata, or hidden columns behind.

**Independent Test**: Enable a table, mutate rows, flush at least one batch, demigrate the table, and verify the base table has the current logical rows and no table-specific KoldStore artifacts remain.

**Acceptance Scenarios**:

1. **Given** an enabled table has hot and cold state, **When** demigration runs in the default mode, **Then** the current logical rows are present in the original table before KoldStore metadata is detached.
2. **Given** demigration succeeds, **When** the table is inspected, **Then** the table has no KoldStore triggers, no KoldStore internal columns, and no dependency on a per-table mirror.
3. **Given** demigration succeeds, **When** the `koldstore` schema is inspected, **Then** the table-specific change-log mirror has been dropped and table metadata has been removed or marked inactive.
4. **Given** demigration cannot safely rehydrate or detach the table, **When** the operation fails, **Then** existing data and mirror state remain intact for retry.

---

### User Story 6 - Retire Legacy Internal State (Priority: P1)

A maintainer removes the old internal-state design so users and tests no longer see global row-event tables or system-column behavior as part of the extension contract.

**Why this priority**: Keeping unused legacy paths increases maintenance cost and can silently reintroduce schema noise. The extension is still in development, so the clean-schema model should replace the old behavior instead of carrying a migration or compatibility path.

**Independent Test**: Install the extension and enable a table, then verify the default catalog does not create the old global row-events table, old system-column guards are not part of the migrated table path, and the test suite no longer asserts legacy system-column behavior for clean-schema migrations.

**Acceptance Scenarios**:

1. **Given** the extension is installed for the clean-schema default, **When** catalog objects are inspected, **Then** the old global append-only row-event table is not created as a required default artifact.
2. **Given** clean-schema table enablement succeeds, **When** tests inspect the user table, **Then** no tests expect `_seq`, `_deleted`, `_commit_seq`, or `_user_id` to exist on the user table.
3. **Given** old tests covered row-events, system-column guards, hot tombstone columns, or direct writes to internal columns, **When** the test suite is updated, **Then** those tests are removed or replaced by mirror, flush, delete-marker, and demigration tests that validate the clean-schema contract.
4. **Given** code paths exist only to support the old internal-column or row-events design, **When** the clean-schema refactor is complete, **Then** those paths are removed rather than maintained behind a compatibility mode.

---

### User Story 7 - Read Latest-State Changes From Mirrors (Priority: P1)

A change consumer calls `koldstore.changes_since` and receives latest-state changes from the table-specific mirror and flushed mirror metadata instead of the old global row-events table.

**Why this priority**: The public change-feed surface must not keep the retired row-events table alive. It should report the clean-schema latest-state model directly.

**Independent Test**: Insert, update, delete, flush, and reinsert rows; call `koldstore.changes_since` with different cursors; verify the returned changes come from mirror/cold mirror metadata and no `koldstore.row_events` table is required.

**Acceptance Scenarios**:

1. **Given** a managed table has unflushed mirror rows, **When** `koldstore.changes_since(table, cursor, limit)` is called, **Then** returned changes are read from that table's change-log mirror using `seq` values newer than the cursor.
2. **Given** mirror rows were flushed and removed from hot replay, **When** `changes_since` reads a cursor that includes those flushed changes, **Then** returned changes can be reconstructed from cold records that include flushed mirror metadata.
3. **Given** the same primary key changed multiple times before the consumer polls, **When** `changes_since` is called, **Then** the feed returns the latest available state for that primary key rather than every intermediate event.
4. **Given** the old global row-events table does not exist, **When** `changes_since` is called, **Then** the function still works from mirror/cold metadata or returns a clear unsupported-range error when the requested cursor predates retained cold metadata.

---

### User Story 8 - Preserve User-Scoped Clean Schema (Priority: P2)

A multi-tenant application enables pg-koldstore while using an existing application scope column instead of accepting an automatically added internal scope column.

**Why this priority**: `_user_id` is also schema pollution. Clean-schema by default should reject missing scope information rather than silently changing the business table.

**Independent Test**: Enable a user-scoped table with an existing scope column and verify no internal scope column is added; then try enabling user scope without a valid scope column and verify the request fails cleanly.

**Acceptance Scenarios**:

1. **Given** a user-scoped table has an application-owned scope column, **When** pg-koldstore is enabled with that column, **Then** scope enforcement uses that column and the table schema remains unchanged.
2. **Given** a user-scoped table has no valid application-owned scope column, **When** clean-schema enablement is requested, **Then** enablement is rejected by default instead of adding `_user_id`.

### Edge Cases

- Existing populated tables: enablement must initialize mirror rows for already-present rows as live latest-state entries without adding columns to the user table, and must not flush/delete those rows during registration by default.
- Primary-key-changing updates: altering primary-key values or primary-key definitions is not implemented in this feature and must be rejected or documented as unsupported before it can create stale cold state.
- Mirror name collisions: if the default mirror name, such as `koldstore.messages__cl`, already exists or would collide with another managed table, enablement must fail with a clear recovery path unless a documented disambiguated name is chosen and recorded in metadata.
- Composite primary keys: mirror rows must preserve the same primary-key column names, PostgreSQL type identity, type modifiers, collations, domain identity where applicable, nullability implied by the primary key, and ordering used by the base table.
- Unsupported primary-key or flush column types: enablement or flush must fail before partial artifacts are created.
- Rollback after mirror update: rolled-back user transactions must not leave committed mirror changes.
- Populated-table backfill race: initial mirror rows must not overwrite newer INSERT, UPDATE, DELETE, or reinsert states captured after enablement began.
- Flush policy with no eligible mirror rows: no flush should run because there is no eligible mirror row set to persist.
- Flush policy race: mirror rows committed after the policy captures its eligible set or cutoff must remain hot/mirrored for a later flush.
- Flush policy duration semantics: `duration:S` or an `interval:S` compatibility spelling means rows whose mirror `changed_at` is older than that duration; it is not elapsed time since the last flush.
- `changes_since` after mirror cleanup: flushed cold records must carry enough mirror metadata for the feed to return latest-state changes newer than the caller cursor.
- `changes_since` after repeated updates to one key: the clean-schema feed is latest-state, so it must not promise every intermediate event.
- Concurrent flush and DML: flush must operate on a stable eligible set and must not delete mirror rows for changes that committed after the flush cutoff.
- Delete-marker cold rows without full payload: logical readers must ignore non-key payload values for delete markers and use the delete metadata only to mask older rows.
- Existing cold rows plus new delete: a tombstone must survive long enough, in hot mirror state or cold delete-marker state, to prevent older cold rows from reappearing.
- Disabled table re-enable: re-enable must create fresh mirror metadata and must not reuse stale dropped artifacts.
- Legacy artifact cleanup: old default artifacts such as a global row-events table or user-table system columns must not remain required for clean-schema operation.
- No legacy migration path: because the extension is still in development, this feature does not need to migrate already-managed old-format tables from internal columns to mirror tables.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: pg-koldstore MUST provide a clean-schema default mode in which enabling a table does not add internal KoldStore columns to the user table.
- **FR-002**: In clean-schema default mode, pg-koldstore MUST NOT add `_seq`, `_deleted`, `_commit_seq`, `_user_id`, or any other internal KoldStore column to the user table.
- **FR-003**: For each enabled table, pg-koldstore MUST create one table-specific latest-state change-log mirror in the `koldstore` schema.
- **FR-004**: The default mirror name for `public.messages` MUST be `koldstore.messages__cl`; any deviation required for collision handling MUST be recorded in table metadata and reported to the operator.
- **FR-005**: The change-log mirror MUST contain the same primary-key column or columns as the original table, preserving names, order, PostgreSQL data types, type modifiers, collations, domain identity where applicable, and primary-key-required non-nullability.
- **FR-006**: The change-log mirror MUST contain `seq BIGINT NOT NULL`, `op SMALLINT NOT NULL`, `changed_at TIMESTAMPTZ NOT NULL DEFAULT now()`, and `commit_lsn PG_LSN NULL`.
- **FR-007**: The change-log mirror MUST use operation values `1 = INSERT`, `2 = UPDATE`, and `3 = DELETE`.
- **FR-008**: The change-log mirror MUST represent latest state only and MUST contain at most one row for each managed primary key.
- **FR-009**: Enablement MUST create the change-log mirror, required indexes, DML capture triggers or equivalent capture mechanisms, and metadata that links the user table to its mirror.
- **FR-010**: Enablement of a populated table MUST initialize mirror entries for existing base-table rows as live entries with assigned sequence values.
- **FR-011**: Enablement MUST be atomic from the operator's perspective: failed enablement must leave the user table schema unchanged and must not leave active partial KoldStore artifacts.
- **FR-012**: Populated-table enablement MUST establish change capture before or atomically with mirror initialization so newly committed user changes cannot be missed.
- **FR-013**: Populated-table enablement MUST keep pre-existing rows in the user table by default and MUST NOT flush them to cold storage or delete them from hot storage as part of registration.
- **FR-014**: Populated-table mirror initialization MUST NOT overwrite a newer committed mirror state for the same primary key.
- **FR-015**: Flush MUST be blocked or skipped for a table or scope until populated-table mirror initialization has reached a complete and safe cutoff.
- **FR-016**: After populated-table mirror initialization completes, normal flush policy MAY flush eligible rows from oldest to newest using mirror sequence state.
- **FR-017**: INSERT on the user table MUST upsert the primary key into the mirror with `op = 1`, a new sequence value, and the current change timestamp.
- **FR-018**: UPDATE on the user table MUST upsert the primary key into the mirror with `op = 2`, a new sequence value, and the current change timestamp.
- **FR-019**: DELETE on the user table MUST keep a tombstone in the mirror with `op = 3`, a new sequence value, and the current change timestamp.
- **FR-020**: Reinsert of a deleted primary key MUST update the existing mirror tombstone back to `op = 1` with a new sequence value.
- **FR-021**: Mirror updates MUST commit or roll back with the user table mutation that caused them.
- **FR-022**: User-scoped clean-schema tables MUST use an existing application-owned scope column; missing scope columns MUST be rejected by default instead of adding `_user_id`.
- **FR-023**: Flush MUST select eligible changes using mirror sequence state rather than internal columns on the user table.
- **FR-024**: Flush MUST write cold records that include all base-table columns plus KoldStore sequence, operation, timestamp, and delete metadata needed for hot/cold winner resolution.
- **FR-025**: Flush MUST write delete-marker cold records for eligible mirror tombstones so deleted rows do not reappear after hot and mirror cleanup.
- **FR-026**: The mirror MUST NOT require `row_data` or `cold_segment_id` columns.
- **FR-027**: For delete-marker cold records, pg-koldstore MUST not require the mirror to retain full non-key row payload; cold readers must treat delete-marker payload values as non-authoritative.
- **FR-028**: Flush cleanup MUST remove matching mirror rows only after the corresponding cold artifact is committed and the mirror rows are no longer needed for hot replay.
- **FR-029**: Flush cleanup MUST NOT remove mirror rows for changes newer than the flush cutoff.
- **FR-030**: The default delete-marker flush policy SHOULD persist delete markers when they are needed to mask older cold rows; operators MAY opt into persisting all delete markers when retention or audit requirements matter more than cold-storage size.
- **FR-031**: Query resolution MUST use the newest sequence-bearing state across hot rows and cold records, and MUST hide rows whose newest state is a delete marker.
- **FR-032**: The clean-schema default MUST retire the old global append-only row-event table as a required catalog object.
- **FR-033**: The extension catalog for clean-schema default MUST NOT create `koldstore.row_events` as a required table.
- **FR-034**: Change-feed or replay behavior that remains in scope MUST be served from the table-specific mirror and cold records, not from a global row-events table.
- **FR-035**: Code used only by the old system-column or row-events design MUST be removed; no old-to-new migration path is required for already-managed development tables.
- **FR-036**: Tests that assert old default behavior for user-table system columns, global row events, hot tombstone columns, or direct writes to internal user-table columns MUST be removed or rewritten for the clean-schema contract.
- **FR-037**: Demigration MUST drop table-specific capture triggers or equivalent capture mechanisms, drop the table-specific mirror, and remove or deactivate table metadata.
- **FR-038**: Default demigration MUST preserve the current logical table contents in the original user table before detaching KoldStore management.
- **FR-039**: Demigration MUST leave the user table schema clean; it must not preserve KoldStore internals as ordinary business columns because clean-schema mode never added them.
- **FR-040**: Tests MUST prove the user table schema is unchanged after enablement.
- **FR-041**: Tests MUST prove the change-log mirror has the same primary-key columns, column order, PostgreSQL data types, type modifiers, collations, domain identity where applicable, and primary-key-required non-nullability as the original table.
- **FR-042**: Tests MUST prove populated-table enablement initializes mirror rows without flushing or deleting base rows during registration.
- **FR-043**: Tests MUST prove populated-table mirror initialization does not overwrite newer committed DML state.
- **FR-044**: Tests MUST prove INSERT, UPDATE, DELETE, and reinsert upsert correctly into the mirror.
- **FR-045**: Tests MUST prove DELETE keeps a tombstone mirror row.
- **FR-046**: Tests MUST prove reinsert changes a tombstone back to insert state.
- **FR-047**: Tests MUST prove flush removes mirror rows only after safe cold persistence and preserves delete markers in cold storage.
- **FR-048**: Tests MUST prove the clean-schema default does not create or require the old global row-events table.
- **FR-049**: Tests MUST prove demigration removes table-specific KoldStore artifacts safely.
- **FR-050**: README limitations MUST state that altering primary-key values or primary-key definitions on managed tables is not implemented yet.
- **FR-051**: Flush policy evaluation MUST read the table/scope change-log mirror, not user-table internal columns.
- **FR-052**: The default `rows:N` policy MUST treat `N` as the hot-row limit and select the oldest pending latest-state mirror rows by `seq` when the table/scope exceeds that limit.
- **FR-053**: A duration policy such as `duration:1d` MUST select mirror rows whose `changed_at` is older than the configured duration. If an `interval:S` spelling is retained for compatibility, it MUST mean row age in seconds, not time since the last flush.
- **FR-054**: When policy evaluation selects rows, pg-koldstore MUST capture a stable eligible set or mirror `seq` cutoff and flush only rows selected by the policy at evaluation time.
- **FR-055**: Mirror rows not selected by the policy, including rows newer than a duration threshold or rows above a row-limit cutoff, MUST remain in the mirror and base table for a later flush.
- **FR-056**: `koldstore.changes_since(table_name regclass, since_commit_seq bigint, limit_rows integer DEFAULT 1000)` MUST read from the table-specific change-log mirror and flushed cold records that contain mirror metadata, not from `koldstore.row_events`.
- **FR-057**: In clean-schema mode, the `since_commit_seq` argument MUST be treated as the caller's last-seen mirror `seq` cursor unless the public signature is renamed before release.
- **FR-058**: `changes_since` MUST return latest-state changes, not every intermediate event, and MUST document this behavior.
- **FR-059**: Tests MUST prove `rows:1000` uses the mirror sequence order as the default hot-row limit policy and that duration policies flush rows older than the configured row age without flushing newer rows.
- **FR-060**: Tests MUST prove `changes_since` works without `koldstore.row_events`, using mirror rows before flush and flushed cold mirror metadata after flush.

### Key Entities *(include if feature involves data)*

- **User Table**: The application-owned table being managed by pg-koldstore. Its business schema, primary key, and application-facing columns remain unchanged in clean-schema mode.
- **Change-Log Mirror**: A per-table latest-state table in the `koldstore` schema containing the managed table primary key plus sequence, operation, timestamp, and optional commit LSN.
- **Mirror Entry**: The single latest-state record for a primary key. It records whether the latest hot change is insert, update, or delete.
- **Mirror Initialization**: The populated-table enablement phase that records existing hot rows in the mirror as live latest-state entries without flushing or deleting them during registration.
- **Cold Record**: A persisted cold-storage record assembled from base-table columns plus KoldStore change metadata.
- **Delete Marker**: A cold or mirror record whose latest operation is delete and whose purpose is to hide older cold live rows for the same primary key.
- **Managed Table Metadata**: Internal catalog state that links a user table to its mirror, primary-key shape, scope configuration, flush policy, storage binding, and active schema version.
- **Flush Cutoff**: The stable sequence boundary that determines which mirror changes are eligible for a flush batch.
- **Flush Policy State**: Per table/scope policy state that tracks configured row limit, optional duration threshold, selected mirror rows, and pending mirror rows for policy evaluation.
- **Latest-State Change Feed**: `changes_since` output derived from unflushed mirror rows and flushed cold records, ordered by mirror sequence and limited to latest available state per primary key.
- **Demigration Operation**: The process that restores or confirms the current logical user-table contents and removes table-specific KoldStore management artifacts.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In migration tests covering single-column and composite primary keys, 100% of enabled user tables retain the exact same application-visible column list before and after enablement.
- **SC-002**: In mirror contract tests, 100% of mirrors contain primary-key columns with the same names, order, PostgreSQL data types, type modifiers, collations, domain identity where applicable, and primary-key-required non-nullability as their source tables.
- **SC-003**: In DML tests, every INSERT, UPDATE, DELETE, and reinsert sequence for a primary key leaves exactly one mirror row with the expected operation value and a strictly newer sequence value after each committed mutation.
- **SC-004**: In populated-table migration tests, 100% of pre-existing rows receive mirror entries without being flushed or deleted during registration.
- **SC-005**: In populated-table race tests, no newer committed DML state is overwritten by the initial mirror backfill.
- **SC-006**: In delete tests, 100% of committed deletes keep a mirror tombstone until the tombstone is safely flushed or otherwise no longer needed for hot replay.
- **SC-007**: In flush tests, no committed delete marker that is needed to mask older cold data is lost during hot and mirror cleanup.
- **SC-008**: In extension catalog tests, the clean-schema default creates 0 required global row-event tables and relies on the per-table mirror instead.
- **SC-009**: In failure-injection tests, interrupted enablement, flush, or demigration leaves either the previous valid state or a retryable managed state, with no partial user-table schema pollution.
- **SC-010**: In demigration tests, 100% of table-specific mirrors, triggers or equivalent capture mechanisms, and active metadata entries are removed after successful demigration.
- **SC-011**: In lifecycle tests, logical query results before flush, after flush, after delete, after reinsert, and after demigration match the expected latest-state table contents for all tested primary keys.
- **SC-012**: In flush policy tests, `rows:1000` keeps at most the configured hot-row limit by flushing oldest mirror rows, while duration policies flush only mirror rows older than the configured row age.
- **SC-013**: In change-feed tests, `changes_since` returns latest-state changes from mirror/cold metadata without requiring `koldstore.row_events`.

## Assumptions

- Clean-schema mode replaces the old development-time system-column design. No migration path from old managed-table syntax or old internal-column state is required for this feature.
- Sequence values are Snowflake-style effect identifiers suitable for ordering latest table state, while `commit_lsn` is optional and may be unavailable for some captured changes.
- Existing populated tables are eligible for clean-schema migration. The default path initializes the mirror from existing hot rows first, keeps those rows hot during registration, blocks flush until the mirror has a safe complete cutoff, and lets normal flush policy move eligible rows afterward.
- The change-log mirror is an internal KoldStore artifact, not a user-facing full history feed.
- The old global row-events table is no longer part of the clean-schema default. Any future full-history feed should be specified separately instead of keeping unused legacy catalog and test paths.
- Delete-marker cold records need the primary key and KoldStore metadata for correctness; non-key business values are not authoritative for delete markers when the source row has already been deleted.
- The default delete-marker flush policy optimizes for correctness with bounded cold-storage growth by flushing delete markers when they are needed to mask older cold rows; broader retention can be configured separately.
- `changes_since` is a latest-state delta feed under clean-schema mode. Consumers that require every intermediate event need a separate future full-history feature.
- The local development and verification loop should remain pgrx-managed PostgreSQL tests, with Docker reserved for packaging or runtime smoke checks.
