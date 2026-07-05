# Tasks: Clean Schema Change-Log Mirrors

**Input**: Design documents from `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/`

**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `contracts/`, `quickstart.md`

**Tests**: Required by the feature specification. Write story tests first, confirm they fail against the current system-column/row-events implementation, then implement.

**Organization**: Tasks are grouped by user story so each story can be implemented and tested independently after the shared foundation is complete.

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Prepare focused modules and helpers for the clean-schema refactor without changing behavior yet.

- [ ] T001 Add `mirror` and `policy` module declarations for planned clean-schema code in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/mod.rs` and `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/flush/mod.rs`
- [ ] T002 [P] Add reusable clean-schema catalog assertion helpers in `/Users/jamal/git/pg-kalam/tests/e2e/common/catalog.rs`
- [ ] T003 [P] Add reusable clean-schema SQL assertion helpers in `/Users/jamal/git/pg-kalam/tests/e2e/common/assertions.rs`
- [ ] T004 [P] Add a regression test fixture helper for single-column, composite, and user-scoped tables in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/clean_schema_helpers.rs`
- [ ] T005 Document the local pgrx-only verification commands for this feature in `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/quickstart.md`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Shared domain objects, catalog metadata, SQL catalog shape, and policy parsing that all user stories depend on.

**Critical**: No user story implementation should start until these tasks are complete.

- [ ] T006 [P] Add unit tests for mirror operation values, latest-state transitions, and delete tombstone state in `/Users/jamal/git/pg-kalam/crates/koldstore-core/tests/core_models.rs`
- [ ] T007 [P] Add unit tests for exact primary-key shape capture including typmod, collation, domain identity, ordering, and non-nullability in `/Users/jamal/git/pg-kalam/crates/koldstore-core/tests/core_models.rs`
- [ ] T008 [P] Add unit tests for `rows:N`, `duration:S`, and optional `interval:S` duration-alias parsing in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/flush_policy.rs`
- [ ] T009 Implement type-safe mirror domain objects and operation enum in `/Users/jamal/git/pg-kalam/crates/koldstore-core/src/row.rs`
- [ ] T010 Implement enriched primary-key shape/domain helpers in `/Users/jamal/git/pg-kalam/crates/koldstore-core/src/pk.rs`
- [ ] T011 Implement clean-schema table metadata fields for mirror relation, initialization state, and primary-key shape in `/Users/jamal/git/pg-kalam/crates/koldstore-catalog/src/table_meta.rs`
- [ ] T012 Update schema registry to store application schema separately from mirror/cold metadata in `/Users/jamal/git/pg-kalam/crates/koldstore-catalog/src/schema_registry.rs`
- [ ] T013 Replace required `koldstore.row_events` catalog DDL with clean-schema metadata support in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/sql/koldstore--0.1.0.sql`
- [ ] T014 Implement `rows:N`, `duration:S`, and optional `interval:S` duration-alias parsing in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/flush/policy.rs`
- [ ] T015 Wire new mirror and policy modules into public crate exports in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/lib.rs`
- [ ] T016 Run foundational unit tests with `cargo test -p koldstore-core -p koldstore-catalog` and record any failures in `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/tasks.md`

**Checkpoint**: Foundation ready; story implementation can proceed.

---

## Phase 3: User Story 1 - Enable KoldStore Without Polluting Business Schema (Priority: P1) - MVP

**Goal**: Enable a table while preserving the user table schema and creating an exact-PK per-table mirror.

**Independent Test**: Capture a source table definition before enablement, run `migrate_table`, and verify the application-visible schema, primary key, constraints, and indexes are unchanged while `koldstore.<table>__cl` exists with matching primary-key shape.

### Tests for User Story 1

- [ ] T017 [P] [US1] Add SQL regression tests proving enablement adds no `_seq`, `_deleted`, `_commit_seq`, `_user_id`, or other KoldStore column to the user table in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/clean_schema_enable.rs`
- [ ] T018 [P] [US1] Add SQL regression tests proving single-column, composite, typmod, collation, and domain primary keys are preserved in mirror tables in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/change_log_mirror_shape.rs`
- [ ] T019 [P] [US1] Add e2e greenfield clean-schema assertions in `/Users/jamal/git/pg-kalam/tests/e2e/migrate/greenfield_matrix.rs`

### Implementation for User Story 1

- [ ] T020 [US1] Implement primary-key metadata extraction from PostgreSQL catalogs in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/register.rs`
- [ ] T021 [US1] Implement mirror table naming, collision checks, and exact-PK `CREATE TABLE` planning in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/mirror.rs`
- [ ] T022 [US1] Remove default system-column addition from greenfield enablement in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/ddl.rs`
- [ ] T023 [US1] Register mirror relation identity and primary-key shape in managed metadata during enablement in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/register.rs`
- [ ] T024 [US1] Reject tables without primary keys before creating mirror artifacts in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/constraints.rs`
- [ ] T025 [US1] Make failed enablement drop inactive mirror artifacts and leave the user schema unchanged in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/rollback.rs`
- [ ] T026 [US1] Run `cargo pgrx test --manifest-path crates/pg_koldstore/Cargo.toml clean_schema_enable change_log_mirror_shape` and record results in `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/tasks.md`

**Checkpoint**: US1 is independently testable and is the MVP.

---

## Phase 4: User Story 2 - Track Latest Hot State in a Per-Table Mirror (Priority: P1)

**Goal**: Capture INSERT, UPDATE, DELETE, and reinsert into one latest-state mirror row per primary key.

**Independent Test**: Enable a table, run INSERT/UPDATE/DELETE/reinsert for the same primary key, and verify the mirror has exactly one row with the expected `op`, `seq`, `changed_at`, and optional `commit_lsn`.

### Tests for User Story 2

- [ ] T027 [P] [US2] Add SQL regression tests for INSERT, UPDATE, DELETE, and reinsert mirror upserts in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/change_log_mirror_dml.rs`
- [ ] T028 [P] [US2] Add transaction rollback and same-primary-key concurrency tests for mirror state in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/change_log_mirror_dml.rs`
- [ ] T029 [P] [US2] Add e2e DML mirror-state assertions in `/Users/jamal/git/pg-kalam/tests/e2e/dml/change_log_mirror.rs`

### Implementation for User Story 2

- [ ] T030 [US2] Implement DML capture SQL generation for mirror upserts in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/dml.rs`
- [ ] T031 [US2] Install INSERT/UPDATE/DELETE capture triggers or equivalent hooks during enablement in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/mirror.rs`
- [ ] T032 [US2] Allocate Snowflake `seq` and mirror `changed_at` transactionally with user DML in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/hooks/executor.rs`
- [ ] T033 [US2] Implement tombstone-to-insert reinsert behavior with `ON CONFLICT` mirror upsert semantics in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/dml.rs`
- [ ] T034 [US2] Ensure mirror mutations roll back with the user transaction in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/hooks/xact.rs`
- [ ] T035 [US2] Run `cargo pgrx test --manifest-path crates/pg_koldstore/Cargo.toml change_log_mirror_dml` and record results in `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/tasks.md`

**Checkpoint**: US2 works independently after US1.

---

## Phase 5: User Story 3 - Enable Populated Tables Safely (Priority: P1)

**Goal**: Initialize mirrors for existing rows without flushing or deleting base rows during registration.

**Independent Test**: Enable KoldStore on a populated table, verify every pre-existing PK has `op = 1` in the mirror, and verify concurrent or follow-up DML is not overwritten by initialization.

### Tests for User Story 3

- [ ] T036 [P] [US3] Add SQL regression tests for populated-table mirror initialization without base-row deletion in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/migrate_existing_clean_schema.rs`
- [ ] T037 [P] [US3] Add SQL regression tests proving mirror initialization does not overwrite newer committed DML state in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/migrate_existing_clean_schema.rs`
- [ ] T038 [P] [US3] Add e2e populated-table migration coverage in `/Users/jamal/git/pg-kalam/tests/e2e/migrate/migrate_existing_matrix.rs`

### Implementation for User Story 3

- [ ] T039 [US3] Replace `_seq` backfill planning with mirror initialization planning in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/jobs.rs`
- [ ] T040 [US3] Implement existing-row mirror insertion with newer-state conflict protection in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/backfill.rs`
- [ ] T041 [US3] Track mirror initialization state and safe cutoff in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/register.rs`
- [ ] T042 [US3] Block or skip flush for incomplete mirror initialization in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/flush/job.rs`
- [ ] T043 [US3] Remove registration-time initial flush enqueueing that depends on user-table `_seq` in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/jobs.rs`
- [ ] T044 [US3] Run `cargo pgrx test --manifest-path crates/pg_koldstore/Cargo.toml migrate_existing_clean_schema` and record results in `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/tasks.md`

**Checkpoint**: US3 works independently after US1 and US2.

---

## Phase 6: User Story 4 - Flush Clean-Schema State to Cold Storage (Priority: P1)

**Goal**: Flush eligible mirror state and base rows to cold storage, preserve delete markers, and clean hot/mirror rows only after cold commit.

**Independent Test**: Insert, update, delete, and reinsert rows; run row-limit and duration-policy flushes; verify cold artifacts include base columns plus mirror metadata, delete markers mask older cold rows, and cleanup excludes unselected mirror rows.

### Tests for User Story 4

- [ ] T045 [P] [US4] Add unit tests for row-limit and duration policy selection in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/flush_policy.rs`
- [ ] T046 [P] [US4] Add unit tests for cold live records and delete-marker schema in `/Users/jamal/git/pg-kalam/crates/koldstore-parquet/tests/schema.rs`
- [ ] T047 [P] [US4] Add SQL regression tests for mirror-backed flush selection and cleanup in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/flush_clean_schema.rs`
- [ ] T048 [P] [US4] Add e2e row-limit and duration-policy flush tests in `/Users/jamal/git/pg-kalam/tests/e2e/flush/flush_policy.rs`
- [ ] T049 [P] [US4] Add e2e delete-marker and reinsert merge tests in `/Users/jamal/git/pg-kalam/tests/e2e/merge/merge_scan_results.rs`

### Implementation for User Story 4

- [ ] T050 [US4] Replace `flush_stats` user-table `_seq` scans with mirror-backed candidate selection in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/ops.rs`
- [ ] T051 [US4] Implement row-limit policy selection of oldest pending mirror rows by `seq` in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/flush/policy.rs`
- [ ] T052 [US4] Implement duration policy selection using mirror `changed_at` row age in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/flush/policy.rs`
- [ ] T053 [US4] Update flush job claiming to persist selected mirror keys/sequence cutoff instead of user-table `_seq` bounds in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/flush/job.rs`
- [ ] T054 [US4] Update Parquet schema generation to write base columns plus `seq`, `op`, `changed_at`, and delete metadata in `/Users/jamal/git/pg-kalam/crates/koldstore-parquet/src/schema.rs`
- [ ] T055 [US4] Update Parquet writer to emit live records from base rows and PK-only delete-marker records from mirror tombstones in `/Users/jamal/git/pg-kalam/crates/koldstore-parquet/src/writer.rs`
- [ ] T056 [US4] Update Parquet reader to expose clean-schema cold metadata and delete markers in `/Users/jamal/git/pg-kalam/crates/koldstore-parquet/src/reader.rs`
- [ ] T057 [US4] Update flush cleanup to remove only selected base rows and mirror rows after manifest visibility commits in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/flush/cleanup.rs`
- [ ] T058 [US4] Update merge resolution so newest sequence-bearing hot/cold state wins without user-table `_deleted` in `/Users/jamal/git/pg-kalam/crates/koldstore-merge/src/resolver.rs`
- [ ] T059 [US4] Update merge tombstone handling so cold delete markers mask older live rows and newer reinserts win in `/Users/jamal/git/pg-kalam/crates/koldstore-merge/src/tombstone.rs`
- [ ] T060 [US4] Run `cargo test -p koldstore-parquet -p koldstore-merge` and record results in `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/tasks.md`
- [ ] T061 [US4] Run `cargo pgrx test --manifest-path crates/pg_koldstore/Cargo.toml flush_clean_schema flush_policy` and record results in `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/tasks.md`

**Checkpoint**: US4 works independently after US1-US3.

---

## Phase 7: User Story 5 - Disable KoldStore Safely (Priority: P1)

**Goal**: Demigrate a table while preserving current logical rows and removing table-specific clean-schema artifacts.

**Independent Test**: Enable, mutate, flush, demigrate, and verify the original table contains current logical rows with no KoldStore triggers, mirror, or active metadata.

### Tests for User Story 5

- [ ] T062 [P] [US5] Add SQL regression tests for demigration dropping mirror/capture artifacts and preserving a clean user table in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/demigrate_clean_schema.rs`
- [ ] T063 [P] [US5] Add e2e demigration lifecycle coverage in `/Users/jamal/git/pg-kalam/tests/e2e/migrate/demigrate_matrix.rs`

### Implementation for User Story 5

- [ ] T064 [US5] Update demigration planning to rehydrate current logical hot+cold state without dropping system columns in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/ddl.rs`
- [ ] T065 [US5] Drop table-specific capture triggers and mirror tables during demigration in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/rollback.rs`
- [ ] T066 [US5] Remove or deactivate mirror metadata during demigration in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/register.rs`
- [ ] T067 [US5] Ensure failed demigration leaves clean-schema managed state retryable in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/rollback.rs`
- [ ] T068 [US5] Run `cargo pgrx test --manifest-path crates/pg_koldstore/Cargo.toml demigrate_clean_schema` and record results in `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/tasks.md`

**Checkpoint**: US5 works independently after US1-US4.

---

## Phase 8: User Story 6 - Retire Legacy Internal State (Priority: P1)

**Goal**: Remove required global row-events and user-table system-column code paths from the clean-schema default.

**Independent Test**: Install the extension, enable a table, and verify no required `koldstore.row_events` table is created and no tests assert clean-schema user-table system columns.

### Tests for User Story 6

- [ ] T069 [P] [US6] Rewrite row-events catalog tests to assert `koldstore.row_events` is not required by default in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/row_events.rs`
- [ ] T070 [P] [US6] Rewrite system-column guard tests to assert clean-schema migration does not require user-table internals in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/system_column_guards.rs`
- [ ] T071 [P] [US6] Update lifecycle catalog tests to remove old row-events and duplicate-index expectations in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/lifecycle.rs`

### Implementation for User Story 6

- [ ] T072 [US6] Remove row-events catalog module exports or narrow them to non-default legacy-free behavior in `/Users/jamal/git/pg-kalam/crates/koldstore-catalog/src/lib.rs`
- [ ] T073 [US6] Remove row-events-only merge helpers or replace them with mirror/cold latest-state helpers in `/Users/jamal/git/pg-kalam/crates/koldstore-merge/src/changelog.rs`
- [ ] T074 [US6] Remove system-column planning helpers from default migration path in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/columns.rs`
- [ ] T075 [US6] Remove old system-column SQL DDL generation from default enablement in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/ddl.rs`
- [ ] T076 [US6] Run `cargo test -p koldstore-catalog -p koldstore-merge` and record results in `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/tasks.md`

**Checkpoint**: US6 works independently after replacement clean-schema paths exist.

---

## Phase 9: User Story 7 - Read Latest-State Changes From Mirrors (Priority: P1)

**Goal**: Serve `koldstore.changes_since` from the table-specific mirror and flushed cold metadata instead of global row events.

**Independent Test**: Insert, update, delete, flush, and reinsert rows; call `changes_since` with cursors before and after flush; verify latest-state changes are returned without `koldstore.row_events`.

### Tests for User Story 7

- [ ] T077 [P] [US7] Add unit tests for latest-state change-feed ordering, cursor handling, and retention gaps in `/Users/jamal/git/pg-kalam/crates/koldstore-merge/tests/changelog.rs`
- [ ] T078 [P] [US7] Add SQL regression tests for mirror-backed `changes_since` before and after flush in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/changes_since.rs`
- [ ] T079 [P] [US7] Add e2e change-feed coverage without `koldstore.row_events` in `/Users/jamal/git/pg-kalam/tests/e2e/dml/change_feed.rs`

### Implementation for User Story 7

- [ ] T080 [US7] Implement latest-state change-feed merge logic over mirror and cold metadata in `/Users/jamal/git/pg-kalam/crates/koldstore-merge/src/changelog.rs`
- [ ] T081 [US7] Update SQL `changes_since` implementation to query the table-specific mirror for unflushed rows in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/events.rs`
- [ ] T082 [US7] Update SQL `changes_since` implementation to query flushed cold metadata after mirror cleanup in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/events.rs`
- [ ] T083 [US7] Update `koldstore.change_event` SQL type and function docs to treat `since_commit_seq` as mirror `seq` cursor unless renamed in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/sql/koldstore--0.1.0.sql`
- [ ] T084 [US7] Run `cargo pgrx test --manifest-path crates/pg_koldstore/Cargo.toml changes_since` and record results in `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/tasks.md`

**Checkpoint**: US7 works independently after US1, US2, US4, and US6.

---

## Phase 10: User Story 8 - Preserve User-Scoped Clean Schema (Priority: P2)

**Goal**: Support user-scoped clean-schema tables only with existing application-owned scope columns.

**Independent Test**: Enable a user-scoped table with an existing scope column and verify no `_user_id` column is added; attempt user-scope enablement without a valid scope column and verify it fails cleanly.

### Tests for User Story 8

- [ ] T085 [P] [US8] Add SQL regression tests for user-scoped clean-schema enablement with an existing scope column in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/greenfield_user.rs`
- [ ] T086 [P] [US8] Add SQL regression tests rejecting user-scoped enablement without an application-owned scope column in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/user_scope_fail_closed.rs`
- [ ] T087 [P] [US8] Add e2e user-scope clean-schema coverage in `/Users/jamal/git/pg-kalam/tests/e2e/scope/user_scope_matrix.rs`

### Implementation for User Story 8

- [ ] T088 [US8] Enforce existing application-owned scope column validation in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/scope.rs`
- [ ] T089 [US8] Remove default `_user_id` column creation from user-scoped enablement in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/ddl.rs`
- [ ] T090 [US8] Apply scope predicates to mirror DML capture, flush selection, and `changes_since` queries in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/security/scope.rs`
- [ ] T091 [US8] Run `cargo pgrx test --manifest-path crates/pg_koldstore/Cargo.toml greenfield_user user_scope_fail_closed` and record results in `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/tasks.md`

**Checkpoint**: US8 works independently after US1, US2, and the shared scope foundation.

---

## Final Phase: Polish & Cross-Cutting Concerns

**Purpose**: Documentation, cleanup, validation, and full local verification after selected stories are complete.

- [ ] T092 [P] Update README clean-schema migration, flush policy, change-feed, and primary-key limitation docs in `/Users/jamal/git/pg-kalam/README.md`
- [ ] T093 [P] Update any SQL API comments or generated extension docs for clean-schema defaults in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/sql/koldstore--0.1.0.sql`
- [ ] T094 Remove or rename obsolete system-column and row-event test files that no longer map to clean-schema behavior in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/system_columns.rs`
- [ ] T095 Run fast workspace tests with `cargo test -p koldstore-core -p koldstore-catalog -p koldstore-merge -p koldstore-parquet` and record results in `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/tasks.md`
- [ ] T096 Run extension regression tests with `cargo pgrx test --manifest-path crates/pg_koldstore/Cargo.toml` and record results in `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/tasks.md`
- [ ] T097 Run local e2e lifecycle tests with `cargo test -p e2e --test greenfield_matrix --test migrate_existing_matrix --test flush_matrix --test change_feed --test demigrate_matrix --test full_lifecycle` and record results in `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/tasks.md`
- [ ] T098 Run `rg '_seq|_commit_seq|_deleted|_user_id|row_events' /Users/jamal/git/pg-kalam/crates /Users/jamal/git/pg-kalam/tests/e2e` and confirm remaining matches are either explicit rejection tests or non-default documentation in `/Users/jamal/git/pg-kalam/specs/002-clean-schema-change-log/tasks.md`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 Setup**: No dependencies.
- **Phase 2 Foundational**: Depends on Phase 1 and blocks all user stories.
- **US1**: Depends on Phase 2 and is the MVP.
- **US2**: Depends on US1 because DML capture needs mirror creation and metadata.
- **US3**: Depends on US1 and US2 because populated-table initialization must coexist with active DML capture.
- **US4**: Depends on US1-US3 because flush needs mirrors, capture, and initialized state.
- **US5**: Depends on US1-US4 because demigration must preserve hot+cold logical state.
- **US6**: Can proceed after US1-US4 replacement paths exist; final removal should happen after clean-schema tests cover the replacement behavior.
- **US7**: Depends on US1, US2, US4, and US6 because change feed must read mirror/cold metadata without row events.
- **US8**: Depends on US1, US2, and scope validation; it can run in parallel with US3-US7 after shared foundations.
- **Final Phase**: Depends on all selected stories.

### User Story Dependencies

- **MVP**: US1 only.
- **Core clean-schema lifecycle**: US1 -> US2 -> US3 -> US4 -> US5.
- **Legacy retirement**: US6 after clean-schema replacements are in place.
- **Change feed**: US7 after mirror/cold flush and legacy retirement.
- **User scope**: US8 after US1-US2; can be implemented before US5-US7 if needed.

### Parallel Opportunities

- Setup helpers T002-T004 can run in parallel.
- Foundational tests T006-T008 can run in parallel.
- US1 tests T017-T019 can run in parallel.
- US2 tests T027-T029 can run in parallel.
- US3 tests T036-T038 can run in parallel.
- US4 tests T045-T049 can run in parallel.
- US5 tests T062-T063 can run in parallel.
- US6 tests T069-T071 can run in parallel.
- US7 tests T077-T079 can run in parallel.
- US8 tests T085-T087 can run in parallel.
- Documentation tasks T092-T093 can run in parallel after implementation stabilizes.

---

## Parallel Example: User Story 1

```bash
Task: "T017 [P] [US1] Add SQL regression tests proving enablement adds no KoldStore columns in /Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/clean_schema_enable.rs"
Task: "T018 [P] [US1] Add SQL regression tests proving mirror PK preservation in /Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/change_log_mirror_shape.rs"
Task: "T019 [P] [US1] Add e2e greenfield clean-schema assertions in /Users/jamal/git/pg-kalam/tests/e2e/migrate/greenfield_matrix.rs"
```

## Parallel Example: User Story 4

```bash
Task: "T045 [P] [US4] Add unit tests for row-limit and duration policy selection in /Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/flush_policy.rs"
Task: "T046 [P] [US4] Add unit tests for cold live records and delete-marker schema in /Users/jamal/git/pg-kalam/crates/koldstore-parquet/tests/schema.rs"
Task: "T048 [P] [US4] Add e2e row-limit and duration-policy flush tests in /Users/jamal/git/pg-kalam/tests/e2e/flush/flush_policy.rs"
```

## Parallel Example: User Story 7

```bash
Task: "T077 [P] [US7] Add unit tests for latest-state change-feed ordering in /Users/jamal/git/pg-kalam/crates/koldstore-merge/tests/changelog.rs"
Task: "T078 [P] [US7] Add SQL regression tests for mirror-backed changes_since in /Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/changes_since.rs"
Task: "T079 [P] [US7] Add e2e change-feed coverage in /Users/jamal/git/pg-kalam/tests/e2e/dml/change_feed.rs"
```

---

## Implementation Strategy

### MVP First

1. Complete Phase 1 and Phase 2.
2. Complete US1.
3. Validate US1 independently with `cargo pgrx test --manifest-path crates/pg_koldstore/Cargo.toml clean_schema_enable change_log_mirror_shape`.
4. Stop and review before moving DML, flush, and legacy-removal work.

### Incremental Delivery

1. US1 delivers clean enablement and mirror shape.
2. US2 makes the mirror authoritative for latest hot DML state.
3. US3 makes existing-table adoption safe.
4. US4 makes flush and merge correct without user-table internals.
5. US5 completes the clean exit path.
6. US6 removes legacy internal state after replacement coverage exists.
7. US7 restores the public change-feed surface on mirror/cold metadata.
8. US8 adds user-scoped clean-schema coverage.

### Verification Loop

Use local pgrx-managed PostgreSQL and Rust tests only for default correctness:

```bash
cargo test -p koldstore-core -p koldstore-catalog -p koldstore-merge -p koldstore-parquet
cargo pgrx test --manifest-path crates/pg_koldstore/Cargo.toml
cargo test -p e2e --test greenfield_matrix --test migrate_existing_matrix --test flush_matrix --test change_feed --test demigrate_matrix --test full_lifecycle
```
