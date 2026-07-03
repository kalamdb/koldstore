# Tasks: pg-koldstore Hot/Cold Storage Extension

**Input**: Design documents from `/Users/jamal/git/pg-kalam/specs/001-pg-kalam-hot-cold-storage/`

**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `contracts/`, `quickstart.md`

**Tests**: Required. The feature spec, contracts, and user request explicitly require unit, SQL regression, integration, E2E, performance, benchmark, memory-allocation, leak, edge-case, and PostgreSQL-version coverage.

**Agent context**: This repository is currently spec-only. Implementation agents must scaffold the Rust/pgrx workspace first, then implement the crates and tests listed below. Use `../kalamdb` only as a reference source; do not copy APIs that conflict with the pg-koldstore plan. Useful references include `../kalamdb/docs/architecture/manifest.md`, `../kalamdb/docs/architecture/transactions.md`, `../kalamdb/backend/crates/kalamdb-filestore/src/parquet/reader.rs`, `../kalamdb/backend/crates/kalamdb-tables/src/manifest/planner.rs`, `../kalamdb/docs/development/MEMORY_ANALYSIS.md`, and `../kalamdb/benchv2/README.md`.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing. Within each story, write tests first and verify they fail before implementation.

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create the Rust/pgrx workspace, PostgreSQL extension skeleton, test harnesses, and tooling needed by all stories.

- [X] T001 Create the root Rust workspace with pgrx, Arrow, Parquet, object_store, serde, tracing, criterion, tokio, and test dependencies in `/Users/jamal/git/pg-kalam/Cargo.toml`
- [X] T002 Create extension metadata and base SQL migration files in `/Users/jamal/git/pg-kalam/koldstore.control` and `/Users/jamal/git/pg-kalam/sql/koldstore--0.1.0.sql`
- [X] T003 [P] Create the pure shared-types crate skeleton in `/Users/jamal/git/pg-kalam/crates/koldstore-core/Cargo.toml` and `/Users/jamal/git/pg-kalam/crates/koldstore-core/src/lib.rs`
- [X] T004 [P] Create the manifest crate skeleton in `/Users/jamal/git/pg-kalam/crates/koldstore-manifest/Cargo.toml` and `/Users/jamal/git/pg-kalam/crates/koldstore-manifest/src/lib.rs`
- [X] T005 [P] Create the object-store crate skeleton in `/Users/jamal/git/pg-kalam/crates/koldstore-storage/Cargo.toml` and `/Users/jamal/git/pg-kalam/crates/koldstore-storage/src/lib.rs`
- [X] T006 [P] Create the Parquet crate skeleton in `/Users/jamal/git/pg-kalam/crates/koldstore-parquet/Cargo.toml` and `/Users/jamal/git/pg-kalam/crates/koldstore-parquet/src/lib.rs`
- [X] T007 [P] Create the merge/resolution crate skeleton in `/Users/jamal/git/pg-kalam/crates/koldstore-merge/Cargo.toml` and `/Users/jamal/git/pg-kalam/crates/koldstore-merge/src/lib.rs`
- [X] T008 [P] Create the catalog validation crate skeleton in `/Users/jamal/git/pg-kalam/crates/koldstore-catalog/Cargo.toml` and `/Users/jamal/git/pg-kalam/crates/koldstore-catalog/src/lib.rs`
- [X] T009 Create the pgrx extension crate skeleton in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/Cargo.toml` and `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/lib.rs`
- [X] T010 Create the PostgreSQL Custom Scan C shim skeleton in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/native/custom_scan.c` and `/Users/jamal/git/pg-kalam/crates/pg_koldstore/native/custom_scan.h`
- [X] T011 Create the local integration environment for PostgreSQL 15, 16, 17, and MinIO in `/Users/jamal/git/pg-kalam/tests/docker-compose.yml`
- [X] T012 Create the PostgreSQL version matrix E2E runner in `/Users/jamal/git/pg-kalam/tests/e2e/run_pg_matrix.sh`
- [X] T013 Create the Rust E2E helper crate/module for pgrx, tokio-postgres, and MinIO bootstrapping in `/Users/jamal/git/pg-kalam/tests/e2e/common/mod.rs`
- [X] T014 Create the benchmarking harness crate and baseline report schema in `/Users/jamal/git/pg-kalam/benchmarks/Cargo.toml` and `/Users/jamal/git/pg-kalam/benchmarks/src/report.rs`
- [X] T015 Create memory and leak testing scripts for ASAN, LSAN, Valgrind, heaptrack, and PostgreSQL memory-context smoke checks in `/Users/jamal/git/pg-kalam/tests/memory/run_memory_checks.sh`
- [X] T016 Create CI workflow gates for Rust tests, pgrx SQL tests, PG matrix E2E, benchmarks, and memory checks in `/Users/jamal/git/pg-kalam/.github/workflows/pg-koldstore-ci.yml`
- [X] T017 Create developer command aliases for formatting, linting, unit tests, pgrx tests, E2E, benchmarks, and memory checks in `/Users/jamal/git/pg-kalam/Makefile`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core types, catalog schema, extension lifecycle, transaction stamping, memory safety boundaries, and shared test infrastructure that all user stories depend on.

**Critical**: No user story work can begin until this phase is complete.

- [X] T018 [P] Implement shared error, result, and diagnostic types in `/Users/jamal/git/pg-kalam/crates/koldstore-core/src/error.rs`
- [X] T019 [P] Implement `SeqId`, `CommitSeq`, `TableKind`, and scope-key types in `/Users/jamal/git/pg-kalam/crates/koldstore-core/src/seq.rs` and `/Users/jamal/git/pg-kalam/crates/koldstore-core/src/table_kind.rs`
- [X] T020 [P] Implement logical primary-key extraction, stable PK hashing, and JSON PK encoding helpers in `/Users/jamal/git/pg-kalam/crates/koldstore-core/src/pk.rs`
- [X] T021 [P] Implement `HotRow`, `ColdRow`, `RowEvent`, and `Tombstone` data models in `/Users/jamal/git/pg-kalam/crates/koldstore-core/src/row.rs`
- [X] T022 [P] Implement safe predicate classification primitives for PK, scope, `_seq`, `_commit_seq`, immutable columns, mutable columns, and RLS quals in `/Users/jamal/git/pg-kalam/crates/koldstore-core/src/filter.rs`
- [X] T023 [P] Implement serializable schema registry, table metadata, cold segment, PK hint, row event, and type matrix models in `/Users/jamal/git/pg-kalam/crates/koldstore-catalog/src/schema_registry.rs`
- [X] T024 [P] Implement PostgreSQL-to-Arrow supported type matrix and unsupported-type diagnostics in `/Users/jamal/git/pg-kalam/crates/koldstore-catalog/src/type_matrix.rs`
- [X] T025 [P] Implement kalamdb-compatible manifest model, segment metadata, file state, publish state, and sync state in `/Users/jamal/git/pg-kalam/crates/koldstore-manifest/src/model.rs`
- [X] T026 [P] Implement manifest JSON schema validation and golden-file fixtures in `/Users/jamal/git/pg-kalam/crates/koldstore-manifest/tests/manifest_schema.rs` and `/Users/jamal/git/pg-kalam/tests/golden/manifest-v1.json`
- [X] T027 [P] Implement object-store backend factory and shared/user path templates for filesystem, S3, GCS, and Azure in `/Users/jamal/git/pg-kalam/crates/koldstore-storage/src/backend.rs`
- [X] T028 [P] Implement backend-safe conditional put/copy/delete helpers without assuming atomic rename in `/Users/jamal/git/pg-kalam/crates/koldstore-storage/src/publish.rs`
- [X] T029 Implement `_PG_init`, extension version SQL, schema creation, hook registration shell, and extension lifecycle checks in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/lib.rs`
- [X] T030 Implement SQL catalog DDL for `koldstore.storage`, `system.schemas`, `koldstore.manifest`, `system.jobs`, `koldstore.cold_segments`, `koldstore.cold_pk_hints`, and `koldstore.row_events` in `/Users/jamal/git/pg-kalam/sql/koldstore--0.1.0.sql`
- [X] T031 Implement pgrx GUC definitions for `koldstore.user_id`, `koldstore.enable_merge_scan`, `koldstore.internal_system_write`, and `koldstore.internal_flush_cleanup` in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/guc.rs`
- [X] T032 Implement privilege checks that prevent application roles from setting internal GUCs or reading storage credentials in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/security/privileges.rs`
- [X] T033 Implement PostgreSQL advisory-lock-backed transaction commit-order allocation for `_commit_seq` in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/hooks/xact.rs`
- [X] T034 Implement `SNOWFLAKE_ID()`, `koldstore_version()`, and `koldstore_user_id()` SQL functions in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/session.rs`
- [X] T035 Implement safe SPI helper wrappers for catalog reads/writes and error mapping in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/spi.rs`
- [X] T036 Implement DDL event-trigger skeleton for managed table schema changes in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/hooks/ddl.rs`
- [X] T037 Implement planner hook skeleton that detects active managed tables and delegates to merge-scan path construction in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/hooks/planner.rs`
- [X] T038 Implement DML hook/trigger skeleton for managed INSERT, UPDATE, DELETE, COPY, and system-column guards in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/hooks/executor.rs`
- [X] T039 Implement PostgreSQL memory-context ownership helpers for FFI allocations, scan state, SPI tuples, Arrow buffers, and object-store handles in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/memory.rs`
- [X] T040 Implement tracing spans for SQL API calls, DML hook work, flush phases, cold reader pruning, merge execution, and object-store I/O in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/observability.rs`
- [X] T041 [P] Add core unit tests for PK hashing, commit sequence types, row/tombstone models, and safe predicate classification in `/Users/jamal/git/pg-kalam/crates/koldstore-core/tests/core_models.rs`
- [X] T042 [P] Add manifest and storage unit tests for path templates, sync states, backend-safe publish helpers, and schema golden compatibility in `/Users/jamal/git/pg-kalam/crates/koldstore-manifest/tests/publish.rs`
- [X] T043 [P] Add catalog unit tests for type matrix, FK policy classification, schema registry serialization, cold segment models, and PK hint models in `/Users/jamal/git/pg-kalam/crates/koldstore-catalog/tests/catalog_models.rs`
- [X] T044 Add SQL regression tests for `CREATE EXTENSION`, schemas, catalog tables, GUC protection, `SNOWFLAKE_ID()`, and extension drop safety in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/lifecycle.rs`
- [X] T045 Add reusable E2E assertions for no duplicate hot PK rows, no object-store reads on hot DML, merge-scan explain checks, and MinIO artifact inspection in `/Users/jamal/git/pg-kalam/tests/e2e/common/assertions.rs`
- [X] T046 Add reusable benchmark client code for a regular heap table baseline and an equivalent pg-koldstore managed table in `/Users/jamal/git/pg-kalam/benchmarks/src/client.rs`
- [X] T047 Add memory allocation baseline helpers for RSS, PostgreSQL memory contexts, allocator stats, and per-scan peak allocation capture in `/Users/jamal/git/pg-kalam/tests/memory/memory_probe.rs`
- [X] T048 Document local build, pgrx setup, PostgreSQL version matrix, MinIO setup, benchmark thresholds, and memory-check workflow in `/Users/jamal/git/pg-kalam/docs/development.md`

**Checkpoint**: Foundation ready. User story implementation can now start in priority order or in parallel where dependencies allow.

---

## Phase 3: User Story 1 - Manage a Greenfield Table (Priority: P1) MVP

**Goal**: A developer creates a normal PostgreSQL table, registers storage, enables pg-koldstore with `koldstore.migrate_table`, inserts rows, and sees system columns plus metadata while preserving the application primary key.

**Independent Test**: Create shared and user-scoped greenfield tables with `DEFAULT SNOWFLAKE_ID()`, run `koldstore.migrate_table`, insert rows, and verify system columns, primary key preservation, scope metadata, and storage binding.

### Tests for User Story 1

- [X] T049 [P] [US1] Add failing SQL regression test for shared greenfield migration with `DEFAULT SNOWFLAKE_ID()` in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/greenfield_shared.rs`
- [X] T050 [P] [US1] Add failing SQL regression test for user-scoped greenfield migration with `scope_column => 'user_id'` and required `koldstore.user_id` in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/greenfield_user.rs`
- [X] T051 [P] [US1] Add failing E2E matrix test for greenfield shared and user-scoped tables on PostgreSQL 15, 16, and 17 in `/Users/jamal/git/pg-kalam/tests/e2e/greenfield_matrix.rs`
- [X] T052 [P] [US1] Add failing unit tests for storage registration path templates and credential redaction in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/storage_registration.rs`

### Implementation for User Story 1

- [X] T053 [US1] Implement `koldstore.register_storage` and `koldstore.alter_storage_credentials` SQL functions in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/ddl.rs`
- [X] T054 [US1] Implement empty-table `koldstore.migrate_table` entry point and shared/user argument validation in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/mod.rs`
- [X] T055 [US1] Implement system-column add for `_seq`, `_commit_seq`, `_deleted`, and optional `_user_id` without changing the application primary key in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/columns.rs`
- [X] T056 [US1] Implement schema registry insertion for greenfield table metadata, scope column, storage binding, flush policy, primary key, and indexed columns in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/register.rs`
- [X] T057 [US1] Implement user-scoped fail-closed policy setup for missing `koldstore.user_id` in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/scope.rs`
- [X] T058 [US1] Wire `SNOWFLAKE_ID()` defaults through pgrx SQL exposure and greenfield inserts in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/session.rs`
- [X] T059 [US1] Update quickstart greenfield examples and expected outputs in `/Users/jamal/git/pg-kalam/specs/001-pg-kalam-hot-cold-storage/quickstart.md`

**Checkpoint**: User Story 1 is independently functional and testable.

---

## Phase 4: User Story 2 - Migrate an Existing Table (Priority: P1)

**Goal**: An administrator converts an existing PostgreSQL table with rows to a managed table without changing the table name, primary key, or hot indexes.

**Independent Test**: Migrate a table with existing rows and indexes, reject unsafe tables, then read/update/delete hot rows through normal SQL.

### Tests for User Story 2

- [X] T060 [P] [US2] Add failing migration rejection tests for tables without a primary key in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/migrate_reject_no_pk.rs`
- [X] T061 [P] [US2] Add failing migration rejection tests for unsupported column types, generated columns, expression primary keys, and unsupported type evolution in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/migrate_type_matrix.rs`
- [X] T062 [P] [US2] Add failing migration tests that preserve existing data, primary key, secondary indexes, check constraints, and not-null constraints in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/migrate_existing.rs`
- [X] T063 [P] [US2] Add failing FK policy tests that reject inbound and outbound FKs when flush is enabled unless `options.allow_fk_hot_only = true` in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/migrate_fk_policy.rs`
- [X] T064 [P] [US2] Add failing E2E matrix test for existing table migration across PostgreSQL 15, 16, and 17 in `/Users/jamal/git/pg-kalam/tests/e2e/migrate_existing_matrix.rs`

### Implementation for User Story 2

- [X] T065 [US2] Implement migration validation for primary key shape, supported data types, generated columns, expression indexes, constraints, scope column, storage, and flush policy in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/constraints.rs`
- [X] T066 [US2] Implement existing-row backfill of `_seq`, `_commit_seq`, and `_deleted = false` under the commit-order lock in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/backfill.rs`
- [X] T067 [US2] Implement preservation checks for the original primary key and existing hot indexes without rewriting the primary key to `(pk, _seq)` in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/constraints.rs`
- [X] T068 [US2] Implement type-matrix capture and index-derived cold stats/bloom candidate registration in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/register.rs`
- [X] T069 [US2] Implement FK hot-only policy recording and explicit operator override handling in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/constraints.rs`
- [X] T070 [US2] Implement migration transaction rollback cleanup for partially added system columns and catalog rows in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/rollback.rs`
- [X] T071 [US2] Add migration operation locks to block concurrent DDL/DML during table conversion in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/lock.rs`

**Checkpoint**: User Story 2 is independently functional and testable.

---

## Phase 5: User Story 5 - Near-Native Hot DML (Priority: P1)

**Goal**: Applications use normal SQL INSERT, UPDATE, DELETE, and revive flows on hot rows while pg-koldstore keeps one hot row per PK and avoids object-store reads on the normal DML path.

**Independent Test**: Insert, update, delete, reinsert, and run concurrent hot DML while verifying one hot row per PK, `_seq` and `_commit_seq` stamping, row events, no cold object reads, and performance within the target threshold versus a regular heap table.

### Tests for User Story 5

- [X] T072 [P] [US5] Add failing hot INSERT/UPDATE/DELETE/revive invariant tests for one hot heap row per PK in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/hot_dml_invariants.rs`
- [X] T073 [P] [US5] Add failing test that direct user writes to `_seq`, `_commit_seq`, and `_deleted` are rejected unless internal guards are active in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/system_column_guards.rs`
- [X] T074 [P] [US5] Add failing test that hot UPDATE mutates the one hot row in place and advances `_seq` and `_commit_seq` in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/hot_update_stamp.rs`
- [X] T075 [P] [US5] Add failing test that hot DELETE physically deletes when no cold segment may contain the PK and tombstones when cold may contain the PK in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/hot_delete_tombstone.rs`
- [X] T076 [P] [US5] Add failing concurrency test for two transactions writing the same PK, commit-order `_commit_seq`, rollback gaps, and no duplicate hot rows in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/hot_dml_concurrency.rs`
- [X] T077 [P] [US5] Add failing instrumentation test proving normal hot INSERT/UPDATE/DELETE does not call the object-store reader in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/hot_dml_no_cold_reads.rs`
- [X] T078 [P] [US5] Add benchmark comparing regular heap table and equivalent pg-koldstore hot DML latency and throughput in `/Users/jamal/git/pg-kalam/benchmarks/src/hot_dml_vs_heap.rs`

### Implementation for User Story 5

- [X] T079 [US5] Implement managed INSERT stamping for `_seq`, `_commit_seq`, `_deleted = false`, manifest `pending_write`, and row-event append in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/hooks/executor.rs`
- [X] T080 [US5] Implement managed UPDATE stamping that updates one hot row in place and appends an update event in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/hooks/executor.rs`
- [X] T081 [US5] Implement managed DELETE routing for physical delete versus tombstone conversion based on local cold PK hints in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/hooks/executor.rs`
- [X] T082 [US5] Implement tombstone revive/upsert helper so reinserting after a hot tombstone updates that row instead of creating a duplicate in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/dml.rs`
- [X] T083 [US5] Implement `_seq` allocation and row/effect stamping helpers in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/dml.rs`
- [X] T084 [US5] Implement `_commit_seq` allocation under the transaction-scoped commit-order lock and hold it until transaction end in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/hooks/xact.rs`
- [X] T085 [US5] Implement direct system-column write guards for INSERT, UPDATE, COPY FROM, and generated DML paths in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/hooks/executor.rs`
- [X] T086 [US5] Implement shared row-event append helper for insert, update, delete, and revive operations in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/events.rs`
- [X] T087 [US5] Add DML hot-path tracing spans and object-store-read counters for regression tests and benchmark reports in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/observability.rs`

**Checkpoint**: User Story 5 is independently functional and testable.

---

## Phase 6: User Story 3 - Flush to Cold Storage (Priority: P1)

**Goal**: Eligible hot rows flush to kalamdb-compatible Parquet and manifest files, local metadata updates after manifest commit, and hot rows are cleaned only after the cold artifacts are safely visible.

**Independent Test**: Insert rows, run `koldstore.flush_table`, verify Parquet and manifest artifacts in MinIO, verify local cold metadata and PK hints, and verify the hot data remains authoritative on failure.

### Tests for User Story 3

- [X] T088 [P] [US3] Add failing unit tests for PostgreSQL schema to Arrow schema conversion, system columns, supported types, and unsupported type errors in `/Users/jamal/git/pg-kalam/crates/koldstore-parquet/tests/schema.rs`
- [X] T089 [P] [US3] Add failing Parquet writer/read round-trip tests for `_seq`, `_commit_seq`, `_deleted`, PK columns, stats, bloom metadata, and kalamdb-compatible layout in `/Users/jamal/git/pg-kalam/crates/koldstore-parquet/tests/writer_roundtrip.rs`
- [X] T090 [P] [US3] Add failing manifest publish tests for temp object, final object validation, manifest commit as visibility boundary, and no atomic rename assumption in `/Users/jamal/git/pg-kalam/crates/koldstore-manifest/tests/publish_protocol.rs`
- [X] T091 [P] [US3] Add failing SQL regression test that DML marks `koldstore.manifest.sync_state = 'pending_write'` without rewriting object-store `manifest.json` in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/manifest_pending.rs`
- [X] T092 [P] [US3] Add failing integration test for `koldstore.flush_table` writing Parquet, manifest, `koldstore.cold_segments`, and `koldstore.cold_pk_hints` in `/Users/jamal/git/pg-kalam/tests/e2e/flush_to_cold.rs`
- [X] T093 [P] [US3] Add failing crash/recovery test for orphan temp objects and unmanifested final objects in `/Users/jamal/git/pg-kalam/tests/e2e/flush_recovery.rs`
- [X] T094 [P] [US3] Add failing object-store outage test that leaves hot data authoritative and records retry/error job state in `/Users/jamal/git/pg-kalam/tests/e2e/flush_object_outage.rs`
- [X] T095 [P] [US3] Add failing PostgreSQL 15/16/17 E2E matrix test for flush, manifest, metadata, and hot cleanup in `/Users/jamal/git/pg-kalam/tests/e2e/flush_matrix.rs`

### Implementation for User Story 3

- [X] T096 [US3] Implement PostgreSQL catalog and `system.schemas` to Arrow schema conversion in `/Users/jamal/git/pg-kalam/crates/koldstore-parquet/src/schema.rs`
- [X] T097 [US3] Implement Parquet writer with compression, PK/_seq/_commit_seq stats, optional PK bloom filters, and row-group sizing in `/Users/jamal/git/pg-kalam/crates/koldstore-parquet/src/writer.rs`
- [X] T098 [US3] Implement manifest segment pruning metadata extraction from written Parquet footers in `/Users/jamal/git/pg-kalam/crates/koldstore-parquet/src/footer.rs`
- [X] T099 [US3] Implement backend-safe temp/final object publishing and manifest commit orchestration in `/Users/jamal/git/pg-kalam/crates/koldstore-manifest/src/publish.rs`
- [X] T100 [US3] Implement local `koldstore.manifest` cache state transitions `pending_write`, `syncing`, `in_sync`, `stale`, and `error` in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/flush/job.rs`
- [X] T101 [US3] Implement `koldstore.set_flush_policy`, `koldstore.flush_table`, and `koldstore.flush_pending` SQL functions in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/ops.rs`
- [X] T102 [US3] Implement bounded hot-row scan and latest-version/tombstone resolution for flush batches in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/flush/job.rs`
- [X] T103 [US3] Implement `koldstore.cold_segments` insertion after manifest commit with min/max `_seq`, min/max `_commit_seq`, row count, byte size, stats, schema version, and manifest identity in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/flush/job.rs`
- [X] T104 [US3] Implement local `koldstore.cold_pk_hints` update after successful flush using exact hashes when configured and bloom/range hints otherwise in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/flush/job.rs`
- [X] T105 [US3] Implement hot cleanup after manifest commit, including live-row removal and tombstone retention while older cold segments may contain the PK in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/flush/cleanup.rs`
- [X] T106 [US3] Implement built-in background worker registration and SQL/pg_cron fallback boundaries in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/flush/worker.rs`
- [X] T107 [US3] Implement idempotent orphan temp/final object recovery and quarantine behavior in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/flush/recovery.rs`

**Checkpoint**: User Story 3 is independently functional and testable.

---

## Phase 7: User Story 4 - Query Hot and Cold Transparently (Priority: P2)

**Goal**: Normal SELECT reads complete logical table contents through `KoldstoreMergeScan`, merging hot heap rows with cold Parquet rows and failing closed when cold data is required but unavailable.

**Independent Test**: Flush rows to cold, update/delete hot overlays, run normal SELECT and EXPLAIN, and verify merged logical results, pruning, residual filters, outage errors, and memory cleanup.

### Tests for User Story 4

- [X] T108 [P] [US4] Add failing unit tests for merge resolver winner rules, hot tie wins, tombstone masking, and one winner per PK in `/Users/jamal/git/pg-kalam/crates/koldstore-merge/tests/resolver.rs`
- [X] T109 [P] [US4] Add failing unit tests for safe pruning versus residual quals, including mutable app-column filters after winner resolution in `/Users/jamal/git/pg-kalam/crates/koldstore-merge/tests/quals.rs`
- [X] T110 [P] [US4] Add failing direct Parquet reader tests for projection, footer stats, row-group `_seq`/`_commit_seq` pruning, and PK bloom checks in `/Users/jamal/git/pg-kalam/crates/koldstore-parquet/tests/reader_pruning.rs`
- [X] T111 [P] [US4] Add failing SQL regression test that `EXPLAIN` shows `Custom Scan (KoldstoreMergeScan)` and heap-only final scan paths are blocked in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/merge_scan_explain.rs`
- [X] T112 [P] [US4] Add failing integration test for hot/cold merged SELECT, hot row winning over older cold row, and tombstone hiding older cold row in `/Users/jamal/git/pg-kalam/tests/e2e/merge_scan_results.rs`
- [X] T113 [P] [US4] Add failing integration test for object-store outage returning ERROR instead of partial hot-only results when cold is required in `/Users/jamal/git/pg-kalam/tests/e2e/merge_scan_outage.rs`
- [X] T114 [P] [US4] Add failing performance test that PK point lookup skips at least 90 percent of row groups in the fixture in `/Users/jamal/git/pg-kalam/benchmarks/src/cold_pruning.rs`
- [X] T115 [P] [US4] Add failing memory leak test that repeated KoldstoreMergeScan executions release scan memory contexts and object-store handles in `/Users/jamal/git/pg-kalam/tests/memory/merge_scan_leak.rs`
- [X] T116 [P] [US4] Add failing PostgreSQL 15/16/17 E2E matrix test for merged SELECT, EXPLAIN, residual quals, and cold outage behavior in `/Users/jamal/git/pg-kalam/tests/e2e/merge_scan_matrix.rs`

### Implementation for User Story 4

- [X] T117 [US4] Implement C shim callbacks for CustomPath, CustomScan, BeginCustomScan, ExecCustomScan, EndCustomScan, and Rescan in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/native/custom_scan.c`
- [X] T118 [US4] Implement planner path replacement that keeps the hot child path inside `custom_paths` and removes heap-only final scan paths for managed reads in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/merge_scan/path.rs`
- [X] T119 [US4] Implement CustomScan plan serialization for table oid, PK columns, system column attnums, scope key, safe quals, residual quals, RLS quals, projection, and segment hints in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/merge_scan/plan.rs`
- [X] T120 [US4] Implement BeginCustomScan metadata loading, snapshot capture, visible segment loading, safe pruning, and cold stream initialization in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/merge_scan/exec.rs`
- [X] T121 [US4] Implement direct object-store Parquet reader with projection, selected row groups, footer stats, PK bloom checks, and async-to-PostgreSQL execution bridging in `/Users/jamal/git/pg-kalam/crates/koldstore-parquet/src/reader.rs`
- [X] T122 [US4] Implement merge resolver for hot and cold row streams by PK, `_seq`, `_commit_seq`, tombstone masking, and hot tie wins in `/Users/jamal/git/pg-kalam/crates/koldstore-merge/src/resolver.rs`
- [X] T123 [US4] Implement residual qual and security qual evaluation through PostgreSQL expression evaluation after winner resolution in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/merge_scan/exec.rs`
- [X] T124 [US4] Implement safe segment and row-group pruning for PK, scope, `_seq`, `_commit_seq`, and immutable/stat-only columns in `/Users/jamal/git/pg-kalam/crates/koldstore-merge/src/quals.rs`
- [X] T125 [US4] Implement scan-state memory context reset, object-store handle cleanup, Arrow buffer drop, and rescan cleanup in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/merge_scan/exec.rs`

**Checkpoint**: User Story 4 is independently functional and testable.

---

## Phase 8: User Story 6 - Cold-Only DML APIs (Priority: P2)

**Goal**: Operators and applications can explicitly hydrate, update, or delete cold-only rows without making every standard SQL DML statement read object storage.

**Independent Test**: Flush a row cold-only, call `koldstore.hydrate_pk`, `koldstore.update_row(..., lookup_cold => true)`, and `koldstore.delete_row`, then verify default SELECT, rowcount semantics, row events, and no default object-store reads.

### Tests for User Story 6

- [X] T126 [P] [US6] Add failing test that `koldstore.hydrate_pk` reads exactly one cold PK and creates one hot row in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/hydrate_pk.rs`
- [X] T127 [P] [US6] Add failing test that `koldstore.update_row(..., lookup_cold => true)` updates a cold-only row and `lookup_cold => false` does not in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/update_row_api.rs`
- [X] T128 [P] [US6] Add failing test that `koldstore.delete_row` writes a PK-only tombstone from local metadata without scanning Parquet on the default path in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/delete_row_api.rs`
- [X] T129 [P] [US6] Add failing SQL regression test that standard SQL cold-only UPDATE affects 0 rows in MVP in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/cold_only_update_mvp.rs`
- [X] T130 [P] [US6] Add failing SQL regression test that standard SQL cold-only DELETE is enabled only for simple PK predicates with exact local metadata in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/cold_only_delete_sql.rs`
- [X] T131 [P] [US6] Add failing E2E test for cold-only DML APIs, row events, and object-store read counters across PostgreSQL 15, 16, and 17 in `/Users/jamal/git/pg-kalam/tests/e2e/cold_dml_matrix.rs`

### Implementation for User Story 6

- [X] T132 [US6] Implement `koldstore.hydrate_pk(table_name regclass, pk jsonb)` SQL function in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/dml.rs`
- [X] T133 [US6] Implement `koldstore.update_row(table_name regclass, pk jsonb, patch jsonb, lookup_cold boolean)` SQL function in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/dml.rs`
- [X] T134 [US6] Implement `koldstore.delete_row(table_name regclass, pk jsonb, allow_may_contain boolean)` SQL function in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/dml.rs`
- [X] T135 [US6] Implement exact local PK metadata lookup and may-contain hint semantics for cold-only DML in `/Users/jamal/git/pg-kalam/crates/koldstore-catalog/src/cold_pk_hints.rs`
- [X] T136 [US6] Implement simple PK-predicate extraction for standard SQL cold-only DELETE with exact rowcount semantics in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/hooks/executor.rs`
- [X] T137 [US6] Implement cold-only DML row-event emission and tombstone creation using shared event helpers in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/events.rs`

**Checkpoint**: User Story 6 is independently functional and testable.

---

## Phase 9: User Story 7 - User-Scoped Security (Priority: P2)

**Goal**: User-scoped managed tables fail closed without an active scope and restrict every hot and cold read/write to `koldstore.user_id`.

**Independent Test**: Set and unset `koldstore.user_id`, attempt cross-scope reads/writes, flush user-scoped rows, and verify cold path selection and RLS/security quals are enforced before object-store access or planning fails closed.

### Tests for User Story 7

- [X] T138 [P] [US7] Add failing SQL regression test that user-scoped SELECT and DML fail before reading hot or cold rows when `koldstore.user_id` is unset in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/user_scope_fail_closed.rs`
- [X] T139 [P] [US7] Add failing SQL regression test that cross-scope INSERT, UPDATE, DELETE, hydrate, update_row, and delete_row are denied in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/user_scope_dml.rs`
- [X] T140 [P] [US7] Add failing integration test that cold path selection applies scope filters before opening object-store streams in `/Users/jamal/git/pg-kalam/tests/e2e/user_scope_cold_pruning.rs`
- [X] T141 [P] [US7] Add failing test that RLS/security quals are evaluated against cold rows or planning fails closed in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/rls_cold_security.rs`
- [X] T142 [P] [US7] Add failing PostgreSQL 15/16/17 E2E matrix test for missing scope, cross-scope denial, flush, merged SELECT, and cold-only APIs in `/Users/jamal/git/pg-kalam/tests/e2e/user_scope_matrix.rs`

### Implementation for User Story 7

- [X] T143 [US7] Implement `koldstore.user_id` lookup, validation, and scope-key normalization for all SQL API paths in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/security/scope.rs`
- [X] T144 [US7] Implement planner-time fail-closed checks for user-scoped reads before hot or cold path setup in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/hooks/planner.rs`
- [X] T145 [US7] Implement DML-time scope enforcement before touching heap rows, local metadata, or object storage in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/hooks/executor.rs`
- [X] T146 [US7] Implement cold segment and PK hint filtering by `scope_key` before stream creation in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/merge_scan/exec.rs`
- [X] T147 [US7] Implement RLS/security qual classification, required-column projection, PostgreSQL residual evaluation, and fail-closed errors in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/security/rls.rs`

**Checkpoint**: User Story 7 is independently functional and testable.

---

## Phase 10: User Story 8 - Change Feed (Priority: P2)

**Goal**: A future realtime service can consume committed row changes through `koldstore.changes_since` using `_commit_seq` rather than duplicate hot heap rows or `_seq`.

**Independent Test**: Insert, update, delete, revive, rollback, and concurrent-commit a PK, then call `koldstore.changes_since` and verify ordered events, delete payloads, rollback omission, and retention gap errors.

### Tests for User Story 8

- [X] T148 [P] [US8] Add failing unit tests for row event ordering, retention gap detection, and event cursor semantics in `/Users/jamal/git/pg-kalam/crates/koldstore-merge/tests/changelog.rs`
- [X] T149 [P] [US8] Add failing SQL regression test that insert, update, delete, revive, rollback, and concurrent commits produce correct `koldstore.row_events` in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/row_events.rs`
- [X] T150 [P] [US8] Add failing SQL regression test that `koldstore.changes_since` orders by `_commit_seq`, not `_seq`, and reports retention gaps in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/changes_since.rs`
- [X] T151 [P] [US8] Add failing E2E test for `changes_since` after flush, cold-only delete, hydrate, and demigration boundary in `/Users/jamal/git/pg-kalam/tests/e2e/change_feed.rs`

### Implementation for User Story 8

- [X] T152 [US8] Implement changelog cursor and retention-gap logic in `/Users/jamal/git/pg-kalam/crates/koldstore-merge/src/changelog.rs`
- [X] T153 [US8] Implement `koldstore.changes_since(table_name regclass, since_commit_seq bigint, limit_rows integer)` SQL function in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/events.rs`
- [X] T154 [US8] Implement row-event retention configuration, purge job, and oldest retained commit sequence tracking in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/events.rs`
- [X] T155 [US8] Add `koldstore.row_events` indexes and retention metadata DDL in `/Users/jamal/git/pg-kalam/sql/koldstore--0.1.0.sql`

**Checkpoint**: User Story 8 is independently functional and testable.

---

## Phase 11: User Story 9 - Demigrate (Priority: P2)

**Goal**: An administrator exits pg-koldstore management and returns to a regular PostgreSQL heap table with current logical rows rehydrated by default.

**Independent Test**: Demigrate a table with hot, cold, and tombstone rows, then verify normal scans show current rows, pg-koldstore hooks are disabled, cold artifacts remain by default, and optional cold deletion works only after successful rehydrate.

### Tests for User Story 9

- [X] T156 [P] [US9] Add failing SQL regression test that default demigration rehydrates current hot+cold logical rows into the heap in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/demigrate_rehydrate.rs`
- [X] T157 [P] [US9] Add failing SQL regression test that demigrated tables no longer use KoldstoreMergeScan, DML hooks, flush jobs, or managed metadata in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/demigrate_disable.rs`
- [X] T158 [P] [US9] Add failing integration test that cold artifacts are retained by default and deleted only with `drop_cold => true` after rehydrate succeeds in `/Users/jamal/git/pg-kalam/tests/e2e/demigrate_cold_artifacts.rs`
- [X] T159 [P] [US9] Add failing test for `rehydrate => false` archive-detach warning and cold-only row invisibility in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/demigrate_archive_detach.rs`
- [X] T160 [P] [US9] Add failing PostgreSQL 15/16/17 E2E matrix test for demigration after flush, cold-only delete, and user-scoped tables in `/Users/jamal/git/pg-kalam/tests/e2e/demigrate_matrix.rs`

### Implementation for User Story 9

- [X] T161 [US9] Implement `koldstore.demigrate_table(table_name regclass, rehydrate boolean, drop_cold boolean, drop_system_columns boolean)` SQL function in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/ddl.rs`
- [X] T162 [US9] Implement exclusive demigration management lock and metadata state transition in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/rehydrate.rs`
- [X] T163 [US9] Implement rehydrate path that reads logical current rows through KoldstoreMergeScan and rebuilds the heap with one non-deleted row per PK in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/rehydrate.rs`
- [X] T164 [US9] Implement hook, planner, flush scheduling, and catalog deactivation after demigration in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/rehydrate.rs`
- [X] T165 [US9] Implement cold artifact retention and safe `drop_cold => true` deletion after successful rehydrate in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/migrate/rehydrate.rs`

**Checkpoint**: User Story 9 is independently functional and testable.

---

## Phase 12: User Story 10 - Operability and Backup (Priority: P3)

**Goal**: Operators can inspect managed tables, validate and recover cold storage, export/import kalamdb-compatible archives, and understand backup limitations.

**Independent Test**: Call status, backup, validate, recover, export/import, and DROP TABLE policy flows against a table with cold segments, corrupt/orphan artifacts, and credentials rotation.

### Tests for User Story 10

- [X] T166 [P] [US10] Add failing SQL regression test for `koldstore.table_status`, `koldstore.backup_manifest`, `koldstore.validate_cold_storage`, and `koldstore.recover_segments` in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/ops_functions.rs`
- [X] T167 [P] [US10] Add failing integration test for `system.jobs` status, error traces, retries, and recovery idempotence in `/Users/jamal/git/pg-kalam/tests/e2e/jobs_and_recovery.rs`
- [X] T168 [P] [US10] Add failing integration test for storage credential rotation without rewriting existing cold object paths in `/Users/jamal/git/pg-kalam/tests/e2e/storage_rotation.rs`
- [X] T169 [P] [US10] Add failing compatibility test that `koldstore_exec('EXPORT TABLE ...')` produces a manifest and Parquet archive readable by kalamdb-compatible tooling in `/Users/jamal/git/pg-kalam/tests/e2e/export_compatibility.rs`
- [X] T170 [P] [US10] Add failing DROP TABLE cleanup policy tests for retained, deleted, and failed object-store artifact cleanup in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/drop_table_policy.rs`

### Implementation for User Story 10

- [X] T171 [US10] Implement `koldstore.table_status` with hot rows, cold segment count, manifest state, pending jobs, storage binding, and last error fields in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/ops.rs`
- [X] T172 [US10] Implement `koldstore.backup_manifest` with table/scope filters and manifest identity output in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/ops.rs`
- [X] T173 [US10] Implement `koldstore.validate_cold_storage` with manifest JSON, Parquet readability, checksum, stats, PK hint, and catalog consistency checks in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/ops.rs`
- [X] T174 [US10] Implement `koldstore.recover_segments` for orphan cleanup, final-object quarantine, catalog repair, and manifest reload in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/ops.rs`
- [X] T175 [US10] Implement `koldstore_exec('EXPORT TABLE ...')` archive writer and `IMPORT TABLE` parser boundary in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/sql/ops.rs`
- [X] T176 [US10] Implement DROP TABLE event handling for object artifact retention/deletion policies in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/src/hooks/ddl.rs`
- [X] T177 [US10] Document base backup, PITR, pg_dump, COPY, logical replication, object backup, and export/import limitations in `/Users/jamal/git/pg-kalam/docs/backup-and-operations.md`

**Checkpoint**: User Story 10 is independently functional and testable.

---

## Final Phase: Polish & Cross-Cutting Concerns

**Purpose**: Full-system quality gates, edge-case coverage, performance proof, memory/leak checks, documentation, and final validation across PostgreSQL versions.

- [X] T178 [P] Add exhaustive edge-case SQL regression suite covering every `spec.md` edge case in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/edge_cases.rs`
- [X] T179 [P] Add end-to-end quickstart validation runner that executes every scenario in `quickstart.md` against PostgreSQL 15, 16, and 17 in `/Users/jamal/git/pg-kalam/tests/e2e/quickstart_matrix.rs`
- [X] T180 [P] Add benchmark suite comparing regular heap versus pg-koldstore for hot INSERT, hot UPDATE, hot DELETE, PK SELECT hot-only, PK SELECT cold-required, flush throughput, and demigration throughput in `/Users/jamal/git/pg-kalam/benchmarks/src/suite.rs`
- [X] T181 [P] Add benchmark report thresholds for SC-002 hot DML within 10 percent of regular heap and SC-006 PK lookup row-group pruning of at least 90 percent in `/Users/jamal/git/pg-kalam/benchmarks/src/verdict.rs`
- [X] T182 [P] Add benchmark JSON and HTML report generation with machine metadata, PostgreSQL version, object-store backend, row counts, percentiles, throughput, RSS, and allocation counters in `/Users/jamal/git/pg-kalam/benchmarks/src/report.rs`
- [X] T183 [P] Add long-running endurance test for repeated migrate, DML, flush, query, cold-only DML, demigrate, and remigrate cycles in `/Users/jamal/git/pg-kalam/tests/e2e/endurance.rs`
- [X] T184 [P] Add memory allocation tests for repeated migration, flush, merge-scan, cold reader, and demigration loops with PostgreSQL memory-context snapshots in `/Users/jamal/git/pg-kalam/tests/memory/allocation_growth.rs`
- [X] T185 [P] Add leak detection script using ASAN/LSAN and Valgrind suppressions for pgrx, PostgreSQL, Arrow, Parquet, and object_store allocations in `/Users/jamal/git/pg-kalam/tests/memory/valgrind.supp`
- [X] T186 [P] Add heaptrack or equivalent RSS profiling instructions and CI artifact upload for benchmark and E2E memory profiles in `/Users/jamal/git/pg-kalam/tests/memory/heap_profile.md`
- [X] T187 [P] Add failure-injection tests for MinIO outage, corrupt Parquet footer, stale manifest generation, missing manifest, orphan final object, credential failure, and network timeout in `/Users/jamal/git/pg-kalam/tests/e2e/failure_injection.rs`
- [X] T188 [P] Add compatibility test comparing pg-koldstore manifest and Parquet golden outputs with kalamdb-compatible reader expectations in `/Users/jamal/git/pg-kalam/tests/compat/kalamdb_manifest_parquet.rs`
- [X] T189 [P] Add pgrx SQL tests for COPY FROM shared tables, COPY FROM user-scoped rejection, COPY `(SELECT ...) TO`, and pg_dump limitation documentation examples in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/copy_and_dump.rs`
- [X] T190 [P] Add schema evolution tests for `ALTER TABLE ADD COLUMN`, older Parquet segment NULL/default coercion, unsupported type rejection, and schema version increment in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/schema_evolution.rs`
- [X] T191 [P] Add security hardening review tests for credential visibility, internal GUC writes, SQL injection-safe dynamic SQL, RLS fail-closed behavior, and application-role permissions in `/Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/security_hardening.rs`
- [X] T192 [P] Add documentation examples for SQL API, migration decisions, DML limitations, cold-only APIs, security, operations, and performance tuning in `/Users/jamal/git/pg-kalam/docs/sql-api.md`
- [X] T193 [P] Add developer architecture notes explaining differences from kalamdb RocksDB/Raft/DataFusion internals and why pg-koldstore uses heap plus Custom Scan in `/Users/jamal/git/pg-kalam/docs/architecture.md`
- [X] T194 [P] Add performance profiling guide with tracing span names, benchmark commands, heap baseline comparison, and investigation workflow in `/Users/jamal/git/pg-kalam/docs/performance.md`
- [X] T195 [P] Add release checklist covering build, install, extension upgrade SQL, PG 15/16/17 matrix, MinIO integration, benchmark thresholds, memory/leak gates, docs, and backup warnings in `/Users/jamal/git/pg-kalam/docs/release-checklist.md`
- [ ] T196 Run `cargo fmt --all`, `cargo clippy --workspace --all-targets --all-features`, `cargo test --workspace`, `cargo pgrx test`, `tests/e2e/run_pg_matrix.sh`, `tests/memory/run_memory_checks.sh`, and `cargo run -p pg-koldstore-benchmarks -- --suite all` and record results in `/Users/jamal/git/pg-kalam/docs/verification-results.md`

---

## Dependencies & Execution Order

### Phase Dependencies

- Setup (Phase 1) has no dependencies.
- Foundational (Phase 2) depends on Setup and blocks all user stories.
- P1 delivery order should be US1, US2, US5, then US3 because flush relies on managed tables and DML stamping.
- US4 depends on US3 and US5 because transparent reads require cold segments and hot overlays.
- US6 depends on US4 and US5 because cold-only APIs use cold readers, hot DML stamping, and tombstones.
- US7 depends on US1, US4, US5, and US6 for scope enforcement across read, write, and API paths.
- US8 depends on US5 and US6 for row events emitted by normal and explicit DML.
- US9 depends on US3 and US4 for hot+cold rehydrate through KoldstoreMergeScan.
- US10 depends on US3, US4, US8, and US9 for operational visibility and recovery over complete state.
- Polish depends on all desired user stories for the release scope.

### User Story Dependencies

- US1: Starts after Phase 2; no story dependency.
- US2: Starts after Phase 2; builds on the same migration primitives as US1.
- US5: Starts after Phase 2 and can run after the migration interface from US1 exists.
- US3: Starts after US1/US2/US5 because flush requires managed metadata and DML manifest state.
- US4: Starts after US3 because it needs cold segments and manifests.
- US6: Starts after US4/US5 because it needs explicit cold read and hot tombstone paths.
- US7: Starts after US1/US4/US5 and should be rechecked after US6.
- US8: Starts after US5 and should be extended after US6.
- US9: Starts after US3/US4.
- US10: Starts after core cold path and demigration are stable.

### Implementation Rules

- Tests in each story phase must be written first and verified failing before implementation.
- Pure Rust crate logic should be implemented before pgrx glue when both are needed.
- Catalog/schema changes must be reflected in SQL migration files and Rust models in the same task or immediately adjacent dependent task.
- Any task touching FFI or PostgreSQL memory contexts must be validated by the memory/leak scripts before completion.
- Performance claims must be backed by the benchmark suite and compared to a regular heap table without pg-koldstore.
- PostgreSQL 15, 16, and 17 E2E matrix tests must pass before marking a story complete when that story has a matrix task.

---

## Parallel Execution Examples

### Phase 1 Setup

```text
Task: "Create the pure shared-types crate skeleton in /Users/jamal/git/pg-kalam/crates/koldstore-core/Cargo.toml and /Users/jamal/git/pg-kalam/crates/koldstore-core/src/lib.rs"
Task: "Create the manifest crate skeleton in /Users/jamal/git/pg-kalam/crates/koldstore-manifest/Cargo.toml and /Users/jamal/git/pg-kalam/crates/koldstore-manifest/src/lib.rs"
Task: "Create the object-store crate skeleton in /Users/jamal/git/pg-kalam/crates/koldstore-storage/Cargo.toml and /Users/jamal/git/pg-kalam/crates/koldstore-storage/src/lib.rs"
Task: "Create the Parquet crate skeleton in /Users/jamal/git/pg-kalam/crates/koldstore-parquet/Cargo.toml and /Users/jamal/git/pg-kalam/crates/koldstore-parquet/src/lib.rs"
```

### User Story 1

```text
Task: "Add failing SQL regression test for shared greenfield migration with DEFAULT SNOWFLAKE_ID() in /Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/greenfield_shared.rs"
Task: "Add failing SQL regression test for user-scoped greenfield migration with scope_column => 'user_id' and required koldstore.user_id in /Users/jamal/git/pg-kalam/crates/pg_koldstore/tests/greenfield_user.rs"
Task: "Add failing E2E matrix test for greenfield shared and user-scoped tables on PostgreSQL 15, 16, and 17 in /Users/jamal/git/pg-kalam/tests/e2e/greenfield_matrix.rs"
```

### User Story 3

```text
Task: "Add failing unit tests for PostgreSQL schema to Arrow schema conversion, system columns, supported types, and unsupported type errors in /Users/jamal/git/pg-kalam/crates/koldstore-parquet/tests/schema.rs"
Task: "Add failing Parquet writer/read round-trip tests for _seq, _commit_seq, _deleted, PK columns, stats, bloom metadata, and kalamdb-compatible layout in /Users/jamal/git/pg-kalam/crates/koldstore-parquet/tests/writer_roundtrip.rs"
Task: "Add failing manifest publish tests for temp object, final object validation, manifest commit as visibility boundary, and no atomic rename assumption in /Users/jamal/git/pg-kalam/crates/koldstore-manifest/tests/publish_protocol.rs"
Task: "Add failing object-store outage test that leaves hot data authoritative and records retry/error job state in /Users/jamal/git/pg-kalam/tests/e2e/flush_object_outage.rs"
```

### User Story 4

```text
Task: "Add failing unit tests for merge resolver winner rules, hot tie wins, tombstone masking, and one winner per PK in /Users/jamal/git/pg-kalam/crates/koldstore-merge/tests/resolver.rs"
Task: "Add failing direct Parquet reader tests for projection, footer stats, row-group _seq/_commit_seq pruning, and PK bloom checks in /Users/jamal/git/pg-kalam/crates/koldstore-parquet/tests/reader_pruning.rs"
Task: "Add failing performance test that PK point lookup skips at least 90 percent of row groups in /Users/jamal/git/pg-kalam/benchmarks/src/cold_pruning.rs"
Task: "Add failing memory leak test that repeated KoldstoreMergeScan executions release scan memory contexts and object-store handles in /Users/jamal/git/pg-kalam/tests/memory/merge_scan_leak.rs"
```

### Polish

```text
Task: "Add benchmark suite comparing regular heap versus pg-koldstore for hot INSERT, hot UPDATE, hot DELETE, PK SELECT hot-only, PK SELECT cold-required, flush throughput, and demigration throughput in /Users/jamal/git/pg-kalam/benchmarks/src/suite.rs"
Task: "Add memory allocation tests for repeated migration, flush, merge-scan, cold reader, and demigration loops with PostgreSQL memory-context snapshots in /Users/jamal/git/pg-kalam/tests/memory/allocation_growth.rs"
Task: "Add failure-injection tests for MinIO outage, corrupt Parquet footer, stale manifest generation, missing manifest, orphan final object, credential failure, and network timeout in /Users/jamal/git/pg-kalam/tests/e2e/failure_injection.rs"
```

---

## Implementation Strategy

### MVP First

1. Complete Phase 1 and Phase 2.
2. Complete US1 and US2 to make table management real.
3. Complete US5 to make hot DML correct and measurable.
4. Complete US3 to make cold artifacts and metadata real.
5. Stop and validate the P1 MVP with `cargo test --workspace`, `cargo pgrx test`, `tests/e2e/run_pg_matrix.sh`, and the heap-vs-pg-koldstore hot DML benchmark.

### Incremental Delivery

1. Add US4 to make cold reads transparent.
2. Add US6 for explicit cold-only mutations.
3. Add US7 and US8 for security and change-feed correctness.
4. Add US9 for exit path safety.
5. Add US10 and Polish tasks for operational release quality.

### Quality Gates

1. Unit tests for every pure crate.
2. SQL regression tests for every public SQL API and error condition.
3. PostgreSQL 15/16/17 E2E matrix for user-facing workflows.
4. MinIO/object-store failure-injection coverage.
5. Benchmark comparison against regular heap table without pg-koldstore.
6. Memory allocation growth and leak detection gates for pgrx, FFI, Arrow, Parquet, and object-store paths.
7. Documentation updates for every behavior that affects operators or application developers.
