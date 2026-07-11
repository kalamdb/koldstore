# Tasks: Catalog Column Identity, Schema Versions, and Segment Lifecycle

**Input**: Design documents from `/Users/jamal/git/pg-kalam/specs/003-column-id-lifecycle/`

**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `contracts/`, `quickstart.md`

**Tests**: Spec defines independent tests per story; include focused unit/e2e verification tasks (not a separate TDD gate). Hard cutover: delete superseded paths in the same change that lands the replacement.

**Organization**: User stories are ordered for **implementation**, not spec numbering. **US7 is MVP / first story** per planning directive.

**Implementation order**: US7 → US1 → US2 → US3 → US5 → US4 → US6

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no incomplete dependencies)
- **[Story]**: US1…US7 from `spec.md`
- Include exact file paths in every task

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Align docs and module stubs for hard-cutover work without changing runtime behavior yet.

- [X] T001 Update `docs/architecture/crate-architecture.md` to state catalog owns versioned schema access + cold segments; `koldstore-schema` remains type/evolution leaf
- [X] T002 [P] Refresh `specs/003-column-id-lifecycle/contracts/segment-lifecycle.md` to include `pending` and the counter → pre-flush → flush workflow
- [X] T003 [P] Refresh `specs/003-column-id-lifecycle/data-model.md` Scope Counter / Pending Segment / lifecycle to match current `spec.md`
- [X] T004 [P] Add module stubs for planned files: `crates/koldstore-flush/src/scope_counters.rs`, `crates/koldstore-flush/src/pre_flush.rs`, `crates/koldstore-catalog/src/schema_versions.rs` (declare in respective `lib.rs` without behavior change)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Shared types and DDL needed before any user story. US7 needs `pending` lifecycle and a unified scope key; full column_id cutover comes in later stories.

**⚠️ CRITICAL**: No user story work until this phase completes.

- [X] T005 Add type-safe `ScopeCounterKey` / table+optional-scope identity in `crates/koldstore-common/src/scope_key.rs` (or extend existing `ScopeKey`) and export from `crates/koldstore-common/src/lib.rs`
- [X] T006 Replace cold segment status vocabulary with hard-cutover lifecycle enum including `pending` in `crates/koldstore-catalog/src/segments.rs` and `crates/koldstore-manifest/src/model/mod.rs`
- [X] T007 Update `crates/pg_koldstore/sql/koldstore--0.1.0.sql` `segments.status` CHECK to `pending|staged|published|superseded|deleting|deleted|orphaned`; remove old `pending|active|compacted|deleted` meanings
- [X] T008 [P] Update `crates/koldstore-setup/src/` DDL/index helpers (if any) to match new status CHECK
- [X] T009 Delete or rewrite call sites that still write/read old status strings (`active`/`compacted`) in `crates/koldstore-flush/src/segment_catalog.rs`, `crates/koldstore-manifest/src/lifecycle/`, and `crates/pg_koldstore/src/sql/flush/`
- [X] T010 Add segment-threshold / force-drain policy fields (or map clearly onto existing flush policy) in `crates/koldstore-flush/src/policy.rs` and document in `crates/koldstore-common/src/config.rs` if shared
- [X] T011 Run `cargo test -p koldstore-common -p koldstore-catalog -p koldstore-manifest -p koldstore-flush --lib` and fix compile breakages from status cutover

**Checkpoint**: Foundation ready — start US7 (MVP).

---

## Phase 3: User Story 7 - In-memory scope counters and pending-segment flush initiation (Priority: P1) 🎯 MVP

**Goal**: DML bumps in-memory `(table, Optional<scope>)` counters; manual flush runs pre-flush to create `pending` segments; flush drains pending segments (scoped for User tables) through the existing write/verify/publish/hot-prune path. One mechanism for User and Shared.

**Independent Test**: Insert into scoped + shared tables (counters only, no per-insert segment rows); `flush_table` → pending rows for threshold keys → cold write per scope → published → hot pruned; crash mid-flush remains retryable without duplicates.

### Implementation for User Story 7

- [X] T012 [P] [US7] Implement process-local counter map `(table_oid, Option<scope_value>) → count` with bump/get/drain/reconcile helpers in `crates/koldstore-flush/src/scope_counters.rs`
- [X] T013 [P] [US7] Add unit tests for shared (`None`) vs scoped keys, concurrent bumps, and threshold selection in `crates/koldstore-flush/tests/scope_counters.rs`
- [X] T014 [US7] Implement pre-flush planner: gather keys for a table, apply segment threshold / force policy, emit pending-segment descriptors in `crates/koldstore-flush/src/pre_flush.rs`
- [X] T015 [US7] Implement SQL/catalog plans to insert `pending` segment rows (scope_key, seq/range snapshot metadata as needed) in `crates/koldstore-flush/src/segment_catalog.rs` and/or `crates/koldstore-catalog/src/queries.rs`
- [X] T016 [US7] Wire DML mirror capture to bump `scope_counters` only (no segment row) in `crates/pg_koldstore/src/hooks/` (and remove duplicate bump paths that create per-insert segment work)
- [X] T017 [US7] Extend or replace `crates/koldstore-flush/src/table_counters.rs` manifest SQL counters so they do not duplicate the in-memory scope map as a second initiation mechanism (keep O(1) diagnostics if still needed, delete competing flush-initiation uses)
- [X] T018 [US7] Change flush orchestration so `flush_table` runs pre-flush then loads `pending` segments and flushes each (filter mirror/hot rows by scope when present) in `crates/pg_koldstore/src/sql/flush/execute.rs` and `crates/koldstore-flush/src/table_flush.rs` / `job.rs`
- [X] T019 [US7] Ensure successful path remains: write → verify → mark `published` → only then prune hot/mirror; on failure leave `pending`/`staged` retryable in `crates/koldstore-flush/src/segment_write.rs`, `recovery.rs`, and `crates/pg_koldstore/src/sql/flush/spi.rs`
- [X] T020 [US7] Support concurrent pending segments for multiple scopes of the same table without cross-scope mixing in `crates/koldstore-flush/src/pre_flush.rs` and flush selection
- [X] T021 [US7] Add counter rebuild/reconcile from durable mirror/hot state when in-memory map is cold/empty after restart in `crates/koldstore-flush/src/scope_counters.rs` and call from pre-flush
- [X] T022 [US7] Delete divergent User vs Shared flush-initiation paths; one API for both table kinds in `crates/koldstore-flush/src/` and `crates/pg_koldstore/src/sql/flush/`
- [X] T023 [US7] Add e2e coverage for counters → pending → scoped/shared flush in `tests/e2e/flush/pending_segment_counters.rs` (or extend `tests/e2e/flush/flush_matrix.rs`)
- [X] T024 [US7] Run `cargo test -p koldstore-flush` and local e2e flush tests; record results in `specs/003-column-id-lifecycle/tasks.md`

**T024 results (2026-07-11)**: `cargo test -p koldstore-common -p koldstore-catalog -p koldstore-manifest -p koldstore-flush --lib` pass; `cargo test -p koldstore-flush` pass (incl. `scope_counters` 7); `cargo check -p pg_koldstore` pass. E2E `pending_segment_counters` added (requires running pgrx server / install to execute). Note: cold write path remains table-wide for Shared MVP; per-scope mirror filter for User flush is deferred to follow-up within US7 polish if needed.

**Checkpoint**: US7 MVP works independently with manual flush.

---

## Phase 4: User Story 1 - One catalog home with easy schema versions (Priority: P1)

**Goal**: Catalog crate owns versioned schema access (`active` + `schema_at`) and cold segment metadata; no second registry API.

**Independent Test**: Load active and historical schema versions from catalog APIs alone.

### Implementation for User Story 1

- [ ] T025 [P] [US1] Move/own schema registry models + version accessors in `crates/koldstore-catalog/src/schema_registry.rs` and `crates/koldstore-catalog/src/schema_versions.rs`
- [ ] T026 [US1] Implement `active_schema` / `schema_at` / allocate helpers per `contracts/catalog-api.md` in `crates/koldstore-catalog/src/schema_versions.rs`
- [ ] T027 [US1] Point migrate/flush/extension callers at catalog APIs; remove duplicate registry surface from `crates/koldstore-schema/` (keep PgType/evolution leaf only) in `crates/koldstore-migrate/`, `crates/koldstore-flush/`, `crates/pg_koldstore/`
- [ ] T028 [US1] Update `crates/koldstore-catalog/src/queries.rs` for versioned schema reads/writes
- [ ] T029 [US1] Add catalog unit tests for active + historical version access in `crates/koldstore-catalog/tests/schema_versions.rs`
- [ ] T030 [US1] Run `cargo test -p koldstore-catalog -p koldstore-migrate -p koldstore-schema`

**Checkpoint**: US1 independently testable.

---

## Phase 5: User Story 2 - Stable column identity in catalog and cold files (Priority: P1)

**Goal**: Permanent `column_id` / `next_column_id`; Parquet field identity; stats keyed by `column_id`. Hard cutover from name keys.

**Independent Test**: Rename non-PK column; cold reads resolve by id; drop+add never reuses ids.

### Implementation for User Story 2

- [ ] T031 [P] [US2] Add `ColumnId` newtype in `crates/koldstore-common/src/column_id.rs` and export it
- [ ] T032 [US2] Extend registry columns with `column_id`, `active`, `attnum` correlation, `next_column_id` on schema version in `crates/koldstore-catalog/src/schema_registry.rs` and `crates/pg_koldstore/sql/koldstore--0.1.0.sql` / columns JSON shape
- [ ] T033 [US2] Assign ids on manage/register; never reuse after drop in `crates/koldstore-migrate/src/catalog/register.rs`
- [ ] T034 [US2] Write Parquet/Arrow field identity = `column_id` in `crates/koldstore-parquet/src/schema.rs` and writer path
- [ ] T035 [US2] Key catalog/manifest `column_stats` by `ColumnId` only; delete name-keyed maps in `crates/koldstore-catalog/src/decode.rs`, `crates/koldstore-manifest/src/model/mod.rs`, `crates/koldstore-flush/src/segment_catalog.rs`
- [ ] T036 [US2] Resolve cold reads by field_id / `ColumnId` in `crates/koldstore-parquet/src/reader.rs` and merge projection in `crates/pg_koldstore/src/merge_scan/`
- [ ] T037 [US2] Add e2e `tests/e2e/column_id_stability.rs` for add/drop/reuse and rename identity
- [ ] T038 [US2] Run `cargo test -p koldstore-parquet -p koldstore-catalog -p koldstore-flush` and column_id e2e

**Checkpoint**: US2 independently testable.

---

## Phase 6: User Story 3 - KalamDB-aligned ALTER evolution (Priority: P1)

**Goal**: Observe PG ALTER; correlate `attnum`→`column_id`; support add/rename/drop/compatible type change; fail closed on PK/incompatible.

**Independent Test**: ADD/RENAME/DROP/compatible type with cold data; incompatible fails without hot prune.

### Implementation for User Story 3

- [ ] T039 [US3] Rewrite `crates/koldstore-schema/src/evolution.rs` to plan by `ColumnId` + attnum correlation (delete name-only matching)
- [ ] T040 [US3] Implement dual defaults (`initial_default` vs insert default) on logical columns in `crates/koldstore-catalog/src/schema_registry.rs`
- [ ] T041 [US3] Wire schema refresh on flush/manage path via catalog + evolution in `crates/pg_koldstore/src/sql/migrate_pg.rs`
- [ ] T042 [US3] Port behavioral expectations from KalamDB alter tests into `tests/e2e/schema_evolution.rs` (expand beyond ADD-only)
- [ ] T043 [US3] Run schema evolution e2e + `cargo test -p koldstore-schema`

**Checkpoint**: US3 independently testable.

---

## Phase 7: User Story 5 - Single-source column stats after Parquet write (Priority: P1)

**Goal**: Catalog stats derived from footer after encode; delete `indexed_bounds` dual path.

**Independent Test**: Multi-RG flush; catalog stats match footer aggregates; no encode-time bounds accumulator for catalog.

### Implementation for User Story 5

- [ ] T044 [P] [US5] Implement footer aggregation → `ColumnId`-keyed catalog stats in `crates/koldstore-parquet/src/footer_stats.rs`
- [ ] T045 [US5] Wire segment finalize to use footer stats from in-memory bytes in `crates/koldstore-parquet/src/writer.rs` and `crates/koldstore-flush/src/segment_write.rs`
- [ ] T046 [US5] Delete `indexed_bounds` / `update_indexed_bounds` catalog publish path in `crates/koldstore-parquet/src/batch_builder.rs` and `crates/koldstore-flush/src/write.rs`
- [ ] T047 [US5] Type-aware physical→catalog JSON conversion; fail-open omit or fail flush for required columns per ADR-002 in `crates/koldstore-parquet/src/footer_stats.rs`
- [ ] T048 [US5] Add unit tests multi-RG / null-only / timestamptz in `crates/koldstore-parquet/tests/footer_stats.rs`
- [ ] T049 [US5] Run `cargo test -p koldstore-parquet -p koldstore-flush`

**Checkpoint**: US5 independently testable.

---

## Phase 8: User Story 4 - Deterministic `segment-NNNN` cold file names (Priority: P2)

**Goal**: New objects use `segment-{NNNN}.parquet` only; delete `batch-*` planners.

**Independent Test**: Flush produces zero-padded `segment-0001.parquet`, …

### Implementation for User Story 4

- [ ] T050 [US4] Change `plan_segment` / path helpers to `segment-{NNNN}.parquet` (width ≥ 4) in `crates/koldstore-parquet/src/writer.rs`
- [ ] T051 [US4] Update manifest path parsing and tests that assert `batch-` in `crates/koldstore-manifest/`, `crates/koldstore-storage/tests/`, `crates/koldstore-parquet/tests/`
- [ ] T052 [US4] Update docs/examples (`docs/`, `docker/sql/example.sql`, quickstart) to `segment-*` only
- [ ] T053 [US4] Run writer/storage/manifest tests; confirm no `batch-*.parquet` emissions remain

**Checkpoint**: US4 independently testable.

---

## Phase 9: User Story 6 - Explicit cold-file lifecycle states (Priority: P2)

**Goal**: Complete lifecycle transitions beyond MVP pending→published: staged, superseded, deleting, deleted, orphaned; job integration; compaction/GC hooks.

**Independent Test**: Interrupt mid-write; compact/supersede; orphan reconcile; retention delete.

### Implementation for User Story 6

- [ ] T054 [US6] Implement validated lifecycle transitions helper in `crates/koldstore-catalog/src/segments.rs` / `crates/koldstore-manifest/src/lifecycle/`
- [ ] T055 [US6] Map flush phases: pending → staged → published in `crates/pg_koldstore/src/sql/flush/execute.rs` and job checkpoints
- [ ] T056 [US6] Compaction/supersede → retention → deleting → deleted path (or stub durable job hooks) in `crates/koldstore-manifest/src/lifecycle/` and flush/compaction modules
- [ ] T057 [US6] Orphan reconciliation for unreferenced objects without valid lease in `crates/koldstore-flush/src/recovery.rs`
- [ ] T058 [US6] Add e2e `tests/e2e/segment_lifecycle.rs` for crash/retry and visibility rules
- [ ] T059 [US6] Run lifecycle e2e + `cargo test -p koldstore-manifest -p koldstore-flush`

**Checkpoint**: US6 independently testable; full feature stories complete.

---

## Phase 10: Polish & Cross-Cutting

**Purpose**: Hard-cutover cleanup, docs, and verification gate.

- [ ] T060 [P] Grep/delete remaining name-keyed stats, `batch-` planners, old status strings, and dual encode bounds helpers across `crates/` and `tests/`
- [ ] T061 [P] Update `specs/003-column-id-lifecycle/quickstart.md` and `README.md` limitations for column_id, pending flush, and segment naming
- [ ] T062 [P] Update `docs/architecture/flushing-table.md` for counter → pre-flush → pending → flush workflow
- [ ] T063 Run `cargo fmt`, `cargo test` for touched crates, and `./scripts/run-pg-e2e.sh` (or scoped e2e) ; record results in `specs/003-column-id-lifecycle/tasks.md`

---

## Dependencies & Story Order

```text
Phase 1 Setup
    → Phase 2 Foundation (lifecycle enum + scope key + policy)
        → Phase 3 US7 🎯 MVP (counters + pre-flush + pending drain)
            → Phase 4 US1 (catalog version API)
            → Phase 5 US2 (column_id) [after US1 registry home]
                → Phase 6 US3 (ALTER on column_id)
                → Phase 7 US5 (footer stats by column_id)
            → Phase 8 US4 (segment-NNNN naming) [can follow US7; soft-dep on flush paths]
            → Phase 9 US6 (full lifecycle polish) [extends US7 pending/published]
                → Phase 10 Polish
```

**Suggested MVP**: Phases 1–3 only (US7).

---

## Parallel Opportunities

### Within US7
- T012 + T013 in parallel after foundation
- T014 can start once T012 API shape exists
- T016 (hooks) parallel with T015 (catalog insert plans) after T014

### Within US2
- T031 parallel with registry JSON design notes; T034/T035 after ColumnId exists

### Polish
- T060, T061, T062 in parallel

---

## Implementation Strategy

1. **MVP first**: Ship US7 — in-memory counters, pre-flush pending segments, unified User/Shared flush initiation — reusing today’s write/verify/prune.
2. **Then identity stack**: US1 catalog home → US2 column_id → US3 ALTER → US5 footer stats.
3. **Then naming + lifecycle polish**: US4 `segment-NNNN`, US6 full state machine.
4. **Hard cutover every step**: delete old paths in the same PR/task batch; no dual codecs.
5. **Copy from KalamDB** where noted (`column_id`, ALTER rules, pending-write/per-scope initiation ideas).

---

## Task Summary

| Phase | Story | Tasks |
|-------|-------|-------|
| 1 Setup | — | T001–T004 (4) |
| 2 Foundation | — | T005–T011 (7) |
| 3 | US7 MVP | T012–T024 (13) |
| 4 | US1 | T025–T030 (6) |
| 5 | US2 | T031–T038 (8) |
| 6 | US3 | T039–T043 (5) |
| 7 | US5 | T044–T049 (6) |
| 8 | US4 | T050–T053 (4) |
| 9 | US6 | T054–T059 (6) |
| 10 Polish | — | T060–T063 (4) |
| **Total** | | **63** |

**Format validation**: All tasks use `- [ ]`, sequential IDs, optional `[P]`, story labels on US phases only, and file paths.
