# Implementation Plan: Clean Schema Change-Log Mirrors

**Branch**: `002-clean-schema-change-log` | **Date**: 2026-07-05 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/002-clean-schema-change-log/spec.md`

## Summary

Replace pg-koldstore's development-time system-column design with a clean-schema default. Enabling a table creates a per-table latest-state change-log mirror in the `koldstore` schema, preserves the user table's primary key shape exactly in that mirror, records DML state through mirror upserts, evaluates default `rows:N` hot-row-limit policies and optional duration row-age policies from mirror state, flushes base-row data plus mirror metadata to cold Parquet, migrates `changes_since` to mirror/cold latest-state metadata, removes the old global row-events/default system-column code paths, and updates tests and README limitations around unsupported primary-key alteration.

## Technical Context

**Language/Version**: Rust 1.96, PostgreSQL extension via pgrx 0.19.1, SQL catalog DDL, C shim for Custom Scan integration

**Primary Dependencies**: pgrx, arrow-array/arrow-schema 59, parquet 59, serde/serde_json, uuid, chrono, tokio for test/runtime support, existing `koldstore-*` workspace crates

**Storage**: PostgreSQL heap tables remain user-owned hot tables; `koldstore` schema stores per-table change-log mirrors and catalog metadata; cold storage remains manifest + Parquet on filesystem/S3/GCS/Azure through existing storage crates

**Testing**: Rust unit tests with `cargo test`; extension SQL/regression tests with `cargo pgrx test`; local e2e tests under `tests/e2e` using pgrx-managed PostgreSQL. Docker remains packaging/runtime smoke only, not the default correctness loop.

**Target Platform**: PostgreSQL 15-18 on supported Rust/pgrx platforms

**Project Type**: Rust workspace plus PostgreSQL extension and local integration test harness

**Performance Goals**: Hot DML keeps near-native behavior by avoiding object-store reads and doing at most one transactional mirror upsert per changed row; policy checks read indexed mirror sequence state; flush uses bounded batches and only cleans mirror/base rows after cold visibility is committed; point lookups continue to merge hot and cold by primary key without user-table system columns.

**Constraints**: User tables must not gain `_seq`, `_deleted`, `_commit_seq`, `_user_id`, or other KoldStore columns in the default path; mirror PK columns must preserve names, order, PostgreSQL data types, type modifiers, collations, domain identity where applicable, and primary-key-required non-nullability; old-to-new migration compatibility is out of scope; primary-key value/definition alteration on managed tables is not implemented; normal DML must not synchronously read object storage.

**Scale/Scope**: Supports greenfield and populated tables, including 1M-row migration validation; single-column and composite primary keys; shared and user-scoped tables with existing application-owned scope columns; replacement coverage for migration, DML, flush, merge, demigration, and catalog lifecycle tests.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

The project constitution file is still the default scaffold and contains no enforceable project-specific gates. The active repo guidance in `AGENTS.md` adds these planning constraints:

- Keep default development and verification local and fast with pgrx-managed PostgreSQL.
- Do not make `tests/` depend on Docker or Docker Compose.
- Prefer type-safe domain objects for identifiers, sequence values, table names, primary keys, and related boundaries.
- Keep modules small and split by responsibility when files become hard to scan.

Initial gate result: PASS. The plan keeps verification local/pgrx-first, does not add Docker-backed correctness tests, and assigns new primary-key/mirror concepts to focused domain objects and modules.

## Project Structure

### Documentation (this feature)

```text
specs/002-clean-schema-change-log/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── sql-api.md
│   ├── change-log-mirror.md
│   ├── flush-and-demigration.md
│   └── test-plan.md
└── tasks.md             # Created later by /speckit-tasks
```

### Source Code (repository root)

```text
crates/
├── koldstore-core/
│   └── src/
│       ├── pk.rs              # Keep/enrich type-safe PK shape and hashing
│       ├── row.rs             # Replace system-column row/event models with mirror/cold state models
│       └── seq.rs             # Keep SeqId-style sequencing for mirror/cold state
├── koldstore-catalog/
│   └── src/
│       ├── schema_registry.rs # Store clean app schema plus mirror metadata
│       ├── table_meta.rs      # Table/mirror ownership metadata
│       └── row_events.rs      # Remove or replace if no non-legacy consumer remains
├── koldstore-parquet/
│   └── src/
│       ├── schema.rs          # Build cold schemas from base columns + KoldStore metadata
│       ├── writer.rs          # Write live rows and delete-marker records
│       └── reader.rs          # Read delete markers and live records for merge
├── koldstore-merge/
│   └── src/
│       ├── resolver.rs        # Resolve hot rows and cold records without user-table system columns
│       ├── tombstone.rs       # Move tombstone decisions to mirror/cold delete-marker model
│       └── changelog.rs       # Remove or replace row-events-only change feed helpers
└── pg_koldstore/
    ├── sql/koldstore--0.1.0.sql
    └── src/
        ├── migrate/           # Create/drop mirrors, initialize populated tables, remove system-column add
        ├── hooks/             # Capture DML into mirrors, reject unsupported PK alteration
        ├── flush/             # Flush from mirror cutoff and clean mirror/base rows safely
        ├── merge_scan/        # Project user rows plus cold records without system columns
        └── sql/               # Public SQL behavior and removal/deferment of row-events APIs

crates/pg_koldstore/tests/
├── clean_schema_*.rs          # New/rewritten SQL regression tests
├── change_log_mirror_*.rs
├── flush_*.rs
└── demigrate_*.rs

tests/e2e/
├── migrate/
├── flush/
├── merge/
└── dml/
```

**Structure Decision**: Keep the existing workspace and crate boundaries. The feature is a behavioral refactor across current crates, not a new crate. Add focused modules only where the mirror contract would otherwise make existing system-column modules too broad.

## Phase 0: Research Output

See [research.md](./research.md). Key decisions:

- Use per-table latest-state mirrors, not a global row-events table.
- Do not support old-format migration because the extension is still in development.
- Initialize populated-table mirrors before allowing flush cleanup; do not flush during registration.
- Evaluate default `rows:N` policies from pending latest-state mirror row limits and optional duration policies from mirror `changed_at` row age.
- Migrate `changes_since` to mirror/cold latest-state metadata; it is not a full event-history feed.
- Preserve primary-key column shape exactly in the mirror and reject unsupported PK alteration.
- Persist cold delete markers from mirror tombstones when needed to mask older cold rows.

## Phase 1: Design Output

Generated artifacts:

- [data-model.md](./data-model.md)
- [contracts/sql-api.md](./contracts/sql-api.md)
- [contracts/change-log-mirror.md](./contracts/change-log-mirror.md)
- [contracts/flush-and-demigration.md](./contracts/flush-and-demigration.md)
- [contracts/test-plan.md](./contracts/test-plan.md)
- [quickstart.md](./quickstart.md)

## Post-Design Constitution Check

Result: PASS.

- Tests remain local and pgrx-managed by default.
- Docker is not introduced into correctness tests.
- Primary-key, sequence, table, mirror, operation, and flush-cutoff concepts are planned as typed domain objects rather than ad hoc strings/integers.
- Existing crate boundaries remain intact; old modules are removed or narrowed where their only purpose was system-column or row-events behavior.

## Complexity Tracking

No constitution violations.
