# Implementation Plan: pg-kalam Hot/Cold Storage Extension

**Branch**: `001-pg-kalam-hot-cold-storage` | **Date**: 2026-07-02 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-pg-kalam-hot-cold-storage/spec.md` plus KalamMergeScan architecture review (Custom Scan + pgrx + DataFusion cold engine).

## Summary

pg-kalam is a PostgreSQL extension for **Kalam-managed logical tables**: hot versions in the heap, cold versions in kalamdb-compatible Parquet on object storage, unified by **KalamMergeScan**. Developers create tables with `CREATE TABLE ... USING kalamdb WITH (...)` (PostgreSQL types + `SNOWFLAKE_ID()`), or migrate existing tables anytime. Deletes on cold rows append hot tombstones. **Dual read semantics**: default PK merge hides tombstones; change-feed (`kalam.changelog` / `kalam.changes_since`) exposes all versions including deletes for future realtime.

**Technical approach**: Rust/pgrx extension with a thin C shim for Custom Scan callbacks; DataFusion used **only** for cold Parquet scan/pruning inside KalamMergeScan; PostgreSQL remains the outer SQL planner. This amends spec FR-036 (DataFusion excluded) per planning research—see [research.md](./research.md).

## Technical Context

**Language/Version**: Rust 1.78+ (pgrx), C (Custom Scan shim), SQL (catalog + API); PostgreSQL 15+ (target 16/17)

**Primary Dependencies**: pgrx, Apache DataFusion (cold Parquet only), `object_store` crate, `parquet`/`arrow` (via DataFusion), PostgreSQL server headers

**Storage**: PostgreSQL heap (hot), object storage Parquet segments (cold), PostgreSQL catalog tables (`kalam.*`, `system.*`, `pg_kalam.*`)

**Testing**: `cargo test`, `cargo pgrx test` / pg_regress, Docker Compose + MinIO integration scripts

**Target Platform**: Linux x86_64/aarch64 (primary); macOS for dev via pgrx

**Project Type**: PostgreSQL extension (single crate + C static lib)

**Performance Goals**: Merged PK lookup correctness 100%; flush within 2 scheduler cycles (60s default) after threshold; segment pruning on `_seq` range queries; cold scan projection/filter pushdown via DataFusion

**Constraints**: KalamMergeScan sole scan path; kalamdb-compatible flush/manifest; minimal DataFusion feature set; small extension binary; joins Kalam+PostgreSQL tables; no RocksDB/Raft; no Citus code copy (AGPL)

**Scale/Scope**: Tables up to 1M rows migration; FILE uploads to 100MB; shared + user-scoped table types; six user stories (P1–P3)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Project constitution (`.specify/memory/constitution.md`) is still a template. Interim gates derived from spec FR-034–FR-035 and extension best practices:

| Gate | Status | Notes |
|------|--------|-------|
| Modular responsibilities | PASS | Catalog, hot access, cold I/O, flush, security, FILE, custom scan separated in design |
| Independently testable boundaries | PASS | Rust unit tests per module; SQL contract tests; integration per quickstart |
| PostgreSQL extension best practices | PASS | Upgrade scripts, background worker crash safety, privilege separation planned |
| No unjustified complexity | PASS WITH TRACKING | DataFusion + Custom Scan justified in Complexity Tracking |
| Spec FR-036 (no DataFusion) | AMENDED | DataFusion limited to cold path only; documented in research |

**Post-design re-check**: All gates pass. Recommend `/speckit-constitution` to ratify project principles before implementation.

## Project Structure

### Documentation (this feature)

```text
specs/001-pg-kalam-hot-cold-storage/
├── plan.md              # This file
├── research.md          # Phase 0
├── data-model.md        # Phase 1
├── quickstart.md        # Phase 1
├── contracts/           # Phase 1
│   ├── sql-api.md
│   ├── kalam-merge-scan.md
│   └── manifest-schema.json
└── tasks.md             # Phase 2 (/speckit-tasks — not yet created)
```

### Source Code (repository root)

```text
pg_kalam/                          # pgrx extension crate root
├── Cargo.toml
├── pg_kalam.control
├── sql/
│   ├── pg_kalam--0.1.0.sql        # Extension install/upgrade
│   └── pg_kalam--0.1.0--0.2.0.sql
├── src/
│   ├── lib.rs                     # _PG_init, GUCs, hook registration
│   ├── catalog/                   # kalam.storage, system.schemas, jobs, cold_segments
│   ├── migrate/                   # kalam.migrate_table, schema evolution
│   ├── flush/                     # Background worker, job queue, Parquet writer
│   ├── merge_scan/                # Custom Scan (Rust side)
│   │   ├── planner.rs             # set_rel_pathlist_hook
│   │   ├── plan.rs                # PlanCustomPath
│   │   └── exec.rs                # Begin/Exec/EndCustomScan
│   ├── cold/                      # DataFusion session, segment pruning, object_store
│   ├── merge/                     # PK resolver, tombstone logic
│   ├── security/                  # Scope GUC, RLS qual translation
│   ├── file/                      # FILE type, upload, manifest files state
│   └── ffi/                       # C shim exports for custom scan structs
├── native/
│   └── custom_scan.c              # CustomScanMethods, TupleTableSlot bridge
└── tests/
    └── sql/                       # pg_regress cases

tests/
├── docker-compose.yml             # PostgreSQL + MinIO
├── integration/
│   └── run.sh                     # quickstart automation
└── fixtures/
    └── manifest/                  # golden manifest.json samples

docs/
└── architecture.md                # KalamMergeScan diagram (optional)
```

**Structure Decision**: Single pgrx crate with internal modules. C shim isolated under `native/` for Custom Scan API surface. Integration tests at repo `tests/` per pgrx conventions.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Custom Scan Provider (KalamMergeScan) | Managed table = logical hot+cold relation; vanilla heap scans return wrong results | FDW or views break transparent single-table semantics and hot index use |
| DataFusion (cold only) | Parquet projection/filter/row-group pruning at scale | Hand-rolled `parquet` reader lacks pushdown and pruning maturity |
| C shim + pgrx split | Custom Scan structs/callbacks poorly wrapped in pgrx | Pure C loses Rust safety for merge/flush; pure pgrx risky for low-level scan API |
| Append-version DML | Aligns with kalamdb; cold rows lack heap ctid | In-place UPDATE breaks version model and flush dedup |
| `pg_kalam.cold_segments` catalog | MVCC visibility for external Parquet | Manifest-only cannot answer snapshot visibility fast enough |

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL SQL Planner                      │
│  joins · aggregates · RLS qual attachment · permissions        │
└──────────────────────────┬──────────────────────────────────┘
                           │
              set_rel_pathlist_hook (pg_kalam)
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              Custom Scan: KalamMergeScan                       │
│  ┌─────────────────────┐    ┌─────────────────────────────┐ │
│  │ hot child plan       │    │ cold engine (DataFusion)    │ │
│  │ Seq/Index/Bitmap     │    │ manifest prune → Parquet    │ │
│  └──────────┬──────────┘    └──────────────┬──────────────┘ │
│               └──────────┬───────────────────┘                │
│                          ▼                                    │
│               merge resolver (PK, _seq, tombstone)            │
│                          ▼                                    │
│               TupleTableSlot → executor                       │
└─────────────────────────────────────────────────────────────┘

Flush path (background worker):
  hot heap → dedupe by PK → Parquet temp → rename → manifest.json
           → pg_kalam.cold_segments → remove hot rows (retention)
```

## Implementation Phases (High Level)

### Phase A — Foundation
Extension scaffold (pgrx), catalog DDL on `CREATE EXTENSION`, `kalam.register_storage`, `system.schemas`, GUC `kalam.user_id`, `kalam_version()`, `kalam_user_id()`, `SNOWFLAKE_ID()`.

### Phase B — Table creation & migration
`CREATE TABLE ... USING kalamdb WITH (...)`, `kalam.migrate_table`, index→bloom mapping, DDL event hooks for ALTER.

### Phase C — KalamMergeScan (hot-only first)
Custom Scan hook, hot child wrapper, planner tests (EXPLAIN), no cold yet.

### Phase D — Cold path
Parquet writer, manifest.json, `pg_kalam.cold_segments`, DataFusion cold reader, merge resolver.

### Phase E — Flush
Background worker, `system.jobs`, flush policies, `kalam.flush_table`, recovery.

### Phase F — Security & ops
RLS on cold, `kalam.table_status`, backup/validate/recover SQL, `kalam_exec` export/import, DROP TABLE storage cleanup, FILE type (P3).

## MVP Scope Boundaries

**In MVP**:
- `CREATE TABLE ... USING kalamdb WITH (...)` + migrate-anytime
- SELECT merge (hot+cold), joins with Kalam + PostgreSQL tables
- INSERT, append-style UPDATE/DELETE on hot; `kalam.update`/`kalam.delete` for cold-only (tombstone overrides cold)
- kalamdb-compatible flush (temp Parquet, full manifest rewrite, sync states)
- Parquet footer bloom/stats pruning; minimal DataFusion build
- `kalam_version()`, `kalam_user_id()`, `SET kalam.user_id`
- `kalam_exec` export/import; DROP TABLE removes storage

**Out of MVP** (per spec + research):
- Transparent SQL UPDATE/DELETE on cold-only rows
- Native UNIQUE/FK across hot+cold
- Parallel custom scan, aggregate/join pushdown to DataFusion
- Compaction, vector indexes, stream tables

## Spec Alignment Notes

| Spec item | Plan decision |
|-----------|---------------|
| FR-020 transparent merge | KalamMergeScan |
| FR-036 no DataFusion | **Amended**: DataFusion cold-only (see research.md) |
| FR-034 modular decomposition | Module layout in Project Structure |
| Out of scope: DataFusion | Superseded for cold engine by architecture review |
| Input line 9 "no DataFusion" | Product positioning preserved: PG-native coordination; DF not primary planner |

Recommend spec update in `/speckit-clarify` or manual edit to FR-036 before implementation tasks.

## Generated Artifacts

| Artifact | Path | Status |
|----------|------|--------|
| Research | [research.md](./research.md) | Complete |
| Data model | [data-model.md](./data-model.md) | Complete |
| SQL API contract | [contracts/sql-api.md](./contracts/sql-api.md) | Complete |
| KalamMergeScan contract | [contracts/kalam-merge-scan.md](./contracts/kalam-merge-scan.md) | Complete |
| Manifest schema | [contracts/manifest-schema.json](./contracts/manifest-schema.json) | Complete |
| Quickstart | [quickstart.md](./quickstart.md) | Complete |
| Tasks | tasks.md | Not created — run `/speckit-tasks` |

## Next Steps

1. `/speckit-constitution` — ratify project principles
2. Update spec FR-036 to reflect DataFusion cold-engine decision
3. `/speckit-tasks` — generate dependency-ordered tasks.md
4. `/speckit-implement` — begin Phase A foundation
