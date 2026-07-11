# Implementation Plan: Catalog Column Identity, Schema Versions, and Segment Lifecycle

**Branch**: `003-column-id-lifecycle` | **Date**: 2026-07-11 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/003-column-id-lifecycle/spec.md` plus planning directive: copy stable designs from `/Users/jamal/git/KalamDB`, hard cutover (no legacy/compat paths), lightweight implementation, delete duplicates.

## Summary

Hard-cutover KoldStore cold catalog and schema evolution onto the KalamDB model: permanent `column_id` / `next_column_id`, catalog-owned versioned schema access, `column_id` Parquet field identity, footer-derived segment stats keyed by `column_id`, KalamDB-aligned ALTER (add/rename/drop/compatible type change), `segment-{NNNN}.parquet` object names, and explicit file lifecycle states. Remove name-keyed identity, `batch-*.parquet` naming, dual encode-time bounds tracking, coarse segment statuses, and any compatibility shims for pre-cutover formats. Prefer porting proven KalamDB types and algorithms over inventing parallel designs.

## Technical Context

**Language/Version**: Rust (workspace edition), PostgreSQL extension via pgrx, SQL catalog DDL in `koldstore--0.1.0.sql`

**Primary Dependencies**: Existing `koldstore-*` crates; arrow/parquet for cold I/O; serde/json for registry + stats; KalamDB reference sources under `/Users/jamal/git/KalamDB` (commons schema models, flush helper, alter handlers, manifest segment metadata)

**Storage**: PostgreSQL `koldstore` catalog tables (`schemas`, `segments`, `segment_stats`, `manifest`, `jobs`); object store Parquet segments + manifest JSON

**Testing**: `cargo test` for library crates; `cargo pgrx test` / local `tests/e2e` with pgrx-managed Postgres. No Docker in the default correctness loop.

**Target Platform**: PostgreSQL 15–18 via pgrx

**Project Type**: Rust workspace + PostgreSQL extension

**Performance Goals**: Segment prune-before-open stays O(catalog segments) using footer-derived catalog stats; no open-every-file for prune; flush encode drops duplicate per-cell bounds work

**Constraints**:
- **Hard cutover**: no backward compatibility, no dual-read legacy formats, no keep-alive of old naming/status/stats-key schemes
- **No duplicates**: one owner per concern; delete superseded helpers when the replacement lands
- **Copy-first**: KalamDB designs for `column_id`, ALTER, and `column_id`-keyed stats are the default; adapt only for PostgreSQL-as-DDL-authority
- Library crates stay PostgreSQL-free; `pgrx` only in `pg_koldstore`
- Prefer type-safe IDs (`ColumnId`, schema version newtypes)

**Scale/Scope**: Managed shared/user tables; schema versioning; flush publish path; merge/cold read by `column_id`; lifecycle for flush/compaction/GC jobs. PK reshape remains fail-closed.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Project constitution file is still a scaffold. Active gates from `AGENTS.md` + crate architecture:

| Gate | Initial | Post-design |
|------|---------|-------------|
| Local pgrx-first tests; no Docker in `tests/` | PASS | PASS |
| Type-safe domain objects for IDs/boundaries | PASS — introduce `ColumnId` | PASS |
| Library crates PostgreSQL-free; `pgrx` only in extension | PASS | PASS |
| Avoid duplicate ownership / catch-all modules | PASS — catalog owns versioned schema + segments; delete dual stats + name identity | PASS |
| Schema vs catalog layering | ADJUST — catalog becomes the **access owner** for versioned schemas + cold segments; `koldstore-schema` shrinks to pure type/evolution leaf (no second registry API). Document in architecture update. | PASS |

Initial gate result: PASS with documented catalog ownership adjustment (no unjustified complexity).

## Project Structure

### Documentation (this feature)

```text
specs/003-column-id-lifecycle/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── catalog-api.md
│   ├── schema-evolution.md
│   ├── segment-lifecycle.md
│   ├── parquet-field-identity.md
│   └── test-plan.md
└── tasks.md             # /speckit-tasks later
```

### Source Code (repository root)

```text
crates/
├── koldstore-common/
│   └── src/
│       └── column_id.rs              # ColumnId newtype (port KalamDB u64 semantics)
├── koldstore-catalog/
│   └── src/
│       ├── schema_registry.rs        # Versioned schemas + columns with column_id (moved/owned here)
│       ├── schema_versions.rs        # active_version / get_version / next_column_id API
│       ├── segments.rs          # lifecycle states + object_path segment-NNNN
│       ├── column_stats.rs           # stats keyed by ColumnId only
│       ├── queries.rs                # catalog SQL (schemas + segments)
│       └── decode.rs                 # decode helpers keyed by ColumnId
├── koldstore-schema/
│   └── src/
│       ├── pg_type.rs                # Keep pure type matrix
│       └── evolution.rs              # Pure ALTER plan by ColumnId (rewrite; delete name-match)
├── koldstore-parquet/
│   └── src/
│       ├── schema.rs                 # Arrow fields with Parquet field_id = column_id
│       ├── writer.rs                 # plan_segment → segment-{NNNN}.parquet; footer stats extract
│       ├── footer_stats.rs           # Aggregate footer min/max → catalog stats by ColumnId
│       └── reader.rs                 # Project/resolve by field_id / ColumnId
├── koldstore-manifest/
│   └── src/
│       └── model/                    # Segment status enum + column_stats by ColumnId
├── koldstore-flush/
│   └── src/
│       ├── segment_write.rs          # Footer-derived stats only; delete indexed_bounds path
│       └── segment_catalog.rs        # Insert plans using ColumnId stats + new lifecycle
├── koldstore-migrate/
│   └── src/catalog/                  # Register via catalog schema_versions API
└── pg_koldstore/
    ├── sql/koldstore--0.1.0.sql      # DDL: column_id, next_column_id, lifecycle CHECK, drop legacy
    └── src/sql/migrate_pg.rs         # Evolution refresh via ColumnId correlation (attnum→id)

tests/e2e/
├── schema_evolution.rs               # add/rename/drop/compatible type
├── column_id_stability.rs            # never reuse IDs
└── segment_lifecycle.rs              # staged→published→superseded→…

# DELETE (do not keep shims)
# - name-keyed column_stats maps
# - indexed_bounds / update_indexed_bounds catalog path
# - batch-{n}.parquet planners
# - SegmentStatus { pending, active, compacted, deleted } (replace)
# - name-only schema evolution matching
# - dual encode+footer stats publishers
```

**Structure Decision**: Expand `koldstore-catalog` as the single catalog access owner for versioned schemas and cold segments. Keep `koldstore-schema` as a thin pure-logic leaf (types + evolution policy). Port algorithms/types from KalamDB; strip all pre-cutover formats.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Catalog owns schema version access while `koldstore-schema` remains a type leaf | Spec requires one catalog home + easy versions; architecture previously split registry vs cold bookkeeping | Fully merging schema+catalog into one crate would pull type-matrix consumers into cold SQL; leaf types stay separate, **duplicate registry API is deleted** |

## Implementation Principles (planning directive)

1. **Copy from KalamDB first** — `ColumnDefinition.column_id`, `TableDefinition.next_column_id`, ALTER handlers, `column_stats: HashMap<u64, …>`, segment metadata fields.
2. **Hard cutover** — rewrite catalog DDL and Rust types in place; no dual codecs, no “if legacy then…”.
3. **One stats owner** — after Parquet encode, derive catalog stats from footer metadata already in memory; delete encode-time bounds tracking used only for catalog publish.
4. **Lightweight** — prefer small focused modules; delete dead helpers in the same change that introduces replacements.
5. **No duplicates** — one path for naming, identity, stats, lifecycle, and evolution.
