# Research: Catalog Column Identity, Schema Versions, and Segment Lifecycle

**Feature**: `003-column-id-lifecycle`  
**Date**: 2026-07-11

## Decision: Hard Cutover — No Legacy / Compat Paths

Replace name-keyed identity, `batch-*.parquet` naming, coarse segment statuses, and dual stats tracking in place. Do not keep dual-read codecs, feature flags for old formats, or migration of already-written development cold files.

**Rationale**: Extension is pre-production; the user directed removal of previous approaches. Dual paths create the exact duplicates this feature must eliminate.

**Alternatives considered**:

- Compatibility mapping for files without `field_id`: rejected by planning directive.
- Keep reading `batch-*.parquet` forever: rejected; only `segment-{NNNN}.parquet` for new and existing code paths after cutover (wipe/recreate managed tables in tests).
- Map old `pending/active/compacted/deleted` alongside new lifecycle: rejected; replace the enum/CHECK constraint entirely.

## Decision: Copy KalamDB Column Identity Model

Port the KalamDB design:

| Concept | KalamDB source | KoldStore target |
|---------|----------------|------------------|
| Permanent `column_id: u64` | `kalamdb-commons` `ColumnDefinition` | `ColumnId` + registry column row |
| `next_column_id` allocator | `TableDefinition.next_column_id` | per-table field on active schema registry |
| Stats keyed by column_id | `SegmentMetadata.column_stats: HashMap<u64, …>` | catalog + manifest stats maps |
| field_id = column_id | ColumnDefinition docs / DuckLake learnings | Parquet/Arrow field metadata on write |
| ALTER add/drop/rename/modify | `kalamdb-handlers` DDL alter + dialect | evolution planner + PG catalog correlation |

**Rationale**: User stated KoldStore replaces KalamDB; copying the stable identity model avoids a second incompatible scheme.

**Alternatives considered**:

- Use PostgreSQL `attnum` as public `column_id`: rejected as public identity; use `attnum` only as correlation key when detecting rename vs drop+add under PG DDL authority.
- UUID column ids: rejected; KalamDB uses monotonic `u64`.

## Decision: Catalog Owns Versioned Schema Access

`koldstore-catalog` becomes the **only** caller-facing owner for:

- active schema version
- historical schema version by number
- column lists with `column_id`
- cold segment rows / stats / lifecycle
- `next_column_id` allocation helpers

`koldstore-schema` remains a leaf for `PgType` / type matrix and pure evolution policy functions. Registry serde models and version accessors move into catalog (delete the duplicate registry surface from schema).

**Rationale**: Spec FR-001/FR-002 demand one catalog home and easy version access. Current split (`schema` registry + `catalog` cold + `migrate` writes + `flush` segment writes) creates duplicate paths. Concentrating access in catalog matches the user ask without forcing every type-matrix consumer to depend on cold SQL.

**Alternatives considered**:

- Merge `koldstore-schema` entirely into catalog: heavier dependency blast radius for parquet/migrate type checks.
- Keep registry in schema and only add facade in catalog: leaves two APIs (duplicate).

## Decision: Footer-Derived Catalog Stats Only (Delete Dual Tracking)

After Parquet encode, aggregate row-group footer statistics into segment-level min/max keyed by `ColumnId`, using in-memory written bytes (validate/publish buffer)—not a post-publish object GET. Delete `indexed_bounds` / `update_indexed_bounds` and any encode-time catalog bounds accumulator.

Align with ADR-002 (`docs/decisions/002-footer-derived-catalog-stats.md`) and with KalamDB’s **column_id-keyed** stats shape. Prefer footer extraction over KalamDB’s current RecordBatch `extract_column_stats` so there is a single statistics owner (the file).

**Rationale**: User explicitly required stopping double-fill; ADR already accepted footer derivation; prune-before-open still uses catalog.

**Alternatives considered**:

- Keep KalamDB batch extract only: rejected (still a second path vs footer).
- Drop catalog stats and open files to prune: rejected (breaks prune-before-open).

## Decision: `segment-{NNNN}.parquet` Naming Only

Replace `plan_segment` / path helpers that emit `batch-{n}.parquet` with zero-padded `segment-{NNNN}.parquet` (default width 4: `segment-0017.parquet`). Update manifest path parsing, tests, docs, examples.

**Rationale**: User-specified layout; clearer than flush “batch” terminology. KalamDB still uses `batch-` in places—KoldStore intentionally diverges here.

**Alternatives considered**:

- Keep `batch-` for KalamDB parity: rejected by user.
- Unpadded `segment-17.parquet`: rejected; lexicographic sort must match numeric order.

## Decision: Explicit File Lifecycle Enum

Replace coarse statuses with:

`staged` → `published` → `superseded` → `deleting` → `deleted`, plus `orphaned`.

Transitions owned by existing durable jobs (leases/phases/checkpoints). Query visibility: only `published` for the current manifest generation.

Map loosely from KalamDB `InProgress/Committed/Tombstone` but implement the fuller set the spec requires.

**Rationale**: Spec US6; plugs into existing job machinery without a parallel GC framework.

**Alternatives considered**:

- Keep `pending/active/compacted/deleted` names with new meanings: rejected (ambiguous, keeps legacy vocabulary).

## Decision: ALTER Detection Under PostgreSQL DDL Authority

PostgreSQL owns `ALTER TABLE`. On flush/schema refresh:

1. Read current PG attributes (`attnum`, name, type).
2. Correlate `attnum` → existing `column_id` for renames.
3. New `attnum` without mapping → allocate `next_column_id` (add).
4. Missing mapped column → mark `column_id` inactive (drop), never reuse.
5. Same `column_id`, type change → apply compatibility rules or fail closed.
6. PK membership change → fail closed.

Port behavioral rules from KalamDB alter tests (add/drop/rename/compatible modify), not KalamDB’s SQL dialect parser (PG already parsed DDL).

**Rationale**: Matches spec assumptions and KalamDB outcomes without reimplementing a second DDL engine.

**Alternatives considered**:

- Name-diff only evolution: rejected (cannot distinguish rename from drop+add).
- Require explicit `koldstore.alter_column` APIs for every change: deferred; observe PG first.

## Decision: Delete Duplicates Aggressively

When a replacement lands, delete in the same change set:

- name-keyed stats helpers and JSON shapes
- `batch-*` path planners and tests
- old segment status variants
- encode-time indexed bounds used only for catalog publish
- name-equality evolution matching
- any second “schema registry” API outside catalog

**Rationale**: User required lightweight code and no duplicates; leaving shims recreates the problem.

## Decision: Update Architecture Doc

Revise `docs/architecture/crate-architecture.md` “Do not merge schema and catalog” to: catalog owns versioned schema **access** + cold bookkeeping; schema crate is type/evolution leaf only; no duplicate registry APIs.

**Rationale**: Plan must not silently violate published architecture; document the intentional adjustment.
