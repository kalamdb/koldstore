# Data Model: Catalog Column Identity, Schema Versions, and Segment Lifecycle

**Feature**: `003-column-id-lifecycle`  
**Date**: 2026-07-11

## Overview

Hard-cutover model (no legacy shapes):

```text
Managed table
  └── SchemaVersion (1..N; one active)
        └── LogicalColumn (column_id permanent)
  └── ScopeCounterKey (table_id, Optional<scope_value>)  [in-memory]
  └── Pending / Segment (segment-{NNNN}.parquet)
        ├── field_id == column_id
        ├── column_stats keyed by column_id
        └── FileLifecycleState
```

## Entity Relationship

```text
koldstore.storage
  -> koldstore.schemas (versioned; next_column_id; columns[] with column_id)
  -> koldstore.manifest
  -> koldstore.segments (lifecycle incl. pending; object_path segment-NNNN)
  -> koldstore.segment_stats (keyed by column_id)

DML path (not durable segment rows):
  mirror row -> in-memory ScopeCounterKey bump
```

## ColumnId

Stable numeric identity for a logical column.

| Field | Rules |
|-------|-------|
| value | `u64`, assigned from table `next_column_id` |
| permanence | Never changes on rename or compatible type change |
| reuse | Never reused after drop |
| Parquet | Written as field identity (`field_id`) on new cold files |
| stats key | Sole key for catalog/manifest column stats |

Validation: `ColumnId` newtype; first column id ≥ 1 (match KalamDB).

## LogicalColumn

| Field | Rules |
|-------|-------|
| `column_id` | Required `ColumnId` |
| `name` | Current PostgreSQL attribute name; mutable on rename |
| `pg_type` / catalog type spelling | Current type; compatible promotions allowed |
| `nullable` | Current nullability |
| `active` | `false` after drop; inactive columns omitted from live schema views |
| `initial_default` | Frozen backfill for older files missing this column; set at ADD |
| `ordinal` | Display / SELECT * order |
| `attnum` (correlation) | PostgreSQL attribute number used only to detect rename vs drop+add; not the public identity |

## SchemaVersion

| Field | Rules |
|-------|-------|
| `table_oid` | Managed table |
| `version` | Monotonic integer starting at 1 |
| `active` | Exactly one active row per table |
| `columns` | LogicalColumn list for this version |
| `next_column_id` | Next unused id for this table (monotonic across versions) |
| `primary_key` | Ordered column names or column_ids; membership changes fail closed |
| `indexed_columns` | By `column_id` (and resolvable to current names) |

Catalog API (owned by `koldstore-catalog`):

- `active_schema(table) -> SchemaVersion`
- `schema_at(table, version) -> SchemaVersion`
- allocate column id via `next_column_id` then increment

## ScopeCounterKey

In-memory initiation key shared by User and Shared tables.

| Field | Rules |
|-------|-------|
| `table_id` / `table_oid` | Managed table |
| `scope_value` | `Some(value)` for User-scoped tables (scope column such as `user_id` / `tenant_id` / `device_id`); `None` for Shared tables |

## In-Memory Row Counter

| Field | Rules |
|-------|-------|
| key | `ScopeCounterKey` |
| count | Process-local count of mirrored DML effects |
| durability | Advisory for thresholding only; not alone durable |
| DML rule | Increment on mirror; **never** create/update a catalog segment row on insert alone |
| restart | Pre-flush/flush MUST reconcile from durable mirror/hot state when the map is cold/empty |

## Pending Segment

Catalog segment row created by **pre-flush** when a counter key reaches the configured segment threshold (or force/drain policy).

| Field | Rules |
|-------|-------|
| `status` | `pending` until flush starts writing |
| scope | Empty/`None` for Shared; concrete scope value for User |
| meaning | Snapshot or range of hot/mirror rows ready to move to cold storage |
| visibility | Not query-visible |

## Segment

| Field | Rules |
|-------|-------|
| `segment_id` | UUID |
| `object_path` | Ends with `segment-{NNNN}.parquet` (zero-padded, width ≥ 4) |
| `segment_number` | Numeric NNNN used in filename (hard-cutover name; replaces batch numbering) |
| `schema_version` | Version at write time |
| `column_stats` | JSON object keyed by **stringified column_id** → `{min,max}` (and null_count if kept) |
| `columns_present` | Optional list of column_ids in file (recommended; enables fast missing-column fill) |
| `status` | Lifecycle enum below |
| `scope_key` | Scope for User segments; empty for Shared |
| seq/commit bounds, row_count, byte_size | Unchanged semantics; byte_size from publish |

### FileLifecycleState

| State | Meaning | Query visible? |
|-------|---------|----------------|
| `pending` | Pre-flush reservation; no cold object yet | No |
| `staged` | Written + validated; not yet in published snapshot | No |
| `published` | Manifest commit succeeded (completed) | Yes |
| `superseded` | Replaced (e.g. compaction) | No |
| `deleting` | Retention passed; delete in progress | No |
| `deleted` | Object removed (or delete acknowledged) | No |
| `orphaned` | No valid catalog/manifest owner / crash leftover | No |

Transitions:

```text
pre-flush -> pending -> (write temp) -> staged -> published
                                           -> superseded -> deleting -> deleted
                pending|staged -> orphaned (failed publish / expired lease / reconcile)
```

Flush drain: load `pending` → write (scoped when present) → verify → `published` → then prune hot/mirror. Multiple pending/flushing segments may exist concurrently.

## Footer-Derived ColumnStats

| Field | Rules |
|-------|-------|
| key | `ColumnId` only |
| min/max | Aggregated across row groups from Parquet footer after encode |
| conversion | Type-aware into catalog JSON; never false-exclude |
| source | In-memory bytes from encode/validate — not a second encode tracker |

## Removed Entities / Fields (hard cutover)

- Name-keyed `column_stats`
- `indexed_bounds` encode accumulators for catalog publish
- `batch-{n}.parquet` paths
- Old segment status meanings `active`, `compacted`, and pre-cutover `pending` if different from pre-flush reservation
- Schema columns without `column_id`
- Name-only evolution identity
- Separate User vs Shared flush-initiation mechanisms (one counter + pending path only)

## Validation Rules

- ADD → new `column_id` = `next_column_id`, then increment
- RENAME → same `column_id`, new name
- DROP → `active=false`, id never reused
- Compatible type change → same `column_id`, new type under rules
- Incompatible / PK change → reject; no hot prune
- New object paths must match `segment-[0-9]{4,}.parquet`
- Only `published` segments participate in normal merge scan candidate sets
- Catalog segment rows for new work appear at pre-flush (`pending`), not on every DML
