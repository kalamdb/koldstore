# Contract: Catalog API

**Feature**: `003-column-id-lifecycle`  
**Owner crate**: `koldstore-catalog`

## Purpose

Single access surface for versioned managed-table schemas and cold segment metadata. Callers (migrate, flush, merge, extension) MUST NOT maintain a second schema-registry API.

## Required Operations

| Operation | Behavior |
|-----------|----------|
| `active_schema(table_oid)` | Return active `SchemaVersion` including columns with `column_id`, `next_column_id` |
| `schema_at(table_oid, version)` | Return exact historical version or error if missing |
| `allocate_column_id(table_oid)` | Return next id and advance allocator (transactionally with schema refresh) |
| `list_segments(table_oid, scope)` | Return segments with lifecycle + `column_stats` keyed by `column_id` |
| `upsert_segment(...)` | Persist segment row using cutover DDL only |
| `set_segment_lifecycle(id, state)` | Validated transition helper used by jobs |

## Invariants

- Exactly one active schema version per managed table.
- `next_column_id` never decreases; never equals an existing or historically used id for that table.
- Stats maps never use column name as key.
- `object_path` for new segments ends with `segment-{NNNN}.parquet`.

## Non-goals

- No legacy dual decoders.
- No Docker-only catalog paths.
