# Contract: Test Plan

**Feature**: `003-column-id-lifecycle`

## Library / Unit

- `ColumnId` allocation: add → drop → add never reuses ids
- Evolution planner: rename via attnum correlation; reject PK change
- Footer stats aggregation: multi-RG, null-only, timestamptz conversion
- Lifecycle transition validators
- Path planner emits only `segment-{NNNN}.parquet`

## Extension / E2E (pgrx-local)

| Scenario | Expect |
|----------|--------|
| Manage + flush | `segment-0001.parquet` (or padded equivalent), stats by column_id, status published |
| ADD COLUMN + flush + read old rows | backfill default/NULL; new column_id |
| RENAME COLUMN + read old cold | same column_id; values under new name |
| DROP non-PK + add other | dropped id unused |
| Compatible type change | same column_id; readable |
| Incompatible type / unsupported | flush fails; hot retained |
| Crash before publish | no query-visible staged file; recovery safe |
| Catalog API | `active_schema` + `schema_at(v)` return full column_id sets |

## Explicit Non-Tests

- No legacy `batch-*` round-trip
- No name-keyed stats fixtures
- No Docker-required correctness tests
