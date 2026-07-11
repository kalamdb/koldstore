# Contract: Schema Evolution

**Feature**: `003-column-id-lifecycle`  
**References**: KalamDB ALTER tests (`backend/tests/misc/schema/`), `koldstore-schema` evolution policy

## Authority

PostgreSQL executes `ALTER TABLE`. KoldStore refreshes registry metadata by correlating `attnum` → `column_id`.

## Supported Outcomes

| PG change | Registry effect |
|-----------|-----------------|
| ADD COLUMN | New `column_id`; set `initial_default` when provided; bump schema version |
| RENAME COLUMN | Same `column_id`; update name; bump version |
| DROP COLUMN (non-PK) | Mark inactive; never reuse id; bump version |
| Compatible type change | Same `column_id`; update type; bump version |
| Index set change | Refresh indexed column ids; bump version if needed |

## Rejected Outcomes (fail closed)

- Primary-key membership or PK column drop/rename that changes identity
- Incompatible type change
- Unsupported cold type
- Name collision on rename

## Read Semantics After Evolution

- Resolve cold fields by `column_id` / Parquet `field_id`.
- Missing column in older file → fill `initial_default` or NULL.
- Dropped columns not projected in live queries.

## Hard Cutover

Delete name-equality matching in `plan_schema_evolution`. No “best effort” rename by identical type+position without `attnum` correlation.
