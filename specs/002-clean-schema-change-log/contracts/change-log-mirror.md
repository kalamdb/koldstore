# Change-Log Mirror Contract

**Feature**: `002-clean-schema-change-log`

## Naming

Default mirror name:

```text
<koldstore schema>.<source table name>__cl
```

Example:

```text
public.messages -> koldstore.messages__cl
```

If a collision requires a different physical name, the selected mirror relation must be recorded in managed table metadata and reported to the operator.

## Required Shape

For a source table:

```sql
CREATE TABLE public.messages (
  tenant_id uuid NOT NULL,
  id bigint NOT NULL,
  body text NOT NULL,
  PRIMARY KEY (tenant_id, id)
);
```

The mirror must have the same primary-key columns as its leading identity:

```sql
CREATE TABLE koldstore.messages__cl (
  tenant_id uuid NOT NULL,
  id bigint NOT NULL,
  seq bigint NOT NULL,
  op smallint NOT NULL,
  changed_at timestamptz NOT NULL DEFAULT now(),
  commit_lsn pg_lsn NULL,
  PRIMARY KEY (tenant_id, id)
);
```

## Primary-Key Preservation

For every source primary-key column, preserve:

- column name
- primary-key order
- PostgreSQL type identity
- type modifier
- collation for collatable types
- domain identity where applicable
- primary-key-required non-nullability

Do not replace the primary key with only a hash or JSON object. Hashes may be secondary metadata but cannot replace exact PK columns in the mirror.

## Operation Values

| Value | Operation |
|-------|-----------|
| `1` | INSERT |
| `2` | UPDATE |
| `3` | DELETE |

Rules:

- The mirror is latest-state only.
- There is at most one mirror row per primary key.
- `seq` must increase for every committed state change for the same primary key.
- DELETE keeps a tombstone mirror row.
- Reinsert after DELETE changes the tombstone back to `op = 1`.

## Explicit Non-Requirements

The mirror must not require:

- `row_data`
- `cold_segment_id`
- `_commit_seq`
- `_deleted`
- `_user_id`

The mirror is not a full history log.
