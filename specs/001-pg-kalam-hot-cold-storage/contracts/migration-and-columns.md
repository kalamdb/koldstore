# Migration, System Columns & Constraint Contract

**Version**: 0.3.0 (planning)
**Branch**: `001-pg-koldstore-hot-cold-storage`

This contract supersedes the earlier append-only-heap model. pg-koldstore keeps the PostgreSQL heap close to native table behavior: at most one hot heap row exists for each logical primary key.

## Entry Point

pg-koldstore does **not** provide a PostgreSQL table access method in MVP.

Greenfield and existing tables use the same two-step model:

```sql
CREATE TABLE app.items (
  id bigint PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  title text NOT NULL,
  created_at timestamptz DEFAULT now()
);

SELECT koldstore.migrate_table(
  'app.items',
  table_type => 'shared',
  storage_name => 'local-minio',
  flush_policy => 'rows:1000,interval:60'
);
```

`CREATE TABLE ... USING koldstore` is rejected from the spec because PostgreSQL `USING method` means a table access method. pg-koldstore uses normal heap storage plus a Custom Scan read path, not a custom table AM.

## System Columns

| Column | PostgreSQL type | Required | Meaning |
|--------|-----------------|----------|---------|
| `_seq` | `bigint` | yes | Monotonic row version/effect id. Useful for tie-breaks and diagnostics, but not the external commit-order watermark. |
| `_commit_seq` | `bigint` | yes | Durable commit-order watermark assigned to committed managed-table mutations. `changes_since` uses this column. |
| `_deleted` | `boolean not null default false` | yes | Hot tombstone marker. Tombstones exist only when an older cold row may need to be masked. |
| `_user_id` | app-compatible scope type or `text` | user tables only when no app scope column is supplied | System-added scope column. |

Rejected alternatives:

- `integer` for `_seq` or `_commit_seq`: long-lived tables exceed `int4`.
- `bit(1)` for `_deleted`: no meaningful heap-size win and worse SQL/index ergonomics.
- `_seq` as the external changelog cursor: sequence allocation can happen before commit and PostgreSQL sequences are nontransactional, so `_seq` is not a commit-order guarantee.

## Hot Heap Invariant

The migrated table keeps its application primary key unchanged.

```sql
-- Before migration
PRIMARY KEY (id)

-- After migration
PRIMARY KEY (id)
-- pg-koldstore records logical PK = (id) in system.schemas
```

Rules:

- There MUST NOT be multiple hot heap tuples for the same logical PK.
- Hot `UPDATE` mutates the one hot row in place and advances `_seq` / `_commit_seq`.
- Hot `DELETE` physically deletes the row only when no cold segment may contain an older version for that PK.
- If cold may contain that PK, delete changes or inserts one hot tombstone row instead of physically removing the masking record.
- Flush removes flushed hot live rows only after Parquet and manifest commit succeed. Hot tombstones are retained while any cold segment may still contain an older live row for the PK.

This differs from kalamdb's append-versioned RocksDB model. pg-koldstore uses PostgreSQL's native unique index on the application PK for hot-path performance.

## Commit Sequence

`_commit_seq` is modeled after kalamdb's durable `commit_seq`, not after PostgreSQL sequences.

Implementation requirement:

1. On the first managed write in a transaction, acquire a transaction-scoped pg-koldstore commit-order lock for the affected commit domain (global MVP; per-table/per-scope optimization later).
2. Allocate the next `_commit_seq` while holding that lock.
3. Stamp changed hot rows, tombstones, and `koldstore.row_events` entries as part of normal DML in the same transaction.
4. Hold the lock until transaction end.

The lock is the cost of strict commit ordering in PostgreSQL. Without it, two transactions can allocate sequence values and commit in the opposite order. Rollback gaps are allowed because `_commit_seq` is a cursor, not a dense sequence.

## Scope Column

User-scoped tables need a scope identifier:

| Pattern | Behavior |
|---------|----------|
| Existing app column | `scope_column => 'user_id'`; pg-koldstore enforces `koldstore.user_id` against that column. |
| System-added column | `scope_column => NULL` and `table_type => 'user'`; pg-koldstore adds `_user_id`. |

Rules:

- Missing `koldstore.user_id` on a user-scoped query or DML fails closed.
- Cross-scope writes fail before touching hot heap or cold metadata.
- Scope is part of cold path routing and PK hint lookup.

## Migration Steps

1. Validate table has a primary key and supported column types.
2. Reject generated columns, expression PKs, or unsupported types unless explicitly listed as supported in `system.schemas`.
3. Add `_seq`, `_commit_seq`, `_deleted`, and optional `_user_id`.
4. Backfill existing rows with monotonic `_seq`, `_commit_seq`, and `_deleted = false`.
5. Preserve the application primary key and existing hot indexes.
6. Register table metadata, logical PK, scope, storage binding, flush policy, type matrix, and indexed columns in `system.schemas`.
7. Build local cold metadata tables as empty (`koldstore.cold_segments`, `koldstore.cold_pk_hints`, `koldstore.row_events`).
8. Enable planner hook participation, DML triggers/hooks, RLS policy wiring, and flush scheduling.

Migration MUST NOT rewrite the primary key to `(pk..., _seq)`.

## Constraint and FK Limitations

Native PostgreSQL constraints only see the hot heap.

| Constraint | MVP behavior |
|------------|--------------|
| Primary key on app columns | Preserved. Enforces one hot row per PK. |
| UNIQUE on app columns | Preserved for hot rows only. Does not inspect cold Parquet. |
| CHECK / NOT NULL | Enforced on hot rows and on rows before flush writes Parquet. |
| FK referencing a managed table | Unsafe if referenced parent rows can flush cold-only. Migration MUST reject by default when inbound FKs exist, unless `options.allow_fk_hot_only = true` or flush is disabled. |
| FK from a managed table | Enforced for hot referenced rows only. Cold-only referenced rows are invisible to native FK checks. Reject by default for flushed tables unless explicitly allowed. |

Operators must not claim global hot+cold uniqueness or referential integrity until pg-koldstore implements a real global constraint layer.

## Cold PK Hints

To avoid object-store reads on every DML, pg-koldstore maintains local metadata:

| Table | Purpose |
|-------|---------|
| `koldstore.cold_segments` | Segment-level commit, seq, stats, schema, status, object path. |
| `koldstore.cold_pk_hints` | Exact PK hashes when configured, or compact may-contain filters per segment/scope. |

Behavior:

- DML may consult local PostgreSQL metadata and indexes.
- DML MUST NOT synchronously scan Parquet or object storage on the normal hot path.
- Exact cold PK lookup is required before claiming exact row counts for cold-only standard SQL DML.
- False-positive may-contain hints may be used by explicit tombstone APIs where the caller accepts idempotent delete semantics.

## Tombstones and Reinsert

Tombstones are not a history mechanism in the hot heap. They are masks for older cold rows.

```text
cold has id=1, _seq=10, _deleted=false
DELETE id=1 -> hot tombstone id=1, _deleted=true, higher _seq/_commit_seq
INSERT id=1 -> revive/update the hot tombstone to _deleted=false with new values
```

Rules:

- If no cold segment may contain the PK, DELETE physically removes the hot row and no tombstone is kept.
- If cold may contain the PK, DELETE leaves exactly one hot tombstone.
- Reinsert after a tombstone is a hot upsert/revive path, not a second hot row.
- Change history lives in `koldstore.row_events`, not duplicate heap rows.

## Demigration

Default demigration is a true rehydration:

```sql
SELECT koldstore.demigrate_table('app.items', rehydrate => true, drop_cold => false);
```

Steps:

1. Acquire exclusive table management lock.
2. Read the logical current state through KoldstoreMergeScan.
3. Rebuild the heap so it contains one non-deleted current row per PK.
4. Disable KoldstoreMergeScan, DML hooks, flush scheduling, and catalog management.
5. Keep `_seq`, `_commit_seq`, and `_deleted` as ordinary columns unless `drop_system_columns => true`.
6. Retain cold objects by default; delete them only when `drop_cold => true`.

`rehydrate => false` is an archive-detach mode: it disables management without pulling cold data back into heap. It is not the default because normal table semantics would otherwise lose cold-only rows.
