# SQL API Contract: pg-kalam

**Version**: 0.2.0 (planning)  
**Branch**: `001-pg-kalam-hot-cold-storage`

Public SQL surface exposed by the `pg_kalam` PostgreSQL extension.

---

## Extension Lifecycle

```sql
CREATE EXTENSION pg_kalam;
DROP EXTENSION pg_kalam;  -- fails if managed tables exist unless CASCADE
```

On install: creates `kalam`, `system`, `pg_kalam` schemas and catalog tables; registers DDL/event hooks for managed-table `ALTER TABLE`.

---

## Built-in Functions

```sql
SELECT kalam_version();    -- extension version string
SELECT kalam_user_id();    -- current session user id (NULL if unset)
SELECT SNOWFLAKE_ID();     -- monotonic PK default (kalamdb-compatible)
```

---

## Session Configuration

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `kalam.user_id` | `text` | NULL | Required for user-scoped tables |
| `kalam.changelog` | `bool` | `off` | When on, SELECT returns all versions by `_seq` including tombstones (change-feed mode) |
| `kalam.enable_merge_scan` | `bool` | `on` | Diagnostic; when off, managed table scans error |

```sql
SET kalam.user_id = 'user-alice';
```

**Contract**: On user-scoped managed tables, if `kalam.user_id` is unset → `ERROR: kalam user_id not set`.

---

## Table Creation (Primary Path)

```sql
CREATE TABLE app.shared_items (
  id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  title TEXT NOT NULL,
  value INTEGER,
  created_at TIMESTAMP DEFAULT NOW()
) USING kalamdb WITH (
  type = 'shared',
  flush_policy = 'rows:1000,interval:60',
  storage_id = 'local-minio',   -- optional if default storage configured
  compression = 'snappy'        -- optional
);

CREATE TABLE app.profiles (
  id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  name TEXT,
  age INTEGER
) USING kalamdb WITH (
  type = 'user'
);
```

**Behavior**:

1. Parses standard PostgreSQL column types
2. Adds `_seq bigint NOT NULL`, `_deleted boolean NOT NULL DEFAULT false`
3. For `type = 'user'`: enables scope enforcement via `kalam.user_id`
4. Registers table in `system.schemas` with indexed-column bloom/stats mapping
5. Marks relation Kalam-managed (KalamMergeScan only)

**`WITH` options**:

| Option | Values | Required |
|--------|--------|----------|
| `type` | `shared`, `user` | yes |
| `flush_policy` | `rows:N`, `interval:S`, `rows:N,interval:S` | no |
| `storage_id` | registered `kalam.storage` name | no (default storage) |
| `compression` | `none`, `snappy`, `zstd` | no |

---

## Table Migration (Existing Tables)

### `kalam.migrate_table`

```sql
kalam.migrate_table(
  table_name regclass,
  table_type text,              -- 'shared' | 'user'
  storage_name text,
  flush_policy text DEFAULT NULL,
  scope_column name DEFAULT 'user_id',
  options jsonb DEFAULT '{}'
) RETURNS void
```

May be run at any time on an existing PostgreSQL table. Reads existing indexes and maps indexed columns to Parquet bloom filters and manifest `column_stats`.

---

## Table Drop

```sql
DROP TABLE app.shared_items;
```

**Contract**: Removes PostgreSQL relation, `system.schemas` entry, `kalam.manifest` rows, and object-storage prefix (manifest, Parquet segments, FILE blobs) for that table/scope.

---

## Versioned DML (Cold-Safe)

### `kalam.update`

```sql
kalam.update(table_name regclass, pk jsonb, patch jsonb) RETURNS bigint
```

Appends new hot version row.

### `kalam.delete`

```sql
kalam.delete(table_name regclass, pk jsonb) RETURNS bigint
```

Appends hot tombstone (`_deleted = true`, new `_seq`). **Must override** all older hot and cold versions for that PK in merge reads.

Standard SQL `UPDATE`/`DELETE` work for rows with hot heap versions; cold-only rows require these functions.

**`_seq` contract**: Every INSERT, UPDATE, and DELETE appends a new row with a strictly higher `_seq`. Deletes set `_deleted = true` on the new row.

---

## Change-Feed (Realtime Readiness)

```sql
-- Option A: session mode
SET kalam.changelog = on;
SELECT * FROM app.shared_items WHERE _seq > 1000 ORDER BY _seq;

-- Option B: function (exact signature TBD)
SELECT * FROM kalam.changes_since('app.shared_items', 1000);
```

**Contract**:

| Mode | Behavior |
|------|----------|
| Default (`kalam.changelog = off`) | PK merge; tombstones excluded from logical results |
| Change-feed (`kalam.changelog = on` or `kalam.changes_since`) | All appended versions returned in `_seq` order, **including tombstones** with `_deleted = true` |

Tombstones are first-class change events for future realtime subscribers.

---

## kalam_exec (Export / Import / Admin)

```sql
SELECT kalam_exec('EXPORT TABLE app.shared_items');
SELECT kalam_exec('IMPORT TABLE app.shared_items FROM ...');
```

Passthrough for kalamdb-compatible administrative SQL including table export/import (Parquet + manifest archive). Exact command grammar matches kalamdb transfer jobs.

---

## Storage Registration

```sql
kalam.register_storage(name, storage_type, base_path, credentials, config, ...)
kalam.alter_storage_credentials(name, credentials)
```

---

## Flush Control

```sql
kalam.flush_table(table_name regclass, scope_key text DEFAULT NULL, force boolean DEFAULT false)
kalam.set_flush_policy(table_name regclass, policy text)
```

Flush contract (kalamdb-compatible):

1. `kalam.manifest.sync_state` → `syncing`
2. Write `batch-N.parquet.tmp` → rename `batch-N.parquet`
3. Rewrite `manifest.json` with **all** committed segments + `column_stats`
4. `kalam.manifest.sync_state` → `in_sync`

---

## Observability

```sql
kalam.table_status(table_name regclass)
kalam.backup_manifest(table_name regclass DEFAULT NULL)
kalam.validate_cold_storage(table_name regclass DEFAULT NULL)
kalam.recover_segments(table_name regclass)
```

---

## FILE Type

```sql
kalam.file_upload(table_name, column_name, filename, mime, content) RETURNS kalam.file
```

---

## Query Semantics

1. `KalamMergeScan` only for managed tables
2. Joins with other Kalam and normal PostgreSQL tables supported
3. **Default read**: PK merge; tombstone (`_deleted = true`) wins and is hidden from logical results
4. **Change-feed read**: all versions by `_seq` including tombstones (for realtime delete propagation)
5. RLS applies to hot and cold paths
6. Cold read uses Parquet footer stats/bloom pruning (kalamdb patterns)

**Verification**:

```sql
EXPLAIN SELECT m.* FROM app.shared_items m
  JOIN app.profiles p ON p.id = m.owner_id;
-- KalamMergeScan on managed relations; no heap-only bypass
```
