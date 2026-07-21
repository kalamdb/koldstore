# SQL API Contract: pg-koldstore

**Version**: 0.3.0 (planning)
**Branch**: `001-pg-koldstore-hot-cold-storage`

Public SQL surface exposed by the `koldstore` PostgreSQL extension.

## Extension Lifecycle

```sql
CREATE EXTENSION koldstore;
DROP EXTENSION koldstore;  -- fails if managed tables exist unless CASCADE
```

Install creates `koldstore` and `system` schemas and catalog tables.

`shared_preload_libraries` **must** include `koldstore` before `CREATE EXTENSION`
(and before `manage_table`). Restart PostgreSQL after changing the preload list.
`session_preload_libraries` is not sufficient. Built-in background scheduling and
merge-scan hooks both require shared preload. pg_cron remains an optional
*scheduler* for manual `flush_table` calls; it does not replace shared preload.

## Built-in Functions

```sql
SELECT koldstore_version();
SELECT koldstore_user_id();
SELECT SNOWFLAKE_ID();
```

`SNOWFLAKE_ID()` is suitable for application primary keys and `_seq` generation. It is not a commit-order cursor.

## Session Configuration

| GUC | Type | Default | Context | Description |
|-----|------|---------|---------|-------------|
| `koldstore.user_id` | `text` | NULL | user | Required for user-scoped tables. |
| `koldstore.enable_merge_scan` | `bool` | `on` | superuser/admin | Diagnostic; managed reads error when disabled. |
| `koldstore.internal_system_write` | `bool` | `off` | internal/SUSET | Allows pg-koldstore to write system columns. |
| `koldstore.internal_flush_cleanup` | `bool` | `off` | internal/SUSET | Allows flush cleanup to remove hot rows. |

Application roles cannot set internal GUCs.

## Storage Registration

```sql
koldstore.register_storage(
  name text,
  storage_type text,
  base_path text,
  credentials jsonb,
  config jsonb DEFAULT '{}',
  shared_path_template text DEFAULT '{namespace}/{tableName}/',
  user_path_template text DEFAULT '{namespace}/{tableName}/{scopeId}/'
) RETURNS uuid

koldstore.alter_storage_credentials(name text, credentials jsonb) RETURNS void
```

Credentials are admin-only and must not be readable by normal application roles.

## Table Management

### Greenfield Table

```sql
CREATE TABLE app.items (
  id bigint PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  title text NOT NULL,
  created_at timestamptz DEFAULT now()
);

SELECT koldstore.migrate_table(
  table_name => 'app.items',
  table_type => 'shared',
  storage_name => 'local-minio',
  flush_policy => 'rows:1000,interval:60'
);
```

### Existing Table

```sql
koldstore.migrate_table(
  table_name regclass,
  table_type text,              -- 'shared' | 'user'
  storage_name text,
  flush_policy text DEFAULT NULL,
  scope_column name DEFAULT NULL,
  options jsonb DEFAULT '{}'
) RETURNS koldstore.managed_table_info
```

Behavior:

1. Adds `_seq`, `_commit_seq`, `_deleted`, and optional `_user_id`.
2. Preserves the application primary key.
3. Registers table in `system.schemas`.
4. Records index-derived cold stats/bloom columns.
5. Enables KoldstoreMergeScan and managed DML hooks.

`CREATE TABLE ... USING koldstore` is not part of the public API.

### Demigration

```sql
koldstore.demigrate_table(
  table_name regclass,
  rehydrate boolean DEFAULT true,
  drop_cold boolean DEFAULT false,
  drop_system_columns boolean DEFAULT false
) RETURNS void
```

Default behavior rehydrates the logical current hot+cold state into a regular heap table before disabling management. Cold artifacts are retained unless `drop_cold = true`.

`rehydrate => false` is archive-detach mode and must warn that cold-only rows will not be visible through normal table scans after demigration.

## DML APIs

Hot rows use standard SQL:

```sql
INSERT INTO app.items (title) VALUES ('new');
UPDATE app.items SET title = 'updated' WHERE id = 42;
DELETE FROM app.items WHERE id = 42;
```

Cold-only write APIs:

```sql
koldstore.hydrate_pk(
  table_name regclass,
  pk jsonb
) RETURNS boolean

koldstore.update_row(
  table_name regclass,
  pk jsonb,
  patch jsonb,
  lookup_cold boolean DEFAULT false
) RETURNS koldstore.dml_result

koldstore.delete_row(
  table_name regclass,
  pk jsonb,
  allow_may_contain boolean DEFAULT true
) RETURNS koldstore.dml_result
```

Rules:

- `hydrate_pk` reads cold only for the requested PK and inserts/updates one hot row.
- `update_row(..., lookup_cold => true)` opts into a cold lookup when no hot row exists.
- `delete_row` may insert a PK-only tombstone using local cold PK hints; it does not scan object storage on the default path.
- Standard SQL cold-only UPDATE is not transparent in MVP.
- Standard SQL cold-only DELETE is supported only for simple PK predicates when exact local cold metadata is available; otherwise use `koldstore.delete_row`.

`koldstore.next_seq(table)` is internal. Public consumers should not depend on it for change ordering.

## Change Feed

```sql
koldstore.changes_since(
  table_name regclass,
  since_commit_seq bigint,
  limit_rows integer DEFAULT 1000
) RETURNS SETOF koldstore.change_event
```

Returns events from `koldstore.row_events` ordered by `_commit_seq`.

Event fields:

| Field | Meaning |
|-------|---------|
| `commit_seq` | Durable commit-order cursor. |
| `seq` | Row/effect version id. |
| `op` | `insert`, `update`, `delete`, `revive`. |
| `pk` | JSON primary key. |
| `deleted` | Tombstone/delete flag. |
| `row_image` | Optional row image depending on retention/projection options. |

If the requested `since_commit_seq` is older than retained events, the function returns a gap error with the oldest available commit sequence.

## Flush Control

```sql
koldstore.flush_table(table_name regclass, scope_key text DEFAULT NULL, force boolean DEFAULT false)
koldstore.flush_pending()
koldstore.set_flush_policy(table_name regclass, policy text)
```

Flush contract:

1. DML marks scope `pending_write`; normal DML does not rewrite `manifest.json`.
2. Flush marks scope `syncing`.
3. Flush writes Parquet through a backend-safe temp/final publish protocol.
4. Flush persists `manifest.json` only after the final Parquet object is readable and validated.
5. PostgreSQL catalog/cache state is updated.
6. Hot cleanup runs after manifest commit; tombstones are kept while needed to mask cold rows.

Object stores do not all support atomic rename. The manifest commit is the visibility boundary.

## Observability and Recovery

```sql
koldstore.table_status(table_name regclass)
koldstore.backup_manifest(table_name regclass DEFAULT NULL)
koldstore.validate_cold_storage(table_name regclass DEFAULT NULL)
koldstore.recover_segments(table_name regclass)
```

PostgreSQL base backup/PITR does not include object-store Parquet or FILE blobs.

## COPY, pg_dump, and Logical Backup

| Operation | MVP behavior |
|-----------|--------------|
| `COPY FROM` shared managed table | Supported only when system columns are omitted or DEFAULTed; triggers/hooks stamp metadata. |
| `COPY FROM` user-scoped table | Rejected under RLS; use INSERT or load into staging then managed INSERT. |
| `COPY table TO` | Must be tested to ensure KoldstoreMergeScan is used; until then document `COPY (SELECT * FROM table) TO`. |
| `pg_dump` | Not sufficient for full cold data unless it uses merged SELECT and cold objects are backed up separately. Use `koldstore_exec('EXPORT TABLE ...')` for full logical export. |
| Logical replication | Native PostgreSQL logical replication sees hot heap/catalog changes, not object-store bytes. Full support is post-MVP. |

## Admin Compatibility

```sql
SELECT koldstore_exec('EXPORT TABLE app.items');
SELECT koldstore_exec('IMPORT TABLE app.items FROM ...');
```

`koldstore_exec` supports kalamdb-compatible export/import grammar for table transfer archives.

## Query Semantics

1. Managed table SELECT uses KoldstoreMergeScan.
2. Joins with managed and unmanaged PostgreSQL tables are supported above the scan.
3. Default read returns one current non-deleted row per PK.
4. Tombstones mask older cold rows.
5. `changes_since` reads row events by `_commit_seq`.
6. Cold pruning uses safe predicates only; mutable app-column filters are residual after merge.
