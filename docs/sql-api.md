# SQL API

This page documents the SQL functions and GUCs exposed by the installed extension
today. Signatures below match the generated `koldstore--0.1.0.sql` output from
pgrx.

## Session

- `snowflake_id()` returns a monotonic Snowflake-like `bigint` id.
- `koldstore_version()` returns the extension version text.
- `koldstore_user_id()` returns the active `koldstore.user_id` GUC value, or
  `NULL` when unset.

## Configuration

Runtime settings use the `koldstore.` GUC prefix:

```sql
SET koldstore.user_id = 'tenant-a';
SET koldstore.cold_reads = 'auto';
SET koldstore.enable_merge_scan = on;
SET koldstore.max_open_parquet_readers = 32;
SET koldstore.max_running_jobs = 4;
SET koldstore.log_level = 'info';
```

Use PostgreSQL-native persistence for durable configuration, for example
`ALTER SYSTEM SET`, `ALTER DATABASE ... SET`, or `ALTER ROLE ... SET`, followed
by the normal PostgreSQL reload rules for the chosen scope.

### Public GUCs

| GUC | Type | Default | Meaning |
|-----|------|---------|---------|
| `koldstore.user_id` | string | empty | Active user-scope id for user-scoped managed tables. Required for scoped reads and writes. |
| `koldstore.cold_reads` | string | `auto` | `auto`, `on`, or `off`. `off` fails cold scans when active cold segments are required. |
| `koldstore.enable_merge_scan` | bool | `on` | Allows the planner to replace managed-table heap scans with `KoldMergeScan`. |
| `koldstore.max_open_parquet_readers` | int | `32` | Global advisory-lock slot count for Parquet readers opened by cold scans. Clamped to `1..=1024`. |
| `koldstore.max_running_jobs` | int | `4` | Maximum concurrently claimed KoldStore jobs. Clamped to `1..=1024`. |
| `koldstore.log_level` | string | `info` | Extension log verbosity: `error`, `warn`, `info`, `debug`, or `trace`. |

### Internal GUCs

These are reserved for extension maintenance paths. Application roles cannot set
them.

| GUC | Type | Default | Meaning |
|-----|------|---------|---------|
| `koldstore.internal_system_write` | bool | `off` | Allows internal KoldStore system writes. |
| `koldstore.internal_flush_cleanup` | bool | `off` | Allows pruning flushed hot and mirror rows during flush cleanup. |

## Storage and Migration

### `koldstore.register_storage`

Two overloads are available:

```sql
koldstore.register_storage(
  name text,
  storage_type text,
  base_path text,
  credentials jsonb,
  config jsonb
) RETURNS uuid;

koldstore.register_storage(
  name text,
  storage_type text,
  base_path text,
  credentials jsonb,
  config jsonb,
  shared_path_template text,
  user_path_template text
) RETURNS uuid;
```

The 5-argument overload uses default path templates
`{namespace}/{tableName}/` and `{namespace}/{tableName}/{scopeId}/`.
`storage_type` must be one of `filesystem`, `s3`, `gcs`, or `azure`.

### `koldstore.alter_storage_credentials`

```sql
koldstore.alter_storage_credentials(
  name text,
  credentials jsonb
) RETURNS void;
```

Rotates credentials without rewriting existing cold object paths.

### `koldstore.alter_storage_location`

```sql
koldstore.alter_storage_location(
  name text,
  base_path text,
  config jsonb
) RETURNS uuid;
```

Updates storage location/configuration without direct catalog DML.

### `koldstore.migrate_table`

Three overloads are available:

```sql
koldstore.migrate_table(
  table_name regclass,
  table_type text,
  storage_name text,
  flush_policy text DEFAULT NULL,
  scope_column text DEFAULT NULL
) RETURNS uuid;

koldstore.migrate_table(
  table_name regclass,
  table_type text,
  storage_name text,
  flush_policy text DEFAULT NULL,
  scope_column text DEFAULT NULL,
  order_column text DEFAULT NULL
) RETURNS uuid;

koldstore.migrate_table(
  table_name regclass,
  table_type text,
  storage_name text,
  flush_policy text DEFAULT NULL,
  scope_column text DEFAULT NULL,
  order_column text DEFAULT NULL,
  compression text DEFAULT NULL
) RETURNS uuid;
```

`migrate_table` validates a heap table, preserves its primary key, creates the
change-log mirror, records a `migrate_backfill` job, and returns that job id.
`table_type` is `shared` or `user`. Existing populated tables need either a
single auto-increment primary key or an explicit `order_column`.

`flush_policy` examples:

- `rows:1000` keeps at most 1000 pending mirror rows hot; older excess rows are
  eligible for flush.
- `duration:1d` flushes mirror rows older than one day.
- Policies can be combined, for example `rows:1000,duration:1h`.

### `koldstore.demigrate_table`

```sql
koldstore.demigrate_table(
  table_name regclass,
  rehydrate boolean DEFAULT NULL,
  drop_cold boolean DEFAULT NULL
) RETURNS bigint;
```

Disables management after rehydration or archive-detach mode.

## Flush and Cold Data

### `koldstore.enqueue_flush_job`

```sql
koldstore.enqueue_flush_job(
  table_name regclass,
  scope_key text DEFAULT NULL,
  force boolean DEFAULT false
) RETURNS bigint;
```

Inserts a pending flush job when none is already active for the table/scope.
Returns `1` when a new job was inserted and `0` when an active flush job already
exists. `force => true` stores `force` in the job payload so the next
`flush_table` call flushes all pending mirror rows instead of applying the table
flush policy.

### `koldstore.flush_table`

```sql
koldstore.flush_table(
  table_name regclass
) RETURNS uuid;
```

Ensures a flush job exists, runs the current flush path synchronously, and
returns the job id. Progress is visible in `koldstore.jobs` and
`koldstore.table_status(...)`.

Row selection behavior:

- If the pending/running job payload has `force = true`, all pending mirror rows
  are flushed.
- Otherwise, when a table flush policy is configured, only policy-selected rows
  are flushed. For example, with `rows:100` and 2000 pending mirror rows, one
  non-forced flush moves 1900 rows and leaves 100 hot.
- When no flush policy is configured, all pending mirror rows are flushed.

`flush_table` does not currently expose a SQL `force` argument. Use
`enqueue_flush_job(..., force => true)` before `flush_table`, or call
`flush_table` directly on tables without a policy.

### `koldstore.recover_segments`

```sql
koldstore.recover_segments(
  table_name regclass,
  dry_run boolean DEFAULT false
) RETURNS bigint;
```

Enqueues a segment recovery job. Returns `1` when a new job was inserted and `0`
when an equivalent active job already exists.

### `koldstore.table_status`

```sql
koldstore.table_status(
  table_name regclass,
  scope_key text DEFAULT NULL
) RETURNS jsonb;
```

Returns managed-table storage, mirror, cold-segment, manifest, and recent job
state. Job entries include ids, phases, statuses, and progress counters such as
`rows_flushed`.

## DML Boundaries

- Normal hot `INSERT`, `UPDATE`, and `DELETE` operate on the heap and mark local
  manifest state pending.
- Standard SQL cold-only `UPDATE` affects zero rows in the MVP.

The following explicit cold DML SQL functions are planned but not yet exposed by
the extension:

- `koldstore.hydrate_pk(...)`
- `koldstore.update_row(...)`
- `koldstore.delete_row(...)`

## Changes and Operations

The following operator SQL functions are planned but not yet exposed by the
extension:

- `koldstore.changes_since(...)`
- `koldstore.set_flush_policy(...)` — policy is currently set through
  `migrate_table(..., flush_policy => ...)`
- `koldstore.flush_pending()`
- `koldstore.backup_manifest(...)`
- `koldstore.validate_cold_storage(...)`
- `koldstore_exec('EXPORT TABLE ...')` — `IMPORT TABLE` remains rejected until
  ownership and conflict rules are complete

## Security

User-scoped tables require `koldstore.user_id` and fail closed when it is
missing. RLS/security qualifiers must be enforceable on cold rows or planning
must fail closed.
