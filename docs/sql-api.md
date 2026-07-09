# SQL API

This page documents the SQL functions and GUCs exposed by the installed extension
today. Signatures below match the generated `koldstore--0.1.0.sql` output from
pgrx.

## Call style

PostgreSQL accepts both positional and named arguments for the same function.
These docs use **named arguments** (`arg => value`) so call sites stay readable
and resilient to optional trailing parameters. Positional calls remain valid;
they are not a second API.

```sql
-- Preferred (named)
SELECT koldstore.register_storage(
  name         => 'local-dev',
  storage_type => 'filesystem',
  base_path    => '/tmp/koldstore-demo',
  credentials  => '{}'::jsonb,
  config       => '{}'::jsonb
);

-- Also valid (positional)
SELECT koldstore.register_storage(
  'local-dev',
  'filesystem',
  '/tmp/koldstore-demo',
  '{}'::jsonb,
  '{}'::jsonb
);
```

## Session

```sql
SELECT snowflake_id();
SELECT koldstore_version();
SELECT koldstore_user_id();
```

| Function | Returns | Meaning |
|----------|---------|---------|
| `snowflake_id()` | `bigint` | Monotonic Snowflake-like id |
| `koldstore_version()` | `text` | Extension version |
| `koldstore_user_id()` | `text` | Active `koldstore.user_id` GUC value, or `NULL` when unset |

## Configuration

Runtime settings use the `koldstore.` GUC prefix:

```sql
SET koldstore.user_id = 'tenant-a';
SET koldstore.cold_reads = 'auto';
SET koldstore.enable_merge_scan = on;
SET koldstore.max_open_parquet_readers = 32;
SET koldstore.max_running_jobs = 4;
SET koldstore.log_level = 'info';
SET koldstore.min_max_rows_per_file = 1000;
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
| `koldstore.max_open_parquet_readers` | int | `32` | Per-backend open Parquet reader cap for cold scans (fail-fast when exceeded). Clamped to `1..=1024`. |
| `koldstore.max_running_jobs` | int | `4` | Maximum concurrently claimed KoldStore jobs. Clamped to `1..=1024`. |
| `koldstore.log_level` | string | `info` | Extension log verbosity: `error`, `warn`, `info`, `debug`, or `trace`. |
| `koldstore.min_max_rows_per_file` | int | `1000` | Minimum allowed `max_rows_per_file` for `manage_table` and flush. Lower temporarily for tests, for example `SET koldstore.min_max_rows_per_file = 100`. Clamped to `1..=1000000`. |

### Internal GUCs

These are reserved for extension maintenance paths. Application roles cannot set
them.

| GUC | Type | Default | Meaning |
|-----|------|---------|---------|
| `koldstore.internal_system_write` | bool | `off` | Allows internal KoldStore system writes. |
| `koldstore.internal_flush_cleanup` | bool | `off` | Allows pruning flushed hot and mirror rows during flush cleanup. |

## Exposed functions

Every SQL-callable function the extension installs today:

| Function | Returns |
|----------|---------|
| `snowflake_id()` | `bigint` |
| `koldstore_version()` | `text` |
| `koldstore_user_id()` | `text` |
| `koldstore.register_storage(...)` | `uuid` |
| `koldstore.alter_storage_credentials(...)` | `void` |
| `koldstore.alter_storage_location(...)` | `uuid` |
| `koldstore.manage_table(...)` | `uuid` |
| `koldstore.unmanage_table(...)` | `bigint` |
| `koldstore.enqueue_flush_job(...)` | `bigint` |
| `koldstore.flush_table(...)` | `uuid` |
| `koldstore.describe_table(...)` | `jsonb` |
| `koldstore.recover_segments(...)` | `bigint` |

## Storage and Migration

### `koldstore.register_storage`

Two overloads are available. Both accept named arguments.

```sql
-- Default path templates: {namespace}/{tableName}/ and
-- {namespace}/{tableName}/{scopeId}/
SELECT koldstore.register_storage(
  name         => 'local-dev',
  storage_type => 'filesystem',
  base_path    => '/tmp/koldstore-demo',
  credentials  => '{}'::jsonb,
  config       => '{}'::jsonb
);

-- Custom path templates
SELECT koldstore.register_storage(
  name                 => 'local-dev',
  storage_type         => 'filesystem',
  base_path            => '/tmp/koldstore-demo',
  credentials          => '{}'::jsonb,
  config               => '{}'::jsonb,
  shared_path_template => '{namespace}/{tableName}/',
  user_path_template   => '{namespace}/{tableName}/{scopeId}/'
);
```

`storage_type` must be one of `filesystem`, `s3`, `gcs`, or `azure`.

### `koldstore.alter_storage_credentials`

```sql
SELECT koldstore.alter_storage_credentials(
  name        => 'local-dev',
  credentials => '{"access_key_id":"...","secret_access_key":"..."}'::jsonb
);
```

Rotates credentials without rewriting existing cold object paths.

### `koldstore.alter_storage_location`

```sql
SELECT koldstore.alter_storage_location(
  name      => 'local-dev',
  base_path => '/var/lib/koldstore',
  config    => '{}'::jsonb
);
```

Updates storage location/configuration without direct catalog DML.

### `koldstore.manage_table`

```sql
SELECT koldstore.manage_table(
  table_name        => 'chat.messages',
  storage           => 's3_archive',
  hot_row_limit     => 10000,
  min_flush_rows    => 1000,
  max_rows_per_file => 1000
);
```

Registers a heap table for KoldStore management with structured flush settings.
`hot_row_limit` is required in the call (pass `NULL` for hot-only tables).
`table_type` defaults to `shared`; optional `scope_column`, `order_column`, and
`compression` arguments are also available.

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `table_name` | required | Table to manage (`regclass`) |
| `storage` | required | Registered storage backend name |
| `hot_row_limit` | required (`NULL` allowed) | Maximum mirror rows to keep hot; `NULL` for hot-only tables |
| `min_flush_rows` | `1000` | Minimum excess rows required before a flush moves data cold |
| `max_rows_per_file` | `1000` | Maximum rows written into one Parquet segment per flush batch (minimum `1000` unless lowered via `koldstore.min_max_rows_per_file`) |
| `table_type` | `'shared'` | `shared` or `user` |
| `scope_column` | `NULL` | Required when `table_type => 'user'` |
| `order_column` | `NULL` | Optional column used for migrate ordering hints |
| `compression` | `NULL` | Optional Parquet compression name |

Non-forced flush selection keeps the newest rows hot by mirror `seq` and always
flushes the oldest eligible excess first. Example with `hot_row_limit = 10000`
and `min_flush_rows = 1000`:

**Constraint note:** when `hot_row_limit` is set, `manage_table` rejects tables
with non-primary-key `UNIQUE` constraints or foreign keys. Koldstore enforces
those constraints on hot rows only after management; cold Parquet is not checked
on normal DML. See [Limitations](limitations.md#unique-and-foreign-key-constraints).

| Mirror rows | Flush result |
|-------------|--------------|
| 10,505 | No flush (`505` excess is below `min_flush_rows`) |
| 11,000 | Flush `1,000` rows into `1` file (`max_rows_per_file = 1000`) |
| 11,250 | Flush `1,000` rows, keep `10,250` hot |
| 11,500 | Flush `1,500` rows into `2` files |

### `koldstore.unmanage_table`

```sql
SELECT koldstore.unmanage_table(
  table_name => 'chat.messages'
);
```

Disables management after rehydration or archive-detach mode. Optional
`rehydrate` and `drop_cold` arguments default to `NULL` and can be passed by
name when needed:

```sql
SELECT koldstore.unmanage_table(
  table_name => 'chat.messages',
  rehydrate  => true,
  drop_cold  => false
);
```

## Flush and Cold Data

### `koldstore.enqueue_flush_job`

```sql
SELECT koldstore.enqueue_flush_job(
  table_name => 'chat.messages'
);

SELECT koldstore.enqueue_flush_job(
  table_name => 'chat.messages',
  force      => true
);
```

Inserts a pending flush job when none is already active for the table.
Returns `1` when a new job was inserted and `0` when an active flush job already
exists. `force => true` stores `force` in the job payload so the next
`flush_table` call flushes all pending mirror rows instead of applying the table
flush policy. `force` defaults to `false`. Flush jobs are table-wide; user-scope
partitioning uses the managed table's `scope_column` and session
`koldstore.user_id`, not an enqueue argument.

### `koldstore.flush_table`

```sql
SELECT koldstore.flush_table(
  table_name => 'chat.messages'
);
```

Ensures a flush job exists, runs the current flush path synchronously, and
returns the job id. Progress is visible in `koldstore.jobs` and
`koldstore.describe_table(...)`.

Row selection behavior:

- If the pending/running job payload has `force = true`, all pending mirror rows
  are flushed.
- Otherwise, when a table flush policy is configured, only policy-selected rows
  are flushed. Tables managed through `manage_table` honor `hot_row_limit`,
  `min_flush_rows`, and `max_rows_per_file`.
- When no flush policy is configured, all pending mirror rows are flushed.

`flush_table` does not currently expose a SQL `force` argument. Use
`enqueue_flush_job(..., force => true)` before `flush_table`, or call
`flush_table` directly on tables without a policy.

### `koldstore.describe_table`

```sql
SELECT koldstore.describe_table(
  table_name => 'chat.messages'
);
```

Returns managed-table storage, mirror, cold-segment, manifest, and recent job
state as JSONB. Counters are table-wide across scopes.

### `koldstore.recover_segments`

```sql
SELECT koldstore.recover_segments(
  table_name => 'chat.messages'
);

SELECT koldstore.recover_segments(
  table_name => 'chat.messages',
  dry_run    => true
);
```

Enqueues a segment recovery job. Returns `1` when a new job was inserted and `0`
when an equivalent active job already exists. `dry_run` defaults to `false`.

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
- `koldstore.backup_manifest(...)`
- `koldstore.validate_cold_storage(...)`
- `koldstore_exec('EXPORT TABLE ...')` — `IMPORT TABLE` remains rejected until
  ownership and conflict rules are complete

## Security

User-scoped tables require `koldstore.user_id` and fail closed when it is
missing. RLS/security qualifiers must be enforceable on cold rows or planning
must fail closed.
