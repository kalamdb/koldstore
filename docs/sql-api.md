# SQL API

This page documents the SQL functions and GUCs exposed by the installed extension
today. Signatures below match the generated `koldstore--<version>.sql` output from
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
SELECT koldstore.preload_status();
```

| Function | Returns | Meaning |
|----------|---------|---------|
| `snowflake_id()` | `bigint` | Monotonic Snowflake-like id |
| `koldstore_version()` | `text` | Extension version |
| `koldstore_user_id()` | `text` | Active `koldstore.user_id` GUC value, or `NULL` when unset |
| `koldstore.preload_status()` | `jsonb` | Whether `shared_preload_libraries` lists `koldstore`, whether this process loaded via shared preload, and `enable_merge_scan` |

`shared_preload_libraries = 'koldstore'` is **mandatory** for correct managed-table
reads. Without it, `_PG_init` / `CREATE EXTENSION` / `LOAD` fail closed, and
`manage_table` errors. `session_preload_libraries` is not sufficient.

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
| `koldstore.cold_reads` | string | `auto` | `auto`: cold eligible by catalog/cost; `on`: cold eligible without forcing unnecessary object reads; `off`: hot-only and ERROR when correctness requires cold segments. |
| `koldstore.enable_merge_scan` | bool | `on` | Required for managed-table SELECT. When `off`, `KoldMergeScan` errors at execution instead of allowing an incorrect heap-only read. |
| `koldstore.max_open_parquet_readers` | int | `32` | Per-backend open Parquet reader cap for cold scans (fail-fast when exceeded). Clamped to `1..=1024`. |
| `koldstore.max_running_jobs` | int | `4` | Maximum concurrently claimed KoldStore jobs. Clamped to `1..=1024`. |
| `koldstore.log_level` | string | `info` | Extension log verbosity: `error`, `warn`, `info`, `debug`, or `trace`. |
| `koldstore.min_max_rows_per_file` | int | `1000` | Minimum allowed `max_rows_per_file` for `manage_table` and flush. Lower temporarily for tests, for example `SET koldstore.min_max_rows_per_file = 100`. Clamped to `1..=1000000`. |
| `koldstore.flush_check_interval_seconds` | int | `30` | How often the database worker evaluates `auto_flush` tables and runs at most one needed flush. Clamped to `1..=86400`. |
| `koldstore.async_apply_poll_interval_ms` | int | `100` | Latch poll interval for the async mirror apply loop. Clamped to `50..=5000`. Prefer `ALTER DATABASE` / `ALTER SYSTEM` + reload for the bgworker (session `SET` does not affect it). |
| `koldstore.async_apply_max_rows_per_tick` | int | `0` | Max source row changes per apply tick (`0` = unlimited / drain available WAL). |
| `koldstore.async_apply_max_ms_per_tick` | int | `0` | Max wall-clock ms per apply tick (`0` = unlimited). When exhausted, commit `applied_lsn` and continue next wake. |
| `koldstore.flush_prelock_max_passes` | int | `3` | Max phase-5.5 pre-lock async apply passes during flush before failing closed. |
| `koldstore.flush_prelock_max_ms` | int | `5000` | Combined wall-clock budget (ms) for flush phase-5.5 pre-lock catch-up. |
| `koldstore.async_mirror_max_retained_bytes` | int | `1073741824` (1 GiB) | Health threshold for slot-retained WAL bytes. Exceeding it marks `async_mirror_status().retention.ok` false but never blocks the applier from draining WAL. `admission` remains a compatibility alias. `0` disables this health threshold. Configure PostgreSQL retention/disk safeguards independently. |

### Internal GUCs

These are reserved for extension maintenance paths. Application roles cannot set
them.

| GUC | Type | Default | Meaning |
|-----|------|---------|---------|
| `koldstore.internal_system_write` | bool | `off` | Allows internal KoldStore system writes. |
| `koldstore.internal_flush_cleanup` | bool | `off` | Allows pruning flushed hot and mirror rows during flush cleanup. |

## Exposed functions

Every SQL-callable function the extension installs today:

| Function | Returns | Value |
|----------|---------|-------|
| `snowflake_id()` | `bigint` | Generated Snowflake-like id |
| `koldstore_version()` | `text` | Extension version string |
| `koldstore_user_id()` | `text` | Active `koldstore.user_id`, or `NULL` |
| `koldstore.register_storage(...)` | `uuid` | Storage backend id (`koldstore.storage.id`) |
| `koldstore.alter_storage_credentials(...)` | `void` | No value |
| `koldstore.alter_storage_location(...)` | `uuid` | Storage backend id |
| `koldstore.manage_table(...)` | `uuid` | Migration job id (`koldstore.jobs.id`) |
| `koldstore.set_table_auto_flush(...)` | `boolean` | `true` when an active managed table was updated |
| `koldstore.unmanage_table(...)` | `bigint` | Count of deactivated `koldstore.schemas` rows |
| `koldstore.wait_for_async_mirror()` | `bigint` | Async source row changes applied by this fence |
| `koldstore.async_mirror_status()` | `jsonb` | Slot lag, retained bytes, apply rates, health |
| `koldstore.async_mirror_slot_name()` | `text` | Deterministic logical-slot name for the current database |
| `koldstore.disable_async_mirror()` | `boolean` | Whether async publication or slot infrastructure was removed |
| `koldstore.enqueue_flush_job(...)` | `bigint` | `1` if a job was inserted, else `0` |
| `koldstore.flush_table(...)` | `uuid` | Flush job id |
| `koldstore.describe_table(...)` | `jsonb` | Table status object (see below) |
| `koldstore.recover_segments(...)` | `bigint` | Number of orphan recovery actions planned |

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

**Returns:** `uuid` — the storage backend id (`koldstore.storage.id`). Upserts on
`name`, so a re-register of an existing name returns the same id.

### `koldstore.alter_storage_credentials`

```sql
SELECT koldstore.alter_storage_credentials(
  name        => 'local-dev',
  credentials => '{"access_key_id":"...","secret_access_key":"..."}'::jsonb
);
```

Rotates credentials without rewriting existing cold object paths.

**Returns:** `void` — no value. Errors if the storage name does not exist.

### `koldstore.alter_storage_location`

```sql
SELECT koldstore.alter_storage_location(
  name      => 'local-dev',
  base_path => '/var/lib/koldstore',
  config    => '{}'::jsonb
);
```

Updates storage location/configuration without direct catalog DML.

**Returns:** `uuid` — the storage backend id. Errors if the storage name does
not exist.

### `koldstore.manage_table`

```sql
SELECT koldstore.manage_table(
  table_name        => 'chat.messages',
  storage           => 's3_archive',
  hot_row_limit     => 10000,
  min_flush_rows    => 1000,
  max_rows_per_file => 1000,
  target_file_size_mb => 256,
  migration_order_by  => 'created_at',
  mirror_capture_mode => 'strict'
);
```

Registers a heap table for KoldStore management with structured flush settings.
`hot_row_limit` is required in the call (pass `NULL` for hot-only tables).
`table_type` defaults to `shared`; optional `scope_column`,
`migration_order_by`, `compression`, and `target_file_size_mb` arguments are
also available.

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `table_name` | required | Table to manage (`regclass`) |
| `storage` | required | Registered storage backend name |
| `hot_row_limit` | required (`NULL` allowed) | Maximum mirror rows to keep hot; `NULL` for hot-only tables |
| `min_flush_rows` | `1000` | Minimum excess rows required before a flush moves data cold |
| `max_rows_per_file` | `1000` | Maximum rows written into one Parquet segment per flush batch (minimum `1000` unless lowered via `koldstore.min_max_rows_per_file`) |
| `table_type` | `'shared'` | `shared` or `user` |
| `scope_column` | `NULL` | Required when `table_type => 'user'` |
| `migration_order_by` | `NULL` | Optional oldest-to-newest column used for populated-table migration |
| `compression` | `NULL` | Optional Parquet compression name |
| `target_file_size_mb` | `NULL` | Optional target Parquet segment size in MiB; stored for future size-aware flushing |
| `mirror_capture_mode` | `'strict'` | `strict` updates the mirror in the source transaction; `async` applies committed PK-only WAL after source commit |
| `auto_flush` | `true` | When `true`, the built-in database worker may enqueue and run flushes for this table; set `false` to reserve flushes for cron / manual `flush_table` |

**Returns:** `uuid` — the migration job id written to `koldstore.jobs` (empty
tables get a completed migrate job; populated tables run mirror initialization
inline and return that job id).

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

#### Mirror capture modes

`strict` is the default and requires no logical-replication setup. Its
statement triggers update the latest-state mirror in the same transaction as
the heap, providing immediate read-your-writes behavior.

`async` removes mirror writes from foreground DML. It requires
`wal_level=logical`; KoldStore automatically manages the empty
`koldstore_async_mirror` publication and one deterministic logical slot per
database. Applications must tolerate the normal short lag or call
`koldstore.wait_for_async_mirror()` at a required consistency boundary.
`flush_table` invokes the same catch-up path automatically.

The mode is selected when the table is first managed. See
[Mirror capture modes](architecture/mirror-capture-modes.md) for the complete
transaction, rollback, WAL-retention, worker, and cleanup model.

### `koldstore.set_table_auto_flush`

```sql
SELECT koldstore.set_table_auto_flush(
  table_name => 'chat.messages',
  enabled    => false
);
```

Updates `koldstore.schemas.options.auto_flush` for an active managed table.
Manual `flush_table` / `enqueue_flush_job` ignore this flag. See
[Scheduling](operations/scheduling.md).

**Returns:** `boolean` — `true` when an active managed row was updated.

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

**Returns:** `bigint` — number of `koldstore.schemas` rows deactivated for the
table (normally `1` when the table was actively managed, `0` if none were
active).

## Async Mirror Operations

These functions operate on database-scoped infrastructure used by tables
managed with `mirror_capture_mode => 'async'`.

### `koldstore.wait_for_async_mirror`

```sql
SELECT koldstore.wait_for_async_mirror();
```

Applies committed source changes available at the fence boundary and waits
until the mirror has reached that boundary. This provides a strong consistency
point for an async table. The background worker normally performs the same work
without an explicit call, and `flush_table` fences automatically.

**Returns:** `bigint` — the number of source row-change messages applied by
this invocation. A return value of `0` can mean the worker had already caught
up; it does not mean async capture is disabled.

### `koldstore.async_mirror_slot_name`

```sql
SELECT koldstore.async_mirror_slot_name();
```

**Returns:** `text` — the deterministic logical replication slot name for the
current database. The function is read-only and is primarily useful when
monitoring `pg_replication_slots`.

### `koldstore.disable_async_mirror`

```sql
SELECT koldstore.disable_async_mirror();
```

Drops the current database's async logical slot and publication and clears its
apply checkpoint. It refuses cleanup while any actively managed table uses
async capture. Unmanage those tables first. Calling it repeatedly is safe; a
later async `manage_table` recreates compatible infrastructure automatically.

**Returns:** `boolean` — `true` when a slot or publication existed and was
removed, otherwise `false`.

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
`force => true` stores `force` in the job payload so the next `flush_table`
call flushes all pending mirror rows instead of applying the table flush
policy. `force` defaults to `false`. Flush jobs are table-wide; user-scope
partitioning uses the managed table's `scope_column` and session
`koldstore.user_id`, not an enqueue argument.

**Returns:** `bigint` — `1` when a new job was inserted, `0` when an active
flush job already exists.

### `koldstore.flush_table`

```sql
SELECT koldstore.flush_table(
  table_name => 'chat.messages'
);

SELECT koldstore.flush_table(
  table_name => 'chat.messages',
  force      => true
);
```

Ensures a flush job exists and runs the current flush path synchronously.
Progress is visible in `koldstore.jobs` and `koldstore.describe_table(...)`.

`force` defaults to `false`. Row selection behavior:

- If `force => true` is passed, or the pending/running job payload has
  `force = true`, all pending mirror rows are flushed.
- Otherwise, when a table flush policy is configured, only policy-selected rows
  are flushed. Tables managed through `manage_table` honor `hot_row_limit`,
  `min_flush_rows`, and `max_rows_per_file`.
- When no flush policy is configured, all pending mirror rows are flushed.

`enqueue_flush_job(..., force => true)` remains available when enqueueing and
executing the flush are intentionally separate operations.

**Returns:** `uuid` — the flush job id (`koldstore.jobs.id`).

### `koldstore.describe_table`

```sql
SELECT koldstore.describe_table(
  table_name => 'chat.messages'
);

SELECT jsonb_pretty(koldstore.describe_table(table_name => 'chat.messages'));
```

**Returns:** `jsonb` — managed-table storage, mirror, cold-segment, size,
manifest, and recent job state. Counters are table-wide across scopes. Errors
if the table is not actively managed.

Sample result after a small flush:

```json
{
  "jobs": [
    {
      "id": "e30eb374-a9db-4ff1-97d3-72f8511dfc60",
      "phase": "finished",
      "status": "completed",
      "job_type": "flush",
      "updated_at": "2026-07-07T16:56:10.123456+03:00",
      "rows_flushed": 12,
      "checkpoint_seq": 332882280212668416,
      "rows_processed": 12,
      "checkpoint_commit_seq": 332882280212668416
    },
    {
      "id": "2c2bcf44-d6ea-4b3e-b62c-cfaf18ad5225",
      "phase": "finished",
      "status": "completed",
      "job_type": "migrate_backfill",
      "updated_at": "2026-07-07T16:56:09.987654+03:00",
      "rows_flushed": 0,
      "checkpoint_seq": 0,
      "rows_processed": 1012,
      "checkpoint_commit_seq": 0
    }
  ],
  "hot_rows": 1000,
  "mirror_rows": 1000,
  "cold_row_count": 12,
  "cold_segment_count": 1,
  "heap_size_bytes": 442368,
  "table_size_bytes": 606208,
  "index_size_bytes": 16384,
  "manifest_state": "in_sync",
  "manifest_max_seq": 332882280212668416,
  "pending_jobs": 0,
  "storage_binding": "4a3b2ab3-5ea8-4761-9e37-1a2f98b128e4",
  "last_error": null
}
```

Top-level fields:

| Field | Type | Meaning |
| ----- | ---- | ------- |
| `hot_rows` | `bigint` | Rows still present in the PostgreSQL heap |
| `mirror_rows` | `bigint` | Primary keys tracked in the `__cl` mirror |
| `cold_row_count` | `bigint` | Rows already copied to active cold segments |
| `cold_segment_count` | `bigint` | Active Parquet segment count |
| `heap_size_bytes` | `bigint` | `pg_relation_size(table)` — main heap fork only |
| `table_size_bytes` | `bigint` | `pg_table_size(table)` — heap + TOAST + FSM/VM, **excluding indexes** |
| `index_size_bytes` | `bigint` | `pg_indexes_size(table)` — all indexes on the table |
| `manifest_state` | `text` | Catalog/manifest sync state; `in_sync` means they agree |
| `manifest_max_seq` | `bigint` | Highest mirror `seq` represented in cold data |
| `pending_jobs` | `bigint` | Jobs for this table with status `pending` or `running` |
| `jobs` | `jsonb` | Up to 20 recent jobs, newest first (see below) |
| `storage_binding` | `text` | Bound storage backend id as text |
| `last_error` | `text` | Last manifest or storage error, or `null` |

Each element of `jobs`:

| Field | Type | Meaning |
| ----- | ---- | ------- |
| `id` | `text` | Job uuid |
| `job_type` | `text` | e.g. `flush`, `migrate_backfill` |
| `status` | `text` | Job status (`pending`, `running`, `completed`, …) |
| `phase` | `text` | Current or final phase |
| `rows_processed` | `bigint` | Rows processed by the job |
| `rows_flushed` | `bigint` | Rows written cold by the job |
| `checkpoint_seq` | `bigint` | Mirror `seq` checkpoint |
| `checkpoint_commit_seq` | `bigint` | Commit-seq checkpoint |
| `updated_at` | `timestamptz` | Last job update time |

Size notes:

- `heap_size_bytes` + `index_size_bytes` is **not** the same as
  `pg_total_relation_size(table)` (that also includes TOAST).
- `table_size_bytes` excludes indexes; use
  `table_size_bytes + index_size_bytes` for a closer total, or call
  `pg_total_relation_size` directly when you need PostgreSQL’s total.
- Percent-saved figures require a caller-held baseline; `describe_table` does
  not store pre-flush sizes.

For job-level progress, inspect `koldstore.jobs`:

```sql
SELECT job_type, status, phase, rows_processed, rows_flushed, error_trace
FROM koldstore.jobs
WHERE table_oid = 'chat.messages'::regclass
ORDER BY created_at DESC
LIMIT 5;
```

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

Discovers orphan cold objects under the table prefix that are not referenced by
the current object-store manifest **or** by active `koldstore.cold_segments`
rows, plans recovery actions for them, and applies the plan unless
`dry_run => true`. `dry_run` defaults to `false`. Catalog-referenced segments
are preserved so crash-before-manifest-publish recovery does not delete Parquet
that merge scan still needs.

**Returns:** `bigint` — number of recovery actions planned (orphan objects
found). With `dry_run => true`, the count is still returned and no objects are
changed.

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
