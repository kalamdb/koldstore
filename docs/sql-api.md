# SQL API

## Session

- `SNOWFLAKE_ID()` allocates a monotonic row sequence.
- `koldstore_version()` returns the extension version.
- `koldstore_user_id()` reads the active user-scope GUC.

## Configuration

Runtime settings use the `koldstore.` GUC prefix:

```sql
SET koldstore.cold_reads = 'auto';
SET koldstore.max_open_parquet_readers = 32;
SET koldstore.max_running_jobs = 4;
SET koldstore.log_level = 'info';
```

Use PostgreSQL-native persistence for durable configuration, for example
`ALTER SYSTEM SET`, `ALTER DATABASE ... SET`, or `ALTER ROLE ... SET`, followed
by the normal PostgreSQL reload rules for the chosen scope.

- `koldstore.cold_reads`: `auto`, `on`, or `off`. `off` fails cold scans when
  active cold segments are required.
- `koldstore.max_open_parquet_readers`: global advisory-lock slot count for
  Parquet readers opened by cold scans. Default `32`.
- `koldstore.max_running_jobs`: maximum concurrently claimed KoldStore jobs.
  Default `4`.
- `koldstore.log_level`: extension log verbosity. Default `info`.

## Storage and Migration

- `koldstore.register_storage(...)` creates or updates a storage binding.
- `koldstore.alter_storage_credentials(...)` rotates credentials without
  rewriting existing cold object paths.
- `koldstore.migrate_table(...)` validates a heap table, preserves its primary
  key, creates the change-log mirror, records a `migrate_backfill` job, and
  returns that job id. Existing populated tables need a single auto-increment
  primary key or the overload that supplies an explicit order column.
- `koldstore.demigrate_table(...)` disables management after rehydration or
  archive-detach mode.

## Flush and Cold Data

- `koldstore.set_flush_policy(...)` records table flush policy.
- `koldstore.flush_table(...)` records one flush job, runs the current flush
  path, and returns the job id. Progress is visible in `koldstore.jobs` and
  `koldstore.table_status(...)`.
- `koldstore.flush_pending()` queues all pending manifest scopes.

## DML Boundaries

- Normal hot `INSERT`, `UPDATE`, and `DELETE` operate on the heap and mark local
  manifest state pending.
- `koldstore.hydrate_pk(...)`, `koldstore.update_row(...)`, and
  `koldstore.delete_row(...)` are explicit cold-only DML APIs.
- Standard SQL cold-only `UPDATE` affects zero rows in the MVP.

## Changes and Operations

- `koldstore.changes_since(...)` streams row events ordered by `_commit_seq`.
- `koldstore.table_status(...)`, `koldstore.backup_manifest(...)`,
  `koldstore.validate_cold_storage(...)`, and
  `koldstore.recover_segments(...)` provide the operator API. `table_status`
  includes recent job ids, phases, statuses, and progress counters.
- `koldstore_exec('EXPORT TABLE ...')` is the export boundary. `IMPORT TABLE`
  is rejected until ownership and conflict rules are complete.

## Security

User-scoped tables require `koldstore.user_id` and fail closed when it is
missing. RLS/security qualifiers must be enforceable on cold rows or planning
must fail closed.
