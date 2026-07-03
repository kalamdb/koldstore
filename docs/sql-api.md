# SQL API

## Session

- `SNOWFLAKE_ID()` allocates a monotonic row sequence.
- `koldstore_version()` returns the extension version.
- `koldstore_user_id()` reads the active user-scope GUC.

## Storage and Migration

- `koldstore.register_storage(...)` creates or updates a storage binding.
- `koldstore.alter_storage_credentials(...)` rotates credentials without
  rewriting existing cold object paths.
- `koldstore.migrate_table(...)` validates a heap table, preserves its primary
  key, adds system columns, and registers managed metadata.
- `koldstore.demigrate_table(...)` disables management after rehydration or
  archive-detach mode.

## Flush and Cold Data

- `koldstore.set_flush_policy(...)` records table flush policy.
- `koldstore.flush_table(...)` queues one flush job.
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
  `koldstore.recover_segments(...)` provide the operator API.
- `koldstore_exec('EXPORT TABLE ...')` is the export boundary. `IMPORT TABLE`
  is rejected until ownership and conflict rules are complete.

## Security

User-scoped tables require `koldstore.user_id` and fail closed when it is
missing. RLS/security qualifiers must be enforceable on cold rows or planning
must fail closed.
