# Existing Table Migration and Flush Architecture

## Status

Accepted for the current pg-koldstore implementation.

## Context

The previous existing-table migration path added pg-koldstore system columns and
then ran one table-wide `UPDATE` to fill `_seq`, `_commit_seq`, and `_deleted`.
That was easy to reason about for small tables, but it was not appropriate for a
table with 100k rows or more:

- it could rewrite many heap rows in the caller session;
- it did not choose a stable oldest-to-newest ordering;
- it did not expose progress as a resumable job;
- it had no batch checkpoint that a restarted server could continue from;
- it risked making `_seq` assignment depend on heap order rather than business
  age.

The new path makes existing-table migration an asynchronous, durable job that is
visible in `koldstore.jobs`.

## Ordering Rule

Existing rows must be backfilled from oldest to newest before the table becomes
active for periodic flushing.

The planner chooses the order this way:

1. If the user provides `order_column`, use it when it exists and has an
   orderable type such as integer, bigint, timestamp, timestamptz, or date.
2. Otherwise, use the primary key only when it is a single auto-increment key
   detected from PostgreSQL identity metadata or a `nextval(...)` default.
3. Otherwise, `koldstore.migrate_table` rejects the migration with:
   `existing table migration requires an auto-increment primary key or explicit order column`.

Composite primary keys and UUID primary keys need an explicit order column.
This keeps age semantics explicit instead of pretending a non-age identifier is
safe.

## Migration Flow

For a populated table, `koldstore.migrate_table` records durable job progress
while initializing the clean-schema mirror. The current SQL wrapper executes the
initialization path inline and returns the job id; the durable job model keeps
the handoff compatible with a future background worker.

1. Resolve table oid, storage id, primary key, and catalog columns.
2. Choose the migration ordering.
3. Create the table-specific clean-schema `__cl` mirror.
4. Register a schema row as inactive with `migration_status =
   mirror_initializing`.
5. Enqueue one `migrate_backfill` job in `koldstore.jobs` with a typed JSON
   payload.
6. The migration path backfills bounded batches ordered by the selected column:
   `ORDER BY <order_column> ASC, ctid ASC LIMIT $1 FOR UPDATE SKIP LOCKED`.
7. Update job progress and mark the job `completed`.
8. Mark the schema active. Later calls to `koldstore.flush_table` record a
   `flush` job and write mirror rows to cold storage.

## Job Model

`koldstore.jobs` is the durable queue. It carries:

- `job_type`: `migrate_backfill`, `flush`, and future job types.
- `status`: `pending`, `running`, `completed`, `cancelled`, or `error`.
- `phase`: durable phase such as `initialize_mirror`, `writing`, or `finished`.
- `lease_owner`, `lease_expires_at`, `lease_epoch`: worker lease fencing.
- `checkpoint_seq`, `checkpoint_commit_seq`: durable cursors.
- `batches_completed`, `rows_processed`, `rows_flushed`: operator progress.
- `flush_seq_upper_bound`: the flush watermark.
- `payload`: typed JSON payload with table name, order column, batch size,
  storage id, scope column, and flush policy.

There is a partial unique index for one active migration per table, a partial
unique index for one active flush per table/scope, and a table-wide partial
unique index that prevents active flush and migration work from overlapping on
the same table. The SQL entrypoints also take a transaction-scoped advisory lock
before inline flush or migration work begins.

Claimable jobs are indexed by job type, status, run time, priority, and id so
thousands of pending jobs can be claimed without a global lock. Claim plans use
`FOR UPDATE SKIP LOCKED`, skip tables that already have an unexpired running
flush or migration job, and stop claiming while running jobs are at
`koldstore.max_running_jobs`.

## Crash Safety

Each phase is safe to retry.

- If the server crashes after adding columns, the next job run sees the columns
  already exist.
- If it crashes after setting defaults, future writes still receive defaults.
- If it crashes after a backfill batch commits but before progress is updated,
  the next run skips those rows because `_seq IS NOT NULL`.
- If it crashes before a batch commits, PostgreSQL rolls the batch back and the
  next worker retries it.
- If a worker lease expires, another worker can claim the job and increments
  `lease_epoch`; stale progress updates are fenced by `lease_owner` and
  `lease_epoch`.
- Heap cleanup during flush happens only after cold persistence and manifest
  publication, and only when the current hot row still matches the flushed
  `_seq` and `_commit_seq`.

## Flush Watermark

The migration handoff enqueues a regular `flush` job with an inclusive
`flush_seq_upper_bound`.

Backfilled old rows receive values from `koldstore.global_seq`, while future
writes use the Snowflake `_seq` default. That makes the migration flush target
only old backfilled rows. Rows inserted, updated, or deleted after the
watermark are skipped by the flush job and remain hot until a later periodic
flush.

## System Column Semantics (`_seq` vs `_commit_seq`)

pg-koldstore uses two different bigint watermarks:

| Column | Source | Purpose |
|--------|--------|---------|
| `_seq` | `koldstore.global_seq` during backfill; `SNOWFLAKE_ID()` for new rows after migration | Monotonic row version / effect id for tie-breaks and diagnostics |
| `_commit_seq` | `koldstore.global_commit_seq` for all managed writes | Durable commit-order watermark used by `changes_since` and flush pruning |

This split is intentional.

- **Backfilled rows** must receive ordered, deterministic `_seq` values from
  `koldstore.global_seq` so migration can walk oldest-to-newest safely and set
  an inclusive `flush_seq_upper_bound` without depending on heap order.
- **New rows after migration** receive Snowflake `_seq` values from the column
  default (`SNOWFLAKE_ID()`), giving globally unique version ids for concurrent
  writers.
- **`_commit_seq` is never a Snowflake id.** It always comes from
  `koldstore.global_commit_seq` and represents commit order, not row identity.

### What this means in `manifest.json`

A migrated table that later receives hot DML can therefore show **mixed**
`_seq` ranges in segment metadata:

- Small values (for example `26938`–`27760`) are backfilled rows from
  `global_seq`.
- Large values (for example `332241131154739200`) are Snowflake ids assigned to
  rows inserted after migration.
- `min_commit_seq` / `max_commit_seq` stay on the smaller `global_commit_seq`
  scale because commit order never uses Snowflake.

Top-level `max_seq` and `max_commit_seq` are the maxima across committed
segments. After hot inserts, `max_seq` may jump to a Snowflake value while
`max_commit_seq` remains on the commit-sequence scale.

Greenfield tables created with `SNOWFLAKE_ID()` defaults never go through
`global_seq` backfill, so their cold segments typically show only Snowflake
`_seq` values.

## Manifest Shape on Flush

Object-store `manifest.json` follows the kalamdb-compatible contract:

- **Segment `path`** is relative to the manifest directory, for example
  `batch-1.parquet`, not `lifecycle/full_lifecycle_pg16/batch-1.parquet`.
  The catalog `koldstore.cold_segments.object_path` keeps the storage-root
  relative path (`{namespace}/{table}/batch-N.parquet`) for internal lookups.
- **`temp_path`** is omitted on committed segments. It is only used during the
  publish workflow when a writer stages objects under `.tmp/...` before the
  manifest commit makes them visible.
- **`publish`** is omitted until a backend-specific publish identity exists.
  The stub flush path writes the manifest directly; full object-store publish
  will populate this block later.
- **`checksum`** is set to `sha256:<hex>` for each flushed Parquet object.
  Unset checksums are not serialized.
- **`files.total_files`** counts only Kalamdb `FILE` datatype objects, not
  Parquet segments. Parquet batch files live under `segments`; `files` stays at
  `0` until FILE uploads are implemented.

## User-Scoped Tables

Shared tables enqueue one initial flush job with an empty scope key. User-scoped
tables enqueue one initial flush per distinct scope value among rows below the
migration flush watermark.

## Operational Status

Operators can inspect migration progress directly:

```sql
SELECT id, table_oid, job_type, status, phase, rows_processed,
       batches_completed, attempts, error_trace, payload
FROM koldstore.jobs
WHERE job_type = 'migrate_backfill'
ORDER BY updated_at DESC;
```

`koldstore.table_status` also counts pending and running jobs for the table.

## Future Extensions

The job model leaves room for:

- separate worker pools by job type;
- adaptive batch sizing based on observed row width;
- per-table migration priorities;
- cancellation and pause/resume controls;
- richer payload versions without changing the core queue table.
