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

For a populated table, `koldstore.migrate_table` now plans and enqueues work
instead of walking every row synchronously.

1. Resolve table oid, storage id, primary key, and catalog columns.
2. Choose the migration ordering.
3. Add system columns as nullable columns:
   `_seq bigint`, `_commit_seq bigint`, `_deleted boolean`.
4. Set defaults for future writes:
   `_seq DEFAULT SNOWFLAKE_ID()`, `_commit_seq DEFAULT nextval(...)`,
   `_deleted DEFAULT false`.
5. Register a schema row as inactive with `migration_status =
   backfill_pending`.
6. Enqueue one `migrate_backfill` job in `koldstore.jobs` with a typed JSON
   payload.
7. A migration worker claims the job with `FOR UPDATE SKIP LOCKED` and a lease.
8. The worker backfills bounded batches ordered by the selected column:
   `ORDER BY <order_column> ASC, ctid ASC LIMIT $1 FOR UPDATE SKIP LOCKED`.
9. The worker updates job progress after each committed batch.
10. When all old rows have `_seq`, finalize system columns as `NOT NULL`.
11. Mark the schema active and enqueue a regular `flush` job with
    `flush_seq_upper_bound` set to the max backfilled `_seq`.
12. The normal flush worker flushes only rows at or below that sequence
    watermark, then periodic flush policy takes over.

## Job Model

`koldstore.jobs` is the durable queue. It carries:

- `job_type`: `migrate_backfill`, `flush`, and future job types.
- `status`: `pending`, `running`, `completed`, `cancelled`, or `error`.
- `phase`: durable phase such as `add_system_columns`, `backfill_seq`, or
  `finished`.
- `lease_owner`, `lease_expires_at`, `lease_epoch`: worker lease fencing.
- `checkpoint_seq`, `checkpoint_commit_seq`: durable cursors.
- `batches_completed`, `rows_processed`, `rows_flushed`: operator progress.
- `flush_seq_upper_bound`: the flush watermark.
- `payload`: typed JSON payload with table name, order column, batch size,
  storage id, scope column, and flush policy.

There is a partial unique index for one active migration per table and a
separate partial unique index for one active flush per table/scope. Claimable
jobs are indexed by job type, status, run time, priority, and id so thousands
of pending jobs can be claimed without a global lock.

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
