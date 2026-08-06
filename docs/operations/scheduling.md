# Scheduling flushes

KoldStore moves excess hot rows to cold Parquet through durable flush jobs in
`koldstore.jobs`. Production default is `koldstore.flush_execution = queue`:
something enqueues a job, then a one-shot flush executor claims and runs it.
Callers do not wait for Parquet encode, upload, or prune.

You choose **when a job is enqueued**. Execution is always policy-based
(`hot_row_limit` / `min_flush_rows` / `max_rows_per_file` from manage-time
options). Starting a job and selecting rows are the same path —
`koldstore.flush_table` is the public “enqueue and start” entry point.

## How auto-flush decides to enqueue

This is **not** PostgreSQL autovacuum.

| | Autovacuum | KoldStore auto-flush |
|---|---|---|
| Trigger | Dead tuples, freeze horizons, wraparound risk | Mirror / hot-row policy on managed tables |
| Cadence | Autovacuum launcher + per-table thresholds | `koldstore.flush_check_interval_seconds` on the database worker |
| Work | VACUUM / ANALYZE heap | Enqueue a flush job → Parquet + prune |

Conceptually both are background maintenance when a threshold is crossed, but
they use different metrics and workers. Autovacuum never enqueues KoldStore
flush jobs, and KoldStore does not hook the autovacuum launcher.

On each `flush_check_interval_seconds` tick the database worker:

1. Applies available async mirror WAL first (when an async slot exists)
2. Evaluates active managed tables with `auto_flush` enabled
3. When `hot_row_limit` / `min_flush_rows` say a flush is due **and** the
   selected row count is at least `max_rows_per_file`, enqueues at most one
   flush job and spawns flush executors up to
   `koldstore.max_parallel_flush_jobs`. Undersized selections (for example 450
   excess with `max_rows_per_file = 1000`) are skipped — no job row is created.

## Built-in scheduler

Requires `shared_preload_libraries = 'koldstore'` so merge-scan hooks and the
cluster launcher exist in every backend after postmaster restart. Shared preload
is mandatory for correctness (not only for scheduling).

```sql
-- Per-database (preferred for the bgworker — new backends inherit this):
ALTER DATABASE mydb SET koldstore.flush_check_interval_seconds = 5;
-- Then restart the database worker (or wait for a new ensure after terminate).

-- Or persist cluster-wide:
ALTER SYSTEM SET koldstore.flush_check_interval_seconds = 60;
SELECT pg_reload_conf();
```

Session `SET` only affects the current backend. The built-in worker reads GUCs
from its own connection (database / system defaults), so use `ALTER DATABASE`
or `ALTER SYSTEM` when changing scheduler cadence for background flushes.

### Async apply commit wakeups and watchdog

Managed-table commits advance a database-scoped shared generation and set the
worker latch. Concurrent commits coalesce: the worker drains through the latest
generation instead of queueing one job per transaction. Each apply tick runs in
**one** PostgreSQL transaction: mirror batch writes and
`async_mirror_state.applied_lsn` commit together (or roll back together on
ERROR).

The worker does not periodically decode on a short poll interval. A safety
watchdog controlled by `koldstore.async_apply_watchdog_interval_ms` (default
`30000`, clamped to `1000..=300000`) catches a lost notification or a two-phase
commit that cannot carry the originating backend's in-memory hint.

```sql
-- Per-database (preferred for the bgworker):
ALTER DATABASE mydb SET koldstore.async_apply_watchdog_interval_ms = 30000;
-- Restart the database worker (or terminate + ensure) so it reconnects with
-- the new database default. SIGHUP also reloads ALTER SYSTEM values.

-- Or persist cluster-wide:
ALTER SYSTEM SET koldstore.async_apply_watchdog_interval_ms = 30000;
SELECT pg_reload_conf();
```

Session `SET` does not affect the background worker. Prefer `ALTER DATABASE`
or `ALTER SYSTEM` + reload / worker restart, matching
`flush_check_interval_seconds`.

### Async retained-WAL health threshold

`koldstore.async_mirror_max_retained_bytes` defaults to **1 GiB**. When the
logical slot’s retained WAL (`pg_wal_lsn_diff(current, confirmed_flush_lsn)`)
exceeds the threshold, `koldstore.async_mirror_status()` becomes unhealthy and
operators should alert. The applier keeps draining: stopping it when WAL is
already high makes the incident worse.

```sql
-- Raise the health threshold for expected catch-up windows:
ALTER SYSTEM SET koldstore.async_mirror_max_retained_bytes = 2147483647; -- ~2 GiB cap
SELECT pg_reload_conf();

-- Disable only this health alarm (monitor pg_wal yourself):
ALTER DATABASE mydb SET koldstore.async_mirror_max_retained_bytes = 0;
```

Use PostgreSQL disk monitoring and a deliberate `max_slot_wal_keep_size` policy
as independent hard safeguards. Reaching PostgreSQL's slot retention limit may
invalidate the logical slot and require mirror rebuild; it is not a normal
backpressure mechanism.

After a failed auto-flush (for example `max_rows_per_file` below the
`koldstore.min_max_rows_per_file` floor), that table is skipped for 60 seconds
so one bad table cannot monopolize every tick.

### Per-table opt-out

Tables that should only flush via cron or manual SQL:

```sql
SELECT koldstore.manage_table(
  table_name => 'app.messages',
  storage => 'local',
  hot_row_limit => 1000,
  auto_flush => false
);

-- Or flip later without remanaging:
SELECT koldstore.set_table_auto_flush('app.messages'::regclass, false);
```

`flush_table` ignores `auto_flush` — opt-out is scheduler-only.

## Manual start (queue job)

```sql
SELECT koldstore.flush_table(table_name => 'app.messages') AS flush_job_id;
```

With `flush_execution = queue` (default):

1. Inserts a pending flush job if none is already active for the table, or
   returns the existing active job UUID
2. Spawns a flush executor when capacity allows
3. Returns the job UUID immediately — poll `koldstore.jobs` /
   `koldstore.list_jobs` for progress

Returns `NULL` when nothing is due (including when excess is below
`max_rows_per_file`). `enqueue_flush_job` inserts or returns the same durable
job UUID but does **not** spawn executors; prefer `flush_table` when you want
work to start.

Row selection always follows the table flush policy (oldest excess by mirror
`seq`). Policy-aware flushes wait until they can fill at least one
`max_rows_per_file` segment.

## pg_cron fallback

Use [pg_cron](https://github.com/citusdata/pg_cron) for `auto_flush => false`
tables, or when you want wall-clock schedules (for example every five minutes)
instead of the built-in check interval. Policy-aware flushes are safe to run
often: when nothing is eligible, `flush_table` returns `NULL` and creates no job.

```sql
CREATE EXTENSION IF NOT EXISTS pg_cron;

SELECT cron.schedule(
  'koldstore-flush-messages',
  '*/5 * * * *',
  $$SELECT koldstore.flush_table(table_name => 'app.messages')$$
);
```

To flush every active managed table:

```sql
SELECT cron.schedule(
  'koldstore-flush-all',
  '*/5 * * * *',
  $$
  SELECT koldstore.flush_table(table_name => s.table_oid)
  FROM koldstore.schemas s
  WHERE s.active
  $$
);
```

Inspect or remove jobs with `cron.job` / `cron.unschedule(...)`.

## Smoke-test against local pgrx

```bash
scripts/readiness/run-test-with-cron.sh
scripts/readiness/run-test-with-cron.sh --pg-version 16
scripts/readiness/run-test-with-cron.sh --skip-prepare   # reuse an already-prepared DB
```

This is intentionally outside the default E2E/CI loop because `pg_cron` needs
`shared_preload_libraries` and a short wait for the scheduler. See
[development](../development.md) for more local setup notes.

Published Docker release images already include `pg_cron` with
`shared_preload_libraries` configured.
