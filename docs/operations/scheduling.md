# Scheduling flushes

KoldStore includes a built-in flush scheduler on the per-database background
worker (the same process that applies async mirror WAL). On each
`koldstore.flush_check_interval_seconds` tick it:

1. Applies available async mirror WAL first (when an async slot exists)
2. Evaluates active managed tables with `auto_flush` enabled
3. When `hot_row_limit` / `min_flush_rows` say a flush is needed, runs one
   `flush_table` (which ensures/claims the job inline)

## Built-in scheduler

Requires `shared_preload_libraries = 'koldstore'` for the cluster launcher to
re-register workers after postmaster restart. Without preload, `manage_table`
and the first backend that needs the worker still start it for the life of the
postmaster.

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

`flush_table` / `enqueue_flush_job` ignore `auto_flush` — opt-out is
scheduler-only.

## pg_cron fallback

Use [pg_cron](https://github.com/citusdata/pg_cron) for `auto_flush => false`
tables, or when the built-in worker is unavailable (no preload and no session
ensure yet). Policy-aware flushes are safe to run often: when nothing is
eligible, the job completes with `rows_flushed = 0`.

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
