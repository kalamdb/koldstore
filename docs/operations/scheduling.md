# Scheduling flushes

For common shared/strict tables, configure scheduling directly:

```sql
ALTER TABLE events SET (
  koldstore_enabled = true,
  koldstore_storage = 'cold_s3',
  koldstore_move_after = '7 days',
  koldstore_min_flush_rows = 1000,
  koldstore_max_rows_per_flush = 10000
);
```

Age scheduling uses a bounded probe on the indexed mirror `seq`, never an
application-table scan. Strict-mode age begins near statement execution;
async-mode age begins when WAL is applied. Updating a row gives it a newer
sequence and restarts inactivity. `seq` encodes age, not commit order.

KoldStore includes a built-in flush scheduler on the per-database background
worker (the same process that applies async mirror WAL). On each
`koldstore.flush_check_interval_seconds` tick it:

1. Applies available async mirror WAL first (when an async slot exists)
2. Evaluates active managed tables with `auto_flush` enabled
3. When `hot_row_limit` / `min_flush_rows` say a flush is needed, runs one
   `flush_table` (which ensures/claims the job inline)

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

### Async apply poll interval

The same worker peeks the logical slot on a latch cadence controlled by
`koldstore.async_apply_poll_interval_ms` (default `100`, clamped to
`50..=5000`). Each apply tick runs in **one** PostgreSQL transaction: mirror
batch writes and `async_mirror_state.applied_lsn` commit together (or roll back
together on ERROR).

Idle path: when the cluster insert LSN is still at or behind the slot's
`confirmed_flush`, the worker skips decode entirely (shared-memory check, no
SPI). After an empty peek it advances `confirmed_flush` past non-publication
WAL and backs off the latch up to 5 seconds so retained-WAL gaps cannot burn
CPU on every checkpoint.

```sql
-- Per-database (preferred for the bgworker):
ALTER DATABASE mydb SET koldstore.async_apply_poll_interval_ms = 50;
-- Restart the database worker (or terminate + ensure) so it reconnects with
-- the new database default. SIGHUP also reloads ALTER SYSTEM values.

-- Or persist cluster-wide:
ALTER SYSTEM SET koldstore.async_apply_poll_interval_ms = 200;
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
