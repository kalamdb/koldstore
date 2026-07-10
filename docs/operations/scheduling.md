# Scheduling flushes with pg_cron

Flush is on-demand today. Until the built-in smart scheduler lands, use
[pg_cron](https://github.com/citusdata/pg_cron) to call `flush_table` on a
schedule. Policy-aware flushes are safe to run often: when nothing is eligible,
the job completes with `rows_flushed = 0`.

## Enable pg_cron

Install and enable `pg_cron` (requires `shared_preload_libraries = 'pg_cron'`
and a restart), then schedule a table:

```sql
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Flush one managed table every 5 minutes.
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

Inspect or remove jobs with `cron.job` / `cron.unschedule(...)`. Pick an
interval that matches how fast the hot set grows; `min_flush_rows` still gates
whether a flush writes cold segments.

## Smoke-test against local pgrx

```bash
scripts/run-test-with-cron.sh
scripts/run-test-with-cron.sh --pg-version 16
scripts/run-test-with-cron.sh --skip-prepare   # reuse an already-prepared DB
```

This is intentionally outside the default E2E/CI loop because `pg_cron` needs
`shared_preload_libraries` and a short wait for the scheduler. See
[development](../development.md) for more local setup notes.

Published Docker release images already include `pg_cron` with
`shared_preload_libraries` configured.
