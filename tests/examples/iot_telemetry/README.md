# IoT telemetry example

Tenant-scoped sensor events ordered by `ts`, with late-arriving uploads and dashboard/report queries.

## What the test covers

- Parallel device writers across tenants
- Late-arriving historical `ts` inserts
- Multi-wave flush and small Parquet batching
- Device/event indexes
- Concurrent hot DML under live ingest
- Cold-then-delete overlay for flushed sensor rows (rematerialize → DELETE → tombstone flush)
- Monthly aggregate cold reads
