# Fintech audit events example

Immutable append-only audit history with long retention. Safe for audit/login/risk events — not account balances in v1.

## What the test covers

- Multi-tenant parallel audit ingest
- Multi-wave flush + forced flush
- Segment proof metadata (`row_count`, `byte_size`) and manifest generations
- Account/event indexes
- Concurrent hot DML (including delete tombstones)
- Cold-then-delete overlay after flush (rematerialize → DELETE → tombstone flush)
- Long-range regulator export over cold Parquet
