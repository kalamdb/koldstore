# Flush Concurrent / Complex / Multi-Table E2E Design

Date: 2026-07-16

## Goals

Extend `tests/e2e/flush` with real-world-style coverage:

1. **Firehose load** — 10 concurrent connections each rotate INSERT / UPDATE / DELETE / SELECT while `flush_table` runs.
2. **Barrier-synchronized overlap** — same 10 mixed workers while flush is paused at `wait:after_select_rows`.
3. **Complex (rich) types** — flush a typed table (jsonb, uuid, text[], numeric, timestamptz + nullables) and query after prune.
4. **Parallel multi-table flush** — several managed tables flushed concurrently via spawned `flush_table` calls, with light DML allowed during the storm.

## Non-goals

- Full baseline equality under 10-writer chaos (prefer PK uniqueness, no stuck jobs, cold metadata, successful queries).
- MinIO variants of these scenarios.
- Product code changes unless a test exposes a real bug.

## Layout

| File | Role |
|------|------|
| `tests/e2e/flush/harness.rs` | Peer connect, barrier lock, mixed workers, rich-types DDL |
| `tests/e2e/flush/flush_concurrent_load.rs` | Firehose: workers run for the full flush duration |
| `tests/e2e/flush/flush_concurrent_barrier.rs` | Failpoint wait + 10 mixed workers mid-flush |
| `tests/e2e/flush/flush_complex_and_multi.rs` | Rich-types flush + parallel multi-table flush |

## Worker model

- 10 peers; each `worker_id` owns a disjoint insert id band: `1_000_000 + worker_id * 10_000 + seq`.
- Per iteration: INSERT → UPDATE (own band / seed) → DELETE (own recent insert) → SELECT by PK + `COUNT(*)`.
- Firehose: `AtomicBool` stop flag set after flush returns so workers overlap the entire flush.
- Barrier: hold advisory lock `0x4B4F_4C44`, arm `wait:after_select_rows`, run workers, unlock, join flush.

## Assertions

- Flush job(s) succeed (multi-table: all relations).
- `assert_no_active_jobs` + cold metadata present per flushed table.
- `assert_pk_unique` after concurrent load.
- Queries succeed; rich-types values roundtrip after prune.
- Concurrent paths do **not** require full hot prune of the original seed count (new DML may remain hot).

## Runtime

- Local pgrx only (`scenario_pg_matrix()`).
- Filesystem storage; no Docker.
- `#[tokio::test(flavor = "multi_thread", ...)]` for concurrent cases.
- Rich-types fixture avoids `text[]` and `numeric` (flush SPI cannot coerce those
  OIDs to `String` today). Uses jsonb (incl. array-shaped JSON), uuid, float8,
  timestamptz, boolean, and nullables.
- Parallel multi-table flush keeps concurrent DML on a separate traffic table so
  selection/write row-count mismatches are not induced on the tables under flush.
- Failpoint `wait:` uses `Spi::run_with_args` for `pg_advisory_lock` (void return).

