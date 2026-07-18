# Benchmarks

KoldStore is a **storage lifecycle tool**, not a universal query accelerator.
These docs explain what the storage comparison harness measures when older rows
leave the PostgreSQL heap for Parquet while applications keep querying the same
table.

**Latest numbers:** [RESULTS.md](RESULTS.md) — columns are PostgreSQL only,
PG + KoldStore (async), and PG + KoldStore (strict). Refresh with
`scripts/run-storage-comparison.sh --all-sides --update-results` (each column
gets a fresh pgrx PostgreSQL: stop → recreate DBs → measure one side alone).

## Documents in this folder

| Doc | Focus |
| --- | --- |
| [README](README.md) (this page) | How to read results + reproduce |
| [RESULTS](RESULTS.md) | Latest published comparison tables only |
| [HammerDB / TPROC-C](hammerdb.md) | Selective-manage OLTP stress: baseline vs HISTORY-only manage |

## Storage comparison

Harness: [`tests/storage/`](../../tests/storage/) with a wide (~50 column) table
from [`tests/storage/schema.sql`](../../tests/storage/schema.sql).

Typical published scale: **10,000,000 rows**, `hot_row_limit = 100000`,
`max_rows_per_file = 1000000`, `--dml-sample 50000` (~9.9M rows flushed, zstd
Parquet). Published RESULTS use `--all-sides`: exactly **three sequential** runs
(pg → async → strict), each alone on a fresh pgrx PostgreSQL (stop → recreate
DBs → measure that side once). They are **not** parallel and do **not** share
a live server or dual-table I/O during measurement. Inserts use committed
100k-row batches. Numbers vary by machine; re-run for your hardware. See
[Mirror capture modes](../architecture/mirror-capture-modes.md).

**Managed PostgreSQL sizes always include** the hot user heap **plus**
`koldstore.<table>__cl` (latest-state change-log mirror) **and** that mirror’s
indexes (PK + `seq` + partial tombstone). Cold Parquet is listed separately and
is outside the PostgreSQL data directory. Report **local PostgreSQL** and
**total hot+cold** as separate rows — combining them into one “99% smaller”
claim is misleading.

Point lookups on hot and cold primary keys still return the same rows as the
unmanaged baseline (`KoldMergeScan`). Flush duration and peak RSS are measured
by the harness (cluster RSS polled every 50ms during `flush_table`).

## How to read the tables

- **Tradeoff** is relative to plain PostgreSQL on the same machine/run
  (slower / faster / smaller).
- **Hot-only queries** are timed **before flush**, so both heaps still hold all
  10M rows — that isolates `KoldMergeScan` overhead vs a plain index lookup,
  not “smaller heap wins.” The timed SQL is a repeated point lookup of the
  **newest** PK (`WHERE id = <rows>`), not a scan of the whole table.
- **Hot+cold queries** (after flush) alternate **newest hot PK** and **oldest
  cold PK** (50/50 of the lookup loop) so the managed path mixes heap and
  Parquet work. **Cold-only queries** repeatedly look up only `id = 1`
  (flushed to Parquet). PostgreSQL-only runs the same SQL against its full
  heap. Each phase uses `QUERY_LOOPS = 100` for throughput and p99.
  **`VACUUM (FULL, ANALYZE)`** is also timed after flush, when the managed
  heap is the hot working set only.
- **p99 latency** rows use nearest-rank over samples from the same phase:
  insert = per 100k-row batch commit; update = per 1k-row update batch;
  hot-query = per pre-flush hot PK lookup; cold-query = per post-flush
  cold-only PK lookup.
- **Dead tuples** come from `pg_stat_user_tables.n_dead_tup` after the same
  update/delete sample, **before flush** — so both sides match here. The
  maintenance win shows up in post-flush VACUUM time / heap size, not in that
  pre-flush counter.
- Autovacuum counters are **not** shown: autovacuum is disabled on both source
  tables and the generated mirror so the longer async catch-up cannot launch
  maintenance during a following timed phase. Explicit VACUUM is timed after
  flush.
- **Backup size / restore time** are TODO until the harness measures
  `pg_dump` / `pg_restore` (or basebackup) of the PostgreSQL database only —
  cold Parquet is outside the cluster and would be protected separately.
- DML rows in published results use `--dml-sample 50000` on the 10M-row table.
  In async mode the foreground number measures the source heap commit; it does
  **not** include the following explicit `koldstore.wait_for_async_mirror()`
  fence. Catch-up rows are therefore part of the result, not optional context.
  Do not publish comparisons from the default 1k-row sample—it is too noisy.
- **Async foreground insert is not “faster than PostgreSQL.”** Both sides time
  the same heap `INSERT` path (100k-row commits). Async defers mirror apply to
  the catch-up rows; strict pays mirror work in the foreground (hence slower).
  When async’s foreground ops/s lands above PostgreSQL-only, that is **not**
  because the sides shared CPU/disk — they run **one after another** on fresh
  servers. It is still only **one sample per side** (often hours apart), so
  machine noise can move tens of percent; do not treat it as a product win
  until repeated isolated runs agree. For “row is visible in the mirror” cost,
  include catch-up or measure backlog with the background worker on.
- **Published runs are exactly three sequential sides**: `--all-sides` stops
  PostgreSQL, recreates empty worker DBs, and measures `pg`, then `async`, then
  `strict` — once each, alone. Not parallel; no dual-table I/O contention.
  `RESULTS.md` records the UTC finish time per side and the git commit stamped
  into each side’s JSON.
- Insert throughput uses committed 100k-row batches on that side alone.
  Bounded source transactions also avoid presenting one large logical-decoding
  transaction as a representative application insert.
- For deterministic phase accounting, the harness keeps the worker GUC on for
  `manage_table` (required for async activation), then sets
  `koldstore.internal_async_mirror_worker` to `off` and terminates the worker so
  each explicit fence receives the full insert, update, or delete phase. This is
  a measurement control only: its default is `on`, and normal async tables keep
  the bounded-lag background worker running without application fences. The
  harness also performs untimed `CHECKPOINT`s before the insert phase and
  before each timed update/delete, so prior writeback is not charged to the
  next measurement.
- Hot+cold PK lookups open matching Parquet segments (min/max prune +
  row-group stats / bloom). At published scale each surviving segment is ~1M
  wide rows, so footer open + merge-scan setup dominates vs a pure B-tree
  probe; streaming execution and tighter segment sizing are follow-ups. See
  [performance](../performance.md).

## Reproduce

```bash
# Published RESULTS.md: three runs, fresh server each (~15–20+ min/side).
scripts/run-storage-comparison.sh --all-sides --update-results \
  --rows 10000000 --hot-limit 100000 --dml-sample 50000
# Or one side at a time:
scripts/run-storage-comparison.sh --side pg --rows 100000
scripts/run-storage-comparison.sh --side async --rows 100000
scripts/run-storage-comparison.sh --side strict --rows 100000
```

Additional pgbench-oriented suites live under [`benchmarks/`](../../benchmarks/).
HammerDB selective-manage comparison: [hammerdb.md](hammerdb.md).
