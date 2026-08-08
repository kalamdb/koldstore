# Benchmarks

KoldStore is a **storage lifecycle tool**, not a universal query accelerator.
These docs explain what the storage comparison harness measures when older rows
leave the PostgreSQL heap for Parquet while applications keep querying the same
table.

**Latest numbers:** [RESULTS.md](RESULTS.md) — columns are PostgreSQL only and
PG + KoldStore (WAL-only). The file currently holds a **draft single-sample**
10M refresh (2026-08-07, `flushed = 9.9M`; `changes_since` full drain skipped).
Refresh a publishable median with
`scripts/run-storage-comparison.sh --all-sides --repetitions 6 --update-results`
(each sample gets a fresh pgrx PostgreSQL; publication requires a clean tree).

## Documents in this folder

| Doc | Focus |
| --- | --- |
| [README](README.md) (this page) | How to read results + reproduce |
| [RESULTS](RESULTS.md) | Latest published comparison tables only |
| [HammerDB / TPROC-C](hammerdb.md) | Selective-manage OLTP: weekly smoke + opt-in deep |

## Storage comparison

Harness: [`tests/storage/`](../../tests/storage/) with a wide (~50 column) table
from [`tests/storage/schema.sql`](../../tests/storage/schema.sql).

Typical published scale: **10,000,000 rows**, `hot_row_limit = 100000`,
`max_rows_per_file = 1000000`, `--dml-sample 50000` (~9.9M rows flushed, zstd
Parquet). The harness sets `koldstore_max_rows_per_flush` to the cold excess
(override with `KOLDSTORE_STORAGE_MAX_ROWS_PER_FLUSH`) so one `flush_table`
call can drain to the hot limit — the product default (10k × 64 waves) only
covers 640k rows per job. Published RESULTS use `--all-sides --repetitions 6`: six
counterbalanced orders of pg and async (WAL-only managed), with every sample alone on a
fresh pgrx PostgreSQL. They are **not** parallel and do **not** share a live
server or dual-table I/O during measurement. Each cell reports the median and
range. Inserts use committed 100k-row batches. Numbers vary by machine; re-run
for your hardware. See
[Mirror capture](../architecture/mirror-capture.md).

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
- **PostgreSQL-only cold-id / hot+cold** also run **before** `VACUUM FULL` on
  the full heap (same post-DML state as hot-only). Measuring them after a
  whole-table rewrite would compare a freshly compacted 10M heap to managed
  Parquet and inflate the gap.
- **Managed hot+cold / cold-only** run **after flush** (Parquet in play) and
  **before** hot-heap `VACUUM FULL`. Hot+cold alternates newest hot PK and
  oldest cold PK (50/50). Cold-only repeatedly looks up only `id = 1`.
  Each phase uses `QUERY_LOOPS` timed iterations after a short discarded
  warm-up so EXPLAIN / first segment open do not dominate.
- **`VACUUM (FULL, ANALYZE)`** is timed after those query phases: full heap on
  PostgreSQL-only, hot working set only on managed.
- **Timed INSERT** always seeds an empty table up to `rows` on every side.
  `hot_row_limit` has not taken effect yet — managed INSERT is **not** faster
  because “there are fewer hot rows.” The PostgreSQL-only side pins a logical
  slot (+ publication on the seed table) for the timed seed so WAL recycle
  pressure matches managed async capture; expect foreground insert ≈ identical
  or managed slightly slower. Mirror apply remains the separate catch-up rows.
- **p99 latency** rows use nearest-rank over samples from the same phase:
  insert = per 100k-row batch commit; update = per 1k-row update batch;
  hot-query = per pre-flush hot PK lookup; cold-query = per post-flush
  cold-only PK lookup.
- **Dead tuples** come from `pg_stat_user_tables.n_dead_tup` after the same
  update/delete sample, **before flush** — so both sides match here. The
  maintenance win shows up in post-flush VACUUM time / heap size, not in that
  pre-flush counter.
- **Flush write throughput / bandwidth** — managed only. Rows flushed ÷ wall
  time of `flush_table` (aggregated if multiple jobs), and cold Parquet bytes
  written ÷ the same wall time (`MiB/s`).
- **`changes_since` full drain** — managed only, after flush and cold PK timing.
  Pages `koldstore.changes_since` from `since_seq = 0` with
  `limit_rows = 500` (override `KOLDSTORE_STORAGE_CHANGES_SINCE_BATCH`),
  advancing the exclusive cursor until the feed is empty. Reports duration and
  rows/s for the full latest-state set (~seeded `rows`). This is a catch-up
  feed cost, not a point-lookup cost.
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
  the same heap `INSERT` path (100k-row commits). The harness pins WAL retention
  on PostgreSQL-only during the timed seed so segment recycle cannot make plain
  PG look artificially slower than managed (which already holds a real slot).
  Managed capture still defers mirror apply to the catch-up rows — include those
  for “row is visible in the mirror” cost.
- **Published runs use counterbalanced repetitions** (or another multiple of
  the side count): every sample stops PostgreSQL, recreates empty worker DBs,
  and measures one side alone. Orders balance first/second position across pg
  and async. `RESULTS.md` reports per-cell median and range and records the git
  commit.
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
- The storage table's foreground DML rows use 1k-row batches and deliberately
  fence async catch-up separately. The `pg-koldstore-benchmarks` hot-DML suite
  covers single-row OLTP. A production release also needs worker-on sustainable
  UPDATE throughput, bounded peak backlog, and drain time; foreground parity
  alone is insufficient.
- Hot+cold PK lookups open matching Parquet segments (min/max prune +
  row-group stats / bloom). At published scale each surviving segment is ~1M
  wide rows, so footer open + merge-scan setup dominates vs a pure B-tree
  probe; streaming execution and tighter segment sizing are follow-ups. See
  [performance](../performance.md).

## Reproduce

Pick a row count with `--rows`. Defaults are small (100k) for a fast local
smoke; published RESULTS use 10M.

```bash
# Smoke (≈ minutes): PostgreSQL-only then managed, fresh wiped pg16 each side.
scripts/run-storage-comparison.sh --all-sides \
  --rows 100000 --hot-limit 10000 --dml-sample 1000

# Medium
scripts/run-storage-comparison.sh --all-sides \
  --rows 1000000 --hot-limit 50000 --dml-sample 10000

# Published RESULTS scale (long: seed + flush + VACUUM FULL on the full heap)
scripts/run-storage-comparison.sh --all-sides --repetitions 1 --update-results \
  --rows 10000000 --hot-limit 100000 --dml-sample 50000

# Release-style publication (clean git tree; six counterbalanced samples/side)
scripts/run-storage-comparison.sh --all-sides --repetitions 6 --update-results \
  --rows 10000000 --hot-limit 100000 --dml-sample 50000
```

Useful flags:

| Flag | Meaning | Default |
| --- | --- | --- |
| `--rows N` | Timed seed row count | `100000` |
| `--hot-limit N` | Rows kept hot after flush | `10000` |
| `--dml-sample N` | UPDATE/DELETE sample size | `1000` |
| `--insert-batch-rows N` | Rows per committed insert batch | `100000` |
| `--warmup-rows N` | Untimed throwaway warm-up (`0` disables) | scale-aware |
| `--side pg\|async` | One side only | (require `--all-sides` or `--side`) |
| `--all-sides` | pg then async (or counterbalanced order) | |
| `--update-results` | Write `docs/benchmarks/RESULTS.md` | |
| `--pg-version N` | pgrx major (lab uses **16**) | `16` |

Env equivalents: `KOLDSTORE_STORAGE_ROWS`, `KOLDSTORE_STORAGE_HOT_LIMIT`,
`KOLDSTORE_STORAGE_DML_SAMPLE`, `KOLDSTORE_STORAGE_CHANGES_SINCE_BATCH`
(default `500` for the post-flush full-drain), and so on. Draft RESULTS
updates on a dirty tree: `KOLDSTORE_STORAGE_DRAFT_RESULTS=1`. Skip the
multi-hour 10M `changes_since` full drain with
`KOLDSTORE_STORAGE_SKIP_CHANGES_SINCE=1` (cells stay TODO).

Each side force-stops PostgreSQL, **wipes `~/.pgrx/data-<ver>`**, then initdb +
prepare so leftover WAL cannot skew the next side. Before timed seeding the
harness runs an untimed warm-up, equalizes logical-slot tip lag, and (on
PostgreSQL-only) pins a temporary logical slot so WAL retention matches managed
async capture during the seed — foreground INSERT should be ≈ identical, not a
marketed speedup. After the timed seed it probes PK bounds and logs WAL bytes +
pre-flush heap/index size.

One side at a time:

```bash
scripts/run-storage-comparison.sh --side pg --rows 100000
scripts/run-storage-comparison.sh --side async --rows 100000
```

Additional pgbench-oriented suites live under [`benchmarks/`](../../benchmarks/).
Capture is always WAL-only:

```bash
cargo run -p pg-koldstore-benchmarks
```

HammerDB selective-manage comparison: [hammerdb.md](hammerdb.md).
