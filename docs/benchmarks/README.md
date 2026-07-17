# Benchmarks

KoldStore is a **storage lifecycle tool**, not a universal query accelerator.
These docs explain what the storage comparison harness measures when older rows
leave the PostgreSQL heap for Parquet while applications keep querying the same
table.

**Latest numbers:** [RESULTS.md](RESULTS.md) — columns are PostgreSQL only,
PG + KoldStore (async), and PG + KoldStore (strict). Refresh with
`scripts/run-storage-comparison.sh --update-results` (add `--both-modes` to
measure both managed columns in one invocation).

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
Parquet). Inserts alternate 100k-row committed batches so neither side is
always measured first and logical decoding stays transaction-bounded. Numbers
vary by machine; re-run for your hardware. The published async run uses local
PG16.13 `release-pg` with `--mirror-capture-mode async`. The strict
transactional path remains the default; see
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
  not “smaller heap wins.”
- **Hot+cold queries** and **`VACUUM (FULL, ANALYZE)`** are timed **after
  flush**, when the managed heap is the hot working set only.
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
- Insert throughput accumulates only each side's execution time across
  alternating 100k-row batches. Alternating order removes sustained-load bias;
  bounded source transactions also avoid presenting one large logical-decoding
  transaction as a representative application insert.
- For deterministic phase accounting, the harness keeps the worker GUC on for
  `manage_table` (required for async activation), then sets
  `koldstore.internal_async_mirror_worker` to `off` and terminates the worker so
  each explicit fence receives the full insert, update, or delete phase. This is
  a measurement control only: its default is `on`, and normal async tables keep
  the bounded-lag background worker running without application fences. The
  harness also performs untimed `CHECKPOINT`s before the interleaved insert
  phase and before each compared update/delete side, so prior writeback is not
  charged to the next measurement.
- Hot+cold PK lookups open matching Parquet segments (min/max prune +
  row-group stats / bloom). At published scale each surviving segment is ~1M
  wide rows, so footer open + merge-scan setup dominates vs a pure B-tree
  probe; streaming execution and tighter segment sizing are follow-ups. See
  [performance](../performance.md).

## Strict DML capture history (before → trigger rewrite)

This section is retained as historical evidence for the default strict mode;
it is not the async 10M run in [RESULTS.md](RESULTS.md). Capture is synchronous
in the user transaction (`AFTER … FOR EACH STATEMENT` with transition tables).
The rewrite keeps small INSERTs on `ON CONFLICT`, uses `MERGE` for bulk INSERT,
updates the mirror directly for UPDATE/DELETE, and moves PK rejection to a
separate `BEFORE UPDATE OF pk…` row trigger so ordinary UPDATEs no longer
materialize `OLD TABLE`.

Local PG16.13, `release-pg`, median of three runs
(`scripts/run-storage-comparison.sh --rows 100000 --hot-limit 10000`):

| Managed op | Before (5k sample) | After (5k sample) | Speedup |
| --- | ---: | ---: | ---: |
| INSERT | 26.7k ops/s | 50.3k ops/s | **1.9×** |
| UPDATE | 887 ops/s | 43.0k ops/s | **48×** |
| DELETE | 39.7k ops/s | 123k ops/s | **3.1×** |

Absolute after numbers at a **100k-row** DML sample (same machine/profile):

| Managed op | After (100k sample) |
| --- | ---: |
| INSERT | 50.5k ops/s |
| UPDATE | 34.9k ops/s |
| DELETE | 87.3k ops/s |

Task-1 UPDATE of 100k rows with the old OLD/NEW PK guard did not finish in
>6 minutes here, so the fair before/after ratio uses the 5k sample. Design and
gates: [plan](../plans/2026-07-15-managed-mirror-dml-performance.md). Architecture:
[dml-table](../architecture/dml-table.md).

## Reproduce

```bash
# Published RESULTS.md scale: 10M rows / 100k hot / 50k DML sample (~15.5 min/mode).
scripts/run-storage-comparison.sh --rows 10000000 --hot-limit 100000 \
  --dml-sample 50000 --mirror-capture-mode async
# Default strong-consistency mode; mirror work is included in foreground DML:
scripts/run-storage-comparison.sh --rows 10000000 --hot-limit 100000 \
  --dml-sample 50000 --mirror-capture-mode strict
# Refresh docs/benchmarks/RESULTS.md for both managed columns:
scripts/run-storage-comparison.sh --both-modes --update-results \
  --rows 10000000 --hot-limit 100000 --dml-sample 50000
# Faster local smoke (defaults: 100k rows / 10k hot / 1k DML sample):
scripts/run-storage-comparison.sh
# Managed DML before/after-style sample (UPDATE/DELETE size):
scripts/run-storage-comparison.sh --rows 100000 --hot-limit 10000 --dml-sample 5000
scripts/run-storage-comparison.sh --rows 100000 --hot-limit 10000 --dml-sample 100000
```

Additional pgbench-oriented suites live under [`benchmarks/`](../../benchmarks/).
HammerDB selective-manage comparison: [hammerdb.md](hammerdb.md).
