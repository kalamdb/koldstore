# Benchmarks

KoldStore is a **storage lifecycle tool**, not a universal query accelerator.
The numbers below measure what happens when older rows leave the PostgreSQL
heap for Parquet while applications keep querying the same table.

## Documents in this folder

| Doc | Focus |
| --- | --- |
| [README](README.md) (this page) | Storage comparison + throughput trade-offs |
| [HammerDB / TPROC-C](hammerdb.md) | Selective-manage OLTP stress: baseline vs HISTORY-only manage |

## Storage comparison (primary result)

Harness: [`tests/storage/`](../../tests/storage/) with a wide (~50 column) table
from [`tests/storage/schema.sql`](../../tests/storage/schema.sql).

Sample run: **10,000,000 rows**, `hot_row_limit = 100000`,
`max_rows_per_file = 1000000`, `--dml-sample 50000` (~9.9M rows flushed, zstd
Parquet). Inserts alternate 100k-row committed batches so neither side is
always measured first and logical decoding stays transaction-bounded. Numbers
vary by machine; re-run for your hardware. Figures below are
from a local PG16.13 `release-pg` run using
`--mirror-capture-mode async`. The strict transactional path remains the
default; see [Mirror capture modes](../architecture/mirror-capture-modes.md).

**Managed PostgreSQL sizes always include** the hot user heap **plus**
`koldstore.<table>__cl` (latest-state change-log mirror) **and** that mirror’s
indexes (PK + `seq` + partial tombstone). Cold Parquet is listed separately and
is outside the PostgreSQL data directory.

| Result | PostgreSQL only | PostgreSQL + KoldStore | Tradeoff |
| --- | --- | --- | --- |
| PostgreSQL heap + indexes (after flush) | 5.85 GiB | 73 MiB | **99% smaller** |
| Index storage (hot + `__cl`) | 415 MiB | 11.5 MiB | **97% smaller** |
| Table storage (hot + `__cl`) | 5.45 GiB | 61.56 MiB (+ 597.05 MiB cold Parquet) | **99% smaller** heap |
| `VACUUM (FULL, ANALYZE)` (after flush) | 57.01 s | 3.43 s | **17× faster** |

Point lookups on hot and cold primary keys still return the same rows as the
unmanaged baseline (`KoldMergeScan`).

## Throughput and trade-offs

How to read the table (Postgres-oriented):

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

| Operation | PostgreSQL only | PostgreSQL + KoldStore | Tradeoff |
| --- | --- | --- | --- |
| insert speed† | 98,978 ops/s | 98,454 ops/s | **1% slower** |
| update speed† | 22,001 ops/s | 21,546 ops/s | **2% slower** |
| delete speed† | 112,889 ops/s | 130,241 ops/s | **15% faster** |
| └ async insert mirror catch-up | — | 28,881 ops/s | outside foreground timing |
| └ async update mirror catch-up | — | 1,170 ops/s | outside foreground timing |
| └ async delete mirror catch-up | — | 58,976 ops/s | outside foreground timing |
| └ async restore mirror catch-up | — | 24,440 ops/s | outside foreground timing |
| query hot only (before flush) | 1,568 ops/s | 1,731 ops/s | **10% faster** |
| query with hot+cold (after flush) | 1,591 ops/s | 506 ops/s‡ | **3× slower** |
| VACUUM time (after flush) | 57.01 s | 3.43 s | **17× faster** |
| dead tuples after workload | 100k (live≈10M) | 100k (live≈10M) | — |
| index storage (hot + `__cl`) | 415 MiB | 11.5 MiB | **97% smaller** |
| table storage (hot + `__cl`) | 5.45 GiB | 61.56 MiB (+ 597.05 MiB cold Parquet) | **99% smaller** |
| total PG backup size | TODO | TODO | — |
| restore time | TODO | TODO | — |

† DML rows use `--dml-sample 50000` on the 10M-row table. In async mode the
foreground number measures the source heap commit; it does **not** include the
following explicit `koldstore.wait_for_async_mirror()` fence. Catch-up rows are
therefore part of the result, not optional context. Async reached the foreground
INSERT acceptance target (no more than 10% below PostgreSQL) at **1% slower**.
The 1,170 ops/s UPDATE catch-up is the largest remaining DML bottleneck and should
not be represented as a completed end-to-end update speedup. Do not publish
comparisons from the default 1k-row sample—it is too noisy.

Insert throughput accumulates only each side's execution time across alternating
100k-row batches. Alternating order removes sustained-load bias; bounded source
transactions also avoid presenting one 18GB logical-decoding transaction as a
representative application insert.

For deterministic phase accounting, the benchmark session sets the internal
`koldstore.internal_async_mirror_worker` control to `off` before `manage_table`.
Each explicit fence therefore receives the full insert, update, or delete phase.
This is a measurement control only: its default is `on`, and normal async tables
keep the bounded-lag background worker running without application fences.
The harness also performs untimed `CHECKPOINT`s before the interleaved insert
phase and before each compared update/delete side, so prior writeback is not
charged to the next measurement.

‡ Hot+cold PK lookups open matching Parquet segments (min/max prune +
row-group stats / bloom). At this scale each surviving segment is ~1M wide
rows, so footer open + merge-scan setup dominates vs a pure B-tree probe;
streaming execution and tighter segment sizing are follow-ups. See
[performance](../performance.md).

## Strict DML capture history (before → trigger rewrite)

This section is retained as historical evidence for the default strict mode;
it is not the async 10M run above. Capture is synchronous in the user
transaction (`AFTER … FOR EACH STATEMENT`
with transition tables). The rewrite keeps small INSERTs on `ON CONFLICT`,
uses `MERGE` for bulk INSERT, updates the mirror directly for UPDATE/DELETE,
and moves PK rejection to a separate `BEFORE UPDATE OF pk…` row trigger so
ordinary UPDATEs no longer materialize `OLD TABLE`.

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
# Table above: 10M rows / 100k hot / 50k DML sample (~15.5 min on this laptop).
scripts/run-storage-comparison.sh --rows 10000000 --hot-limit 100000 \
  --dml-sample 50000 --mirror-capture-mode async
# Default strong-consistency mode; mirror work is included in foreground DML:
scripts/run-storage-comparison.sh --rows 10000000 --hot-limit 100000 \
  --dml-sample 50000 --mirror-capture-mode strict
# Faster local smoke (defaults: 100k rows / 10k hot / 1k DML sample):
scripts/run-storage-comparison.sh
# Managed DML before/after-style sample (UPDATE/DELETE size):
scripts/run-storage-comparison.sh --rows 100000 --hot-limit 10000 --dml-sample 5000
scripts/run-storage-comparison.sh --rows 100000 --hot-limit 10000 --dml-sample 100000
```

Additional pgbench-oriented suites live under [`benchmarks/`](../../benchmarks/).
HammerDB selective-manage comparison: [hammerdb.md](hammerdb.md).
