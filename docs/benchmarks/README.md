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
`max_rows_per_file = 1000000` (~9.9M rows flushed, zstd Parquet). Numbers vary
by machine; re-run for your hardware. Figures below are from a local PG16.13
`release-pg` run on the managed-mirror DML capture rewrite.

| Result | PostgreSQL only | PostgreSQL + KoldStore | Win |
| --- | --- | --- | --- |
| PostgreSQL heap + indexes (after flush) | 5.85 GiB | 73 MiB | **99% smaller** |
| Index storage | 415 MiB | 11.5 MiB | **97% smaller** |
| Table storage | 5.45 GiB | 62 MiB (+ 599 MiB cold Parquet) | **99% smaller** heap |
| `VACUUM (FULL, ANALYZE)` (after flush) | 71.8 s | 5.16 s | **93% faster** |

Point lookups on hot and cold primary keys still return the same rows as the
unmanaged baseline (`KoldMergeScan`).

## Throughput and trade-offs

How to read the table (Postgres-oriented):

- **Hot-only queries** are timed **before flush**, so both heaps still hold all
  10M rows — that isolates `KoldMergeScan` overhead vs a plain index lookup,
  not “smaller heap wins.”
- **Hot+cold queries** and **`VACUUM (FULL, ANALYZE)`** are timed **after
  flush**, when the managed heap is the hot working set only.
- **Dead tuples** come from `pg_stat_user_tables.n_dead_tup` after the same
  update/delete sample, **before flush** — so both sides match here. The
  maintenance win shows up in post-flush VACUUM time / heap size, not in that
  pre-flush counter.
- Autovacuum counters are **not** shown: this harness is too short for
  autovacuum to run, so `autovacuum_count` stays 0 on both sides and would be
  misleading.
- **Backup size / restore time** are TODO until the harness measures
  `pg_dump` / `pg_restore` (or basebackup) of the PostgreSQL database only —
  cold Parquet is outside the cluster and would be protected separately.

| Operation | PostgreSQL only | PostgreSQL + KoldStore | Storage win |
| --- | --- | --- | --- |
| insert speed† | 59k ops/s | 45k ops/s | — |
| update speed† | 6.0k ops/s | 8.0k ops/s | — |
| delete speed† | 1.1M ops/s | 130k ops/s | — |
| query hot only (before flush) | 1.5k ops/s | 1.7k ops/s | — |
| query with hot+cold (after flush) | 1.5k ops/s | 119 ops/s‡ | — |
| VACUUM time (after flush) | 71.8 s | 5.16 s | **93%** |
| dead tuples after workload | 2010 (live≈10M) | 2000 (live≈10M) | — |
| index storage | 415 MiB | 11.5 MiB | **97%** |
| table storage | 5.45 GiB | 62 MiB (+ 599 MiB cold Parquet) | **99%** |
| total PG backup size | TODO | TODO | — |
| restore time | TODO | TODO | — |

† DML timings use the harness default `--dml-sample 1000` on the 10M-row table.
Managed INSERT/UPDATE/DELETE still maintain the latest-state mirror
(`koldstore.<table>__cl`) in the same transaction. After the statement-level
capture rewrite, a 1k-row UPDATE on this run was competitive with (here, slightly
faster than) the unmanaged heap; bulk DML before/after ratios are in the section
below. Reproduce with `--dml-sample N`.

‡ Hot+cold PK lookups open matching Parquet segments (min/max prune +
row-group stats / bloom). At this scale each surviving segment is ~1M wide
rows, so footer open + merge-scan setup dominates vs a pure B-tree probe;
streaming execution and tighter segment sizing are follow-ups. See
[performance](../performance.md).

## Managed DML capture (before → after)

Capture is synchronous in the user transaction (`AFTER … FOR EACH STATEMENT`
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
# Table above: 10M rows / 100k hot (~30 min on a laptop; release-pg extension).
scripts/run-storage-comparison.sh --rows 10000000 --hot-limit 100000
# Faster local smoke (defaults: 100k rows / 10k hot / 1k DML sample):
scripts/run-storage-comparison.sh
# Managed DML before/after-style sample (UPDATE/DELETE size):
scripts/run-storage-comparison.sh --rows 100000 --hot-limit 10000 --dml-sample 5000
scripts/run-storage-comparison.sh --rows 100000 --hot-limit 10000 --dml-sample 100000
```

Additional pgbench-oriented suites live under [`benchmarks/`](../../benchmarks/).
HammerDB selective-manage comparison: [hammerdb.md](hammerdb.md).
