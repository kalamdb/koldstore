# Benchmarks

KoldStore is a **storage lifecycle tool**, not a universal query accelerator.
The numbers below measure what happens when older rows leave the PostgreSQL
heap for Parquet while applications keep querying the same table.

## Storage comparison (primary result)

Harness: [`tests/storage/`](../tests/storage/) with a wide (~50 column) table
from [`tests/storage/schema.sql`](../tests/storage/schema.sql).

Sample run: **10,000,000 rows**, `hot_row_limit = 100000`,
`max_rows_per_file = 1000000` (~9.9M rows flushed, zstd Parquet). Numbers vary
by machine; re-run for your hardware.

| Result | PostgreSQL only | PostgreSQL + KoldStore | Win |
| --- | --- | --- | --- |
| PostgreSQL heap + indexes (after flush) | 5.85 GiB | 72 MiB | **99% smaller** |
| Index storage | 415 MiB | 11.4 MiB | **97% smaller** |
| Table storage | 5.45 GiB | 61 MiB (+ 597 MiB cold Parquet) | **99% smaller** heap |
| `VACUUM (FULL, ANALYZE)` (after flush) | 131 s | 6.4 s | **95% faster** |

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
| insert speed† | 69k ops/s | 23k ops/s | — |
| update speed† | 6.8k ops/s | 5.5k ops/s | — |
| delete speed† | 1.0M ops/s | 38k ops/s | — |
| query hot only (before flush) | 1.6k ops/s | 1.3k ops/s | — |
| query with hot+cold (after flush) | 1.5k ops/s | 127 ops/s‡ | — |
| VACUUM time (after flush) | 131 s | 6.4 s | **95%** |
| dead tuples after workload | 2000 (live≈10M) | 2000 (live≈10M) | — |
| index storage | 415 MiB | 11.4 MiB | **97%** |
| table storage | 5.45 GiB | 61 MiB (+ 597 MiB cold Parquet) | **99%** |
| total PG backup size | TODO | TODO | — |
| restore time | TODO | TODO | — |

† DML is slower under KoldStore because `manage_table` installs capture
triggers that maintain the latest-state change-log mirror
(`koldstore.<table>__cl`: one row per PK with `seq` / `op`). That is the cost
of flush cutoffs and change cursors. The payoff is a smaller hot heap/indexes,
cheaper VACUUM, and (planned) `changes_since` so sync/cache consumers can
follow changes without a second CDC pipeline.

‡ Hot+cold PK lookups open matching Parquet segments (min/max prune +
row-group stats / bloom). At this scale each surviving segment is ~1M wide
rows, so footer open + merge-scan setup dominates vs a pure B-tree probe;
streaming execution and tighter segment sizing are follow-ups. See
[performance](performance.md).

## Reproduce

```bash
# Table above: 10M rows / 100k hot (~30 min on a laptop; release-pg extension).
scripts/run-storage-comparison.sh --rows 10000000 --hot-limit 100000
# Faster local smoke (defaults: 100k rows / 10k hot):
scripts/run-storage-comparison.sh
```

Additional pgbench-oriented suites live under [`benchmarks/`](../benchmarks/).
