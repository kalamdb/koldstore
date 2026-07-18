# Latest benchmark results

> **Published 10M `--all-sides` run in progress.** The tables below are from a
> small methodology smoke (10k rows) and will be replaced when the full run
> finishes. Each column uses a fresh pgrx PostgreSQL. Methodology:
> [README.md](README.md).

Published numbers from the most recent storage comparison run(s). Re-run
`scripts/run-storage-comparison.sh --all-sides --update-results` to refresh
this file. Each column is measured alone on a fresh pgrx PostgreSQL
(stop → recreate DBs → one side). Methodology: [README.md](README.md).

**Run:** 10000 rows · `hot_row_limit = 1000` · `max_rows_per_file = 1000` · `--dml-sample 500` · `insert_batch_rows = 10000` · zstd Parquet · isolated fresh server per side · sides measured: **pg + async + strict** (smoke only — not published scale)

Managed PostgreSQL sizes include hot heap + `koldstore.<table>__cl` + mirror
indexes. Cold Parquet is outside the PostgreSQL data directory. Columns are
**PostgreSQL only**, **PG + KoldStore (async)**, and **PG + KoldStore (strict)**.

## Main comparison

| Metric | PostgreSQL only | PG + KoldStore (async) | PG + KoldStore (strict) |
| --- | --- | --- | --- |
| foreground insert throughput | 95987 ops/s | 98298 ops/s | 52040 ops/s |
| sustainable insert throughput | TODO | TODO | TODO |
| insert p99 latency | TODO | TODO | TODO |
| update p99 latency | TODO | TODO | TODO |
| hot-query p99 latency | TODO | TODO | TODO |
| cold-query p99 latency | TODO | TODO | TODO |
| hot+cold query throughput | 1493 ops/s | 1147 ops/s | 1095 ops/s |
| cold files fetched/query | — | TODO | TODO |
| cold bytes fetched/query | — | TODO | TODO |
| peak memory under workload | TODO | TODO | TODO |
| peak RSS during flush | — | 95.89 MiB (before=64.59 MiB, after=95.89 MiB) | 78.55 MiB (before=47.59 MiB, after=78.55 MiB) |
| flush duration | — | 485.4 ms (18543 rows/s) | 382.8 ms (23512 rows/s) |
| CPU seconds per 1M operations | TODO | TODO | TODO |
| WAL generated per 1M operations | TODO | TODO | TODO |
| local bytes written | TODO | TODO | TODO |
| VACUUM duration | 165.1 ms | 25.2 ms | 24.1 ms |
| local PostgreSQL storage | 6.42 MiB | 880.0 KiB | 880.0 KiB |
| total hot+cold storage | 6.42 MiB | 1.44 MiB | 1.44 MiB |
| peak open file descriptors | TODO | TODO | TODO |
| combined backup size | TODO | TODO | TODO |
| full query-ready restore time | TODO | TODO | TODO |
| mirror backlog after workload | — | TODO | TODO |
| backlog drain time | — | TODO | TODO |

‡ Hot+cold PK lookups open matching Parquet segments; footer open + merge-scan
setup can dominate vs a pure B-tree probe at large segment sizes. See
[performance](../performance.md).

## Detail (throughput and storage)

| Operation | PostgreSQL only | PG + KoldStore (async) | PG + KoldStore (strict) |
| --- | --- | --- | --- |
| insert speed† | 95987 ops/s (10 µs/op) | 98298 ops/s (10 µs/op) | 52040 ops/s (19 µs/op) |
| update speed† | 80233 ops/s (12 µs/op) | 43505 ops/s (23 µs/op) | 50559 ops/s (20 µs/op) |
| delete speed† | 821524 ops/s (1 µs/op) | 86473 ops/s (12 µs/op) | 91735 ops/s (11 µs/op) |
| └ async insert mirror catch-up | — | 23989 ops/s (42 µs/op) | — |
| └ async update mirror catch-up | — | 5014 ops/s (199 µs/op) | — |
| └ async delete mirror catch-up | — | 7686 ops/s (130 µs/op) | — |
| └ async restore mirror catch-up | — | 8394 ops/s (119 µs/op) | — |
| query hot only (before flush) | 1586 ops/s (631 µs/op) | 1724 ops/s (580 µs/op) | 1592 ops/s (628 µs/op) |
| query with hot+cold (after flush) | 1493 ops/s (670 µs/op) | 1147 ops/s (872 µs/op) | 1095 ops/s (913 µs/op) |
| VACUUM time (after flush) | 165.1 ms | 25.2 ms | 24.1 ms |
| dead tuples after workload | 1000 (live=10000) | 1000 (live=10000) | 1000 (live=10000) |
| index storage (hot + __cl) | 856.0 KiB | 240.0 KiB | 240.0 KiB |
| table storage (hot + __cl) | 5.59 MiB | 640.0 KiB | 640.0 KiB |
| └ cold Parquet | — | 596.9 KiB | 597.1 KiB |
| └ hot heap only | 5.59 MiB | 584.0 KiB | 584.0 KiB |
| └ __cl mirror heap | — | 56.0 KiB | 56.0 KiB |
| └ __cl mirror indexes | — | 88.0 KiB | 88.0 KiB |
| PostgreSQL heap + indexes (after flush) | 6.42 MiB | 880.0 KiB | 880.0 KiB |
| total PG backup size | TODO | TODO | TODO |
| restore time | TODO | TODO | TODO |

† Strict DML updates the change-log mirror in the foreground. Async DML
records heap WAL in the foreground; catch-up rows appear only in the async
column.
