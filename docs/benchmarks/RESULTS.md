# Latest benchmark results

Published numbers from the most recent storage comparison run(s). Re-run
`scripts/run-storage-comparison.sh --update-results` (once per mode, or
with `--both-modes`) to refresh this file. Methodology:
[README.md](README.md).

**Run:** 10000000 rows · `hot_row_limit = 100000` · `max_rows_per_file = 1000000` · `--dml-sample 50000` · `insert_batch_rows = 100000` · zstd Parquet · modes measured: **async + strict**

Managed PostgreSQL sizes include hot heap + `koldstore.<table>__cl` + mirror
indexes. Cold Parquet is outside the PostgreSQL data directory. Columns are
**PostgreSQL only**, **PG + KoldStore (async)**, and **PG + KoldStore (strict)**.

## Main comparison

| Metric | PostgreSQL only | PG + KoldStore (async) | PG + KoldStore (strict) |
| --- | --- | --- | --- |
| foreground insert throughput | 46915 ops/s | 91864 ops/s | 21957 ops/s |
| sustainable insert throughput | TODO | TODO | TODO |
| insert p99 latency | TODO | TODO | TODO |
| update p99 latency | TODO | TODO | TODO |
| hot-query p99 latency | TODO | TODO | TODO |
| cold-query p99 latency | TODO | TODO | TODO |
| hot+cold query throughput | 1420 ops/s | 1262 ops/s | 1175 ops/s |
| cold files fetched/query | — | TODO | TODO |
| cold bytes fetched/query | — | TODO | TODO |
| peak memory under workload | TODO | TODO | TODO |
| peak RSS during flush | — | 450.52 MiB (before=351.64 MiB, after=450.52 MiB) | 1.95 GiB (before=198.66 MiB, after=1.95 GiB) |
| flush duration | — | 152.18 s (65054 rows/s) | 212.23 s (46647 rows/s) |
| CPU seconds per 1M operations | TODO | TODO | TODO |
| WAL generated per 1M operations | TODO | TODO | TODO |
| local bytes written | TODO | TODO | TODO |
| VACUUM duration | 112.79 s | 3.40 s | 6.01 s |
| local PostgreSQL storage | 5.85 GiB | 73.01 MiB | 73.01 MiB |
| total hot+cold storage | 5.85 GiB | 670.06 MiB | 671.68 MiB |
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
| insert speed† | 46915 ops/s (21 µs/op) | 91864 ops/s (11 µs/op) | 21957 ops/s (46 µs/op) |
| update speed† | 21194 ops/s (47 µs/op) | 22486 ops/s (44 µs/op) | 19055 ops/s (52 µs/op) |
| delete speed† | 129358 ops/s (8 µs/op) | 147819 ops/s (7 µs/op) | 48357 ops/s (21 µs/op) |
| └ async insert mirror catch-up | — | 29225 ops/s (34 µs/op) | — |
| └ async update mirror catch-up | — | 1197 ops/s (835 µs/op) | — |
| └ async delete mirror catch-up | — | 73630 ops/s (14 µs/op) | — |
| └ async restore mirror catch-up | — | 26388 ops/s (38 µs/op) | — |
| query hot only (before flush) | 1562 ops/s (640 µs/op) | 1844 ops/s (542 µs/op) | 1715 ops/s (583 µs/op) |
| query with hot+cold (after flush) | 1420 ops/s (704 µs/op) | 1262 ops/s (793 µs/op) | 1175 ops/s (851 µs/op) |
| VACUUM time (after flush) | 112.79 s | 3.40 s | 6.01 s |
| dead tuples after workload | 100000 (live=10000000) | 100000 (live=10000000) | 100000 (live=10000000) |
| index storage (hot + __cl) | 414.86 MiB | 11.45 MiB | 11.45 MiB |
| table storage (hot + __cl) | 5.45 GiB | 61.56 MiB | 61.56 MiB |
| └ cold Parquet | — | 597.05 MiB | 598.67 MiB |
| └ hot heap only | 5.45 GiB | 55.81 MiB | 55.81 MiB |
| └ __cl mirror heap | — | 5.75 MiB | 5.75 MiB |
| └ __cl mirror indexes | — | 4.32 MiB | 4.32 MiB |
| PostgreSQL heap + indexes (after flush) | 5.85 GiB | 73.01 MiB | 73.01 MiB |
| total PG backup size | TODO | TODO | TODO |
| restore time | TODO | TODO | TODO |

† Strict DML updates the change-log mirror in the foreground. Async DML
records heap WAL in the foreground; catch-up rows appear only in the async
column.
