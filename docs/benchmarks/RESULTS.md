# Latest benchmark results

Published numbers from the most recent storage comparison run(s). Re-run
`scripts/run-storage-comparison.sh --update-results` (once per mode, or
with `--both-modes`) to refresh this file. Methodology:
[README.md](README.md).

**Run:** 100000 rows · `hot_row_limit = 10000` · `max_rows_per_file = 10000` · `--dml-sample 1000` · `insert_batch_rows = 100000` · zstd Parquet · modes measured: **async + strict**

Managed PostgreSQL sizes include hot heap + `koldstore.<table>__cl` + mirror
indexes. Cold Parquet is outside the PostgreSQL data directory. Columns are
**PostgreSQL only**, **PG + KoldStore (async)**, and **PG + KoldStore (strict)**.

## Main comparison

| Metric | PostgreSQL only | PG + KoldStore (async) | PG + KoldStore (strict) |
| --- | --- | --- | --- |
| foreground insert throughput | 109815 ops/s | 114000 ops/s | 84703 ops/s |
| sustainable insert throughput | TODO | TODO | TODO |
| insert p99 latency | TODO | TODO | TODO |
| update p99 latency | TODO | TODO | TODO |
| hot-query p99 latency | TODO | TODO | TODO |
| cold-query p99 latency | TODO | TODO | TODO |
| hot+cold query throughput | 1421 ops/s | 1304 ops/s | 1033 ops/s |
| cold files fetched/query | — | TODO | TODO |
| cold bytes fetched/query | — | TODO | TODO |
| peak memory under workload | TODO | TODO | TODO |
| peak RSS during flush | — | 397.75 MiB (before=285.92 MiB, after=397.75 MiB) | 383.67 MiB (before=163.23 MiB, after=383.67 MiB) |
| flush duration | — | 1.63 s (55175 rows/s) | 2.53 s (35547 rows/s) |
| CPU seconds per 1M operations | TODO | TODO | TODO |
| WAL generated per 1M operations | TODO | TODO | TODO |
| local bytes written | TODO | TODO | TODO |
| VACUUM duration | 906.7 ms | 149.7 ms | 155.4 ms |
| local PostgreSQL storage | 62.94 MiB | 7.48 MiB | 7.48 MiB |
| total hot+cold storage | 62.94 MiB | 12.95 MiB | 12.97 MiB |
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
| insert speed† | 109815 ops/s (9 µs/op) | 114000 ops/s (9 µs/op) | 84703 ops/s (12 µs/op) |
| update speed† | 65112 ops/s (15 µs/op) | 57816 ops/s (17 µs/op) | 30922 ops/s (32 µs/op) |
| delete speed† | 724857 ops/s (1 µs/op) | 300515 ops/s (3 µs/op) | 52885 ops/s (19 µs/op) |
| └ async insert mirror catch-up | — | 34220 ops/s (29 µs/op) | — |
| └ async update mirror catch-up | — | 2683 ops/s (373 µs/op) | — |
| └ async delete mirror catch-up | — | 24822 ops/s (40 µs/op) | — |
| └ async restore mirror catch-up | — | 15356 ops/s (65 µs/op) | — |
| query hot only (before flush) | 1502 ops/s (666 µs/op) | 1799 ops/s (556 µs/op) | 1727 ops/s (579 µs/op) |
| query with hot+cold (after flush) | 1421 ops/s (704 µs/op) | 1304 ops/s (767 µs/op) | 1033 ops/s (968 µs/op) |
| VACUUM time (after flush) | 906.7 ms | 149.7 ms | 155.4 ms |
| dead tuples after workload | 2000 (live=100000) | 2000 (live=100000) | 2000 (live=100000) |
| index storage (hot + __cl) | 7.12 MiB | 1.31 MiB | 1.31 MiB |
| table storage (hot + __cl) | 55.81 MiB | 6.17 MiB | 6.17 MiB |
| └ cold Parquet | — | 5.47 MiB | 5.49 MiB |
| └ hot heap only | 55.81 MiB | 5.59 MiB | 5.59 MiB |
| └ __cl mirror heap | — | 592.0 KiB | 592.0 KiB |
| └ __cl mirror indexes | — | 488.0 KiB | 488.0 KiB |
| PostgreSQL heap + indexes (after flush) | 62.94 MiB | 7.48 MiB | 7.48 MiB |
| total PG backup size | TODO | TODO | TODO |
| restore time | TODO | TODO | TODO |

† Strict DML updates the change-log mirror in the foreground. Async DML
records heap WAL in the foreground; catch-up rows appear only in the async
column.
