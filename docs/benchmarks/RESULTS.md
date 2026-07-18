# Latest benchmark results

Published numbers from the most recent storage comparison run(s). Re-run
`scripts/run-storage-comparison.sh --all-sides --update-results` to refresh
this file. Each column is measured alone on a fresh pgrx PostgreSQL
(stop → recreate DBs → one side). Methodology: [README.md](README.md).

> **Re-run required for p99 + cold-only.** Harness now records insert/update/hot/cold
> p99 and a separate cold-only throughput row (hot+cold is a 50/50 mix). The
> 10M numbers below are from the previous published run and omit those new
> cells until `--all-sides --update-results` finishes.

**When:** 2026-07-18 UTC (pg 15:07:29Z, async 15:57:32Z, strict 16:14:16Z)
**Git:** `971397fdf77f` (`971397fdf77fb9d6533dcf86628b21c5640bcc4d`) · dirty tree — branch tip at RESULTS refresh; async/strict sides also included uncommitted fence-timeout / flush / harness fixes present in the working tree that day
**Run:** 10000000 rows · `hot_row_limit = 100000` · `max_rows_per_file = 1000000` · `--dml-sample 50000` · `insert_batch_rows = 100000` · zstd Parquet · **sequential** isolated fresh server per side (pg → async → strict; not parallel) · sides measured: **pg + async + strict**

Managed PostgreSQL sizes include hot heap + `koldstore.<table>__cl` + mirror
indexes. Cold Parquet is outside the PostgreSQL data directory. Columns are
**PostgreSQL only**, **PG + KoldStore (async)**, and **PG + KoldStore (strict)**.

## Main comparison

| Metric | PostgreSQL only | PG + KoldStore (async) | PG + KoldStore (strict) |
| --- | --- | --- | --- |
| foreground insert throughput | 54823 ops/s | 65716 ops/s | 28464 ops/s |
| sustainable insert throughput | TODO | TODO | TODO |
| insert p99 latency | pending re-run | pending re-run | pending re-run |
| update p99 latency | pending re-run | pending re-run | pending re-run |
| hot-query p99 latency | pending re-run | pending re-run | pending re-run |
| cold-query p99 latency | pending re-run | pending re-run | pending re-run |
| hot+cold query throughput | 1331 ops/s | 1224 ops/s | 1162 ops/s |
| cold-only query throughput | pending re-run | pending re-run | pending re-run |
| cold files fetched/query | — | TODO | TODO |
| cold bytes fetched/query | — | TODO | TODO |
| peak memory under workload | TODO | TODO | TODO |
| peak RSS during flush | — | 1.07 GiB (before=344.48 MiB, after=1.07 GiB) | 1.03 GiB (before=186.09 MiB, after=1.03 GiB) |
| flush duration | — | 145.40 s (68089 rows/s) | 204.64 s (48377 rows/s) |
| CPU seconds per 1M operations | TODO | TODO | TODO |
| WAL generated per 1M operations | TODO | TODO | TODO |
| local bytes written | TODO | TODO | TODO |
| VACUUM duration | 159.10 s | 4.22 s | 4.15 s |
| local PostgreSQL storage | 5.85 GiB | 72.23 MiB | 72.23 MiB |
| total hot+cold storage | 5.85 GiB | 670.88 MiB | 670.91 MiB |
| peak open file descriptors | TODO | TODO | TODO |
| combined backup size | TODO | TODO | TODO |
| full query-ready restore time | TODO | TODO | TODO |
| mirror backlog after workload | — | TODO | TODO |
| backlog drain time | — | TODO | TODO |

‡ **Hot+cold query** (after re-run) alternates newest hot PK and oldest cold PK
(50/50). Prior published hot+cold numbers above were cold-PK-only (legacy).
**Cold-only** is the pure cold PK path. See [README.md](README.md).

## Detail (throughput and storage)

| Operation | PostgreSQL only | PG + KoldStore (async) | PG + KoldStore (strict) |
| --- | --- | --- | --- |
| insert speed† | 54823 ops/s (18 µs/op) | 65716 ops/s (15 µs/op) | 28464 ops/s (35 µs/op) |
| update speed† | 20615 ops/s (49 µs/op) | 22792 ops/s (44 µs/op) | 35418 ops/s (28 µs/op) |
| delete speed† | 108314 ops/s (9 µs/op) | 396240 ops/s (3 µs/op) | 105124 ops/s (10 µs/op) |
| └ async insert mirror catch-up | — | 29912 ops/s (33 µs/op) | — |
| └ async update mirror catch-up | — | 824 ops/s (1214 µs/op) | — |
| └ async delete mirror catch-up | — | 30352 ops/s (33 µs/op) | — |
| └ async restore mirror catch-up | — | 4055 ops/s (247 µs/op) | — |
| query hot only (before flush) | 1173 ops/s (852 µs/op) | 1839 ops/s (544 µs/op) | 1794 ops/s (557 µs/op) |
| query with hot+cold (after flush) | 1331 ops/s (752 µs/op) | 1224 ops/s (817 µs/op) | 1162 ops/s (861 µs/op) |
| query cold only (after flush) | pending re-run | pending re-run | pending re-run |
| VACUUM time (after flush) | 159.10 s | 4.22 s | 4.15 s |
| dead tuples after workload | 100000 (live=10000000) | 100000 (live=10000000) | 100000 (live=10000000) |
| index storage (hot + __cl) | 414.86 MiB | 11.45 MiB | 11.45 MiB |
| table storage (hot + __cl) | 5.45 GiB | 60.79 MiB | 60.79 MiB |
| └ cold Parquet | — | 598.65 MiB | 598.67 MiB |
| └ hot heap only | 5.45 GiB | 55.81 MiB | 55.81 MiB |
| └ __cl mirror heap | — | 4.98 MiB | 4.98 MiB |
| └ __cl mirror indexes | — | 4.32 MiB | 4.32 MiB |
| PostgreSQL heap + indexes (after flush) | 5.85 GiB | 72.23 MiB | 72.23 MiB |
| total PG backup size | TODO | TODO | TODO |
| restore time | TODO | TODO | TODO |

† Strict DML updates the change-log mirror in the foreground. Async DML
records heap WAL in the foreground; catch-up rows appear only in the async
column.

### Why does async insert look faster than PostgreSQL only?

It is **not** a KoldStore acceleration of `INSERT`. Both columns time the same
kind of work: committed 100k-row batches into the user heap (+ indexes). Async
does **not** update `koldstore.<table>__cl` in that timed window — that cost is
the separate **async insert mirror catch-up** row. Strict pays mirror work in
the foreground, which is why it is slower.

Sides are **not** run in parallel and do **not** share a live server during
measurement: `--all-sides` runs **pg, then async, then strict**, each after
`cargo pgrx stop` + empty DB recreate. So the ~20% foreground gap here is not
cross-column I/O contention. It is still a **single sample per side** hours
apart on one machine (load / disk cache / thermal can move ~tens of percent).
Do not treat async > PostgreSQL-only insert as a product claim until repeated
isolated runs agree. For end-to-end “row is mirrored” cost, add catch-up (or
run with the background worker and measure lag).
