# Latest benchmark results

Published numbers from the most recent storage comparison run(s). Re-run
`scripts/run-storage-comparison.sh --all-sides --repetitions 6 --update-results` to refresh
this file. Each column is measured alone on a fresh pgrx PostgreSQL
(stop → recreate DBs → one side). Methodology: [README.md](README.md).

**When:** 2026-07-31 UTC (pg 04:42:16Z, async 04:53:46Z, strict 05:04:55Z)
**Git:** `efb93e2b2d55` (`efb93e2b2d557305721a0d877819bbb1c2760925`)
**Run:** 10000000 rows · `hot_row_limit = 100000` · `max_rows_per_file = 1000000` · `--dml-sample 50000` · `insert_batch_rows = 100000` · `warmup_rows = 1000000` · zstd Parquet · **counterbalanced sequential** isolated fresh server per sample (not parallel) · sides measured: **pg + async + strict** · **single sample per side**

Managed PostgreSQL sizes include hot heap + `koldstore.<table>__cl` + mirror
indexes. Cold Parquet is outside the PostgreSQL data directory. Columns are
**PostgreSQL only**, **PG + KoldStore (async)**, and **PG + KoldStore (strict)**.

## Main comparison

| Metric | PostgreSQL only | PG + KoldStore (async) | PG + KoldStore (strict) |
| --- | --- | --- | --- |
| foreground insert throughput | 37926 ops/s | 81771 ops/s | 24819 ops/s |
| sustainable insert throughput | TODO | TODO | TODO |
| sustainable update throughput | TODO | TODO | TODO |
| insert p99 latency | 7068.24 ms | 3785.01 ms | 10483.58 ms |
| update p99 latency | 208.38 ms | 110.04 ms | 112.61 ms |
| hot-query p99 latency | 359 µs | 359 µs | 420 µs |
| cold-query p99 latency | 361 µs | 2.32 ms | 2.08 ms |
| hot+cold query throughput | 4452 ops/s | 976 ops/s | 901 ops/s |
| cold-only query throughput | 4393 ops/s | 610 ops/s | 581 ops/s |
| cold files fetched/query | — | TODO | TODO |
| cold bytes fetched/query | — | TODO | TODO |
| peak memory under workload | TODO | TODO | TODO |
| peak RSS during flush | — | 607.44 MiB (before=339.77 MiB, after=607.44 MiB) | 954.7 MiB (before=180.05 MiB, after=954.70 MiB) |
| flush duration | — | 139.33 s (71054 rows/s) | 231.99 s (42675 rows/s) |
| CPU seconds per 1M operations | TODO | TODO | TODO |
| WAL generated per 1M operations | TODO | TODO | TODO |
| local bytes written | TODO | TODO | TODO |
| VACUUM duration | 174.36 s | 3.59 s | 3.67 s |
| local PostgreSQL storage | 5.85 GiB | 72.23 MiB | 72.23 MiB |
| total hot+cold storage | 5.85 GiB | 670.75 MiB | 670.78 MiB |
| peak open file descriptors | TODO | TODO | TODO |
| combined backup size | TODO | TODO | TODO |
| full query-ready restore time | TODO | TODO | TODO |
| mirror backlog after workload | — | TODO | TODO |
| backlog drain time | — | TODO | TODO |

‡ **Hot+cold query** alternates newest hot PK (`id = <rows>`) and oldest
cold PK (`id = 1`) after flush — **50/50** of the lookup loop.
**Cold-only** repeatedly looks up only `id = 1` (Parquet on managed).
**Hot-only** (before flush) repeatedly looks up `id = <rows>`.
p99 insert = per insert-batch; update = per 1k-row batch; queries = per
PK lookup (`QUERY_LOOPS = 400` after 40 discarded warm-up lookups). See [README.md](README.md).

## Detail (throughput and storage)

| Operation | PostgreSQL only | PG + KoldStore (async) | PG + KoldStore (strict) |
| --- | --- | --- | --- |
| insert speed† | 37926 ops/s (26 µs/op) | 81771 ops/s (12 µs/op) | 24819 ops/s (40 µs/op) |
| update speed† | 63194 ops/s (16 µs/op) | 54640 ops/s (18 µs/op) | 50226 ops/s (20 µs/op) |
| delete speed† | 36267 ops/s (28 µs/op) | 114842 ops/s (9 µs/op) | 49935 ops/s (20 µs/op) |
| └ async insert mirror catch-up | — | 30360 ops/s (33 µs/op) | — |
| └ async update mirror catch-up | — | 1563 ops/s (640 µs/op) | — |
| └ async delete mirror catch-up | — | 29968 ops/s (33 µs/op) | — |
| └ async restore mirror catch-up | — | 25272 ops/s (40 µs/op) | — |
| query hot only (before flush) | 4181 ops/s (239 µs/op) | 3547 ops/s (282 µs/op) | 2975 ops/s (336 µs/op) |
| query with hot+cold (after flush) | 4452 ops/s (225 µs/op) | 976 ops/s (1024 µs/op) | 901 ops/s (1110 µs/op) |
| query cold only (after flush) | 4393 ops/s (228 µs/op) | 610 ops/s (1639 µs/op) | 581 ops/s (1722 µs/op) |
| VACUUM time (after flush) | 174.36 s | 3.59 s | 3.67 s |
| dead tuples after workload | 99916 (live=10000000) | 99916 (live=10000000) | 99916 (live=10000000) |
| index storage (hot + __cl) | 414.86 MiB | 11.45 MiB | 11.45 MiB |
| table storage (hot + __cl) | 5.45 GiB | 60.79 MiB | 60.79 MiB |
| └ cold Parquet | — | 598.52 MiB | 598.54 MiB |
| └ hot heap only | 5.45 GiB | 55.81 MiB | 55.81 MiB |
| └ __cl mirror heap | — | 4.98 MiB | 4.98 MiB |
| └ __cl mirror indexes | — | 4.32 MiB | 4.32 MiB |
| PostgreSQL heap + indexes (after flush) | 5.85 GiB | 72.23 MiB | 72.23 MiB |
| total PG backup size | TODO | TODO | TODO |
| restore time | TODO | TODO | TODO |

† Strict DML updates the change-log mirror in the foreground. Async DML
records heap WAL in the foreground; catch-up rows appear only in the async
column.

## Storage wins at a glance (this run)

KoldStore is a **storage lifecycle** tool. The durable wins after flush are heap
size, index size, and VACUUM time — not universal DML/query acceleration.
Async column below (vs PostgreSQL-only). Single-sample draft after a clean
single-pg16 lab (no concurrent pgrx 15/17/18).

| Result | Before → after flush | Tradeoff |
| --- | --- | --- |
| Total footprint (hot + cold) | 5.85 GiB → 671 MiB | **89% smaller** |
| └ hot in PostgreSQL (heap + `__cl`) | 5.85 GiB → 72 MiB | **99% smaller** |
| └ cold Parquet | — → 599 MiB | outside the database |
| Indexes (hot + `__cl`) | 415 MiB → 11.5 MiB | **97% smaller** |
| `VACUUM (FULL, ANALYZE)` | 174.36 s → 3.59 s | **49× faster** |

### Why was delete reported faster before — and is it?

Foreground delete is a single `DELETE … WHERE id BETWEEN …` over
`--dml-sample` rows **before flush**. Async does **not** update the mirror in
that window (catch-up is a separate row). Strict updates
`koldstore.<table>__cl` to `op = 3` in the same transaction, so strict being
slower than plain PostgreSQL is expected.

Async can still land below PostgreSQL-only: one-shot bulk DELETE has high
variance across isolated sides, and the managed table still carries a logical
publication. Prior “async delete much faster” tables mixed mismatched side
JSON. Do **not** publish “KoldStore makes DELETE faster” from a single sample.

### Segment object-path layout

Flush keys use `{namespace}/{table}/{folder:03}/segment-{NNNN}-{8hex}.parquet`
(100 segments per folder). Manifest stores the table-relative path. This does
**not** change DML, VACUUM, or Parquet byte size; it only improves listing
hygiene vs a flat `batch-*` / full-UUID layout. Keep the short token for
orphan-retry uniqueness; week/Hive folders are unnecessary while catalog stats
prune reads.

### Why does async insert look faster than PostgreSQL only?

It is **not** a KoldStore acceleration of `INSERT`. Both columns time the same
kind of work: committed 100k-row batches into the user heap (+ indexes). Async
does **not** update `koldstore.<table>__cl` in that timed window — that cost is
the separate **async insert mirror catch-up** row. Strict pays mirror work in
the foreground, which is why it is slower.

Sides are **not** run in parallel and do **not** share a live server during
measurement: publication uses six counterbalanced side orders, each sample after
`cargo pgrx stop` + empty DB recreate. Large foreground gaps can still reflect
machine variance. Do not treat async > PostgreSQL-only
insert as a product claim until repeated isolated runs agree. For end-to-end
“row is mirrored” cost, add catch-up (or run with the background worker and
measure lag).

Lab note: the storage harness may set `koldstore.async_mirror_max_retained_bytes = 0`
while the worker is off so 10M-row seeding can retain multi-GiB slot WAL until
the post-insert fence. Production keeps the default 1 GiB health threshold;
crossing it alerts but never blocks apply from draining retained WAL.
