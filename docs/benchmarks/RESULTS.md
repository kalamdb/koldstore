# Latest benchmark results

Published numbers from the most recent storage comparison run(s). Re-run
`scripts/run-storage-comparison.sh --all-sides --update-results` to refresh
this file. Each column is measured alone on a fresh pgrx PostgreSQL
(stop → recreate DBs → one side). Methodology: [README.md](README.md).

**When:** 2026-07-20 UTC (pg 15:14:28Z, async 15:28:18Z, strict 15:38:45Z)
**Git:** `bd7c09dc885b` (`bd7c09dc885bcdf717855a97c879eb343a05c8d1`) · dirty tree
**Run:** 10000000 rows · `hot_row_limit = 100000` · `max_rows_per_file = 1000000` · `--dml-sample 50000` · `insert_batch_rows = 100000` · `warmup_rows = 1000000` · zstd Parquet · **sequential** isolated fresh server per side (pg → async → strict; not parallel) · sides measured: **pg + async + strict**

Managed PostgreSQL sizes include hot heap + `koldstore.<table>__cl` + mirror
indexes. Cold Parquet is outside the PostgreSQL data directory. Columns are
**PostgreSQL only**, **PG + KoldStore (async)**, and **PG + KoldStore (strict)**.

## Main comparison

| Metric | PostgreSQL only | PG + KoldStore (async) | PG + KoldStore (strict) |
| --- | --- | --- | --- |
| foreground insert throughput | 94302 ops/s | 107030 ops/s | 28537 ops/s |
| sustainable insert throughput | TODO | TODO | TODO |
| insert p99 latency | 2261.57 ms | 1143.11 ms | 5766.58 ms |
| update p99 latency | 182.37 ms | 115.60 ms | 47.30 ms |
| hot-query p99 latency | 650 µs | 889 µs | 878 µs |
| cold-query p99 latency | 663 µs | 1.11 ms | 1.25 ms |
| hot+cold query throughput | 1793 ops/s | 1529 ops/s | 1362 ops/s |
| cold-only query throughput | 1762 ops/s | 1242 ops/s | 1150 ops/s |
| cold files fetched/query | — | TODO | TODO |
| cold bytes fetched/query | — | TODO | TODO |
| peak memory under workload | TODO | TODO | TODO |
| peak RSS during flush | — | 1.18 GiB (before=349.81 MiB, after=1.18 GiB) | 2.00 GiB (before=198.83 MiB, after=2.00 GiB) |
| flush duration | — | 160.70 s (61606 rows/s) | 237.43 s (41696 rows/s) |
| CPU seconds per 1M operations | TODO | TODO | TODO |
| WAL generated per 1M operations | TODO | TODO | TODO |
| local bytes written | TODO | TODO | TODO |
| VACUUM duration | 149.29 s | 3.44 s | 5.08 s |
| local PostgreSQL storage | 5.85 GiB | 72.23 MiB | 72.23 MiB |
| total hot+cold storage | 5.85 GiB | 670.88 MiB | 670.91 MiB |
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
PK lookup (`QUERY_LOOPS = 100`). See [README.md](README.md).

## Detail (throughput and storage)

| Operation | PostgreSQL only | PG + KoldStore (async) | PG + KoldStore (strict) |
| --- | --- | --- | --- |
| insert speed† | 94302 ops/s (11 µs/op) | 107030 ops/s (9 µs/op) | 28537 ops/s (35 µs/op) |
| update speed† | 69239 ops/s (14 µs/op) | 52446 ops/s (19 µs/op) | 54354 ops/s (18 µs/op) |
| delete speed† | 119350 ops/s (8 µs/op) | 179882 ops/s (6 µs/op) | 26953 ops/s (37 µs/op) |
| └ async insert mirror catch-up | — | 30173 ops/s (33 µs/op) | — |
| └ async update mirror catch-up | — | 914 ops/s (1094 µs/op) | — |
| └ async delete mirror catch-up | — | 28417 ops/s (35 µs/op) | — |
| └ async restore mirror catch-up | — | 24906 ops/s (40 µs/op) | — |
| query hot only (before flush) | 1800 ops/s (555 µs/op) | 1825 ops/s (548 µs/op) | 1679 ops/s (596 µs/op) |
| query with hot+cold (after flush) | 1793 ops/s (558 µs/op) | 1529 ops/s (654 µs/op) | 1362 ops/s (734 µs/op) |
| query cold only (after flush) | 1762 ops/s (567 µs/op) | 1242 ops/s (805 µs/op) | 1150 ops/s (870 µs/op) |
| VACUUM time (after flush) | 149.29 s | 3.44 s | 5.08 s |
| dead tuples after workload | 99916 (live=10000000) | 99916 (live=10000000) | 99916 (live=10000000) |
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

## Storage wins at a glance (this run)

Each side used a **fresh** pgrx PostgreSQL, then an **untimed 1M-row warm-up**
(throwaway table → `DROP` → `CHECKPOINT`) before the timed 10M seed. That
rejects cold-start insert skew after install/start.

| Result | PostgreSQL only → async after flush | Tradeoff |
| --- | --- | --- |
| Total footprint (hot + cold) | 5.85 GiB → 670.88 MiB | **89% smaller** |
| └ hot in PostgreSQL (heap + `__cl`) | 5.85 GiB → 72.23 MiB | **99% smaller** |
| └ cold Parquet | — → 598.65 MiB | outside the database |
| Indexes (hot + `__cl`) | 414.86 MiB → 11.45 MiB | **97% smaller** |
| `VACUUM (FULL, ANALYZE)` | 149.29 s → 3.44 s | **43× faster** |

### DML / query (warm-up run)

| Operation | PG only | Async foreground | Strict | How to read |
| --- | ---: | ---: | ---: | --- |
| INSERT | 94.3k ops/s | 107.0k ops/s | 28.5k ops/s | Async ≈ PG (within noise). Strict pays mirror in-txn. |
| UPDATE | 69.2k ops/s | 52.4k ops/s | 54.4k ops/s | Managed a bit slower. |
| DELETE | 119.4k ops/s | 179.9k ops/s | 27.0k ops/s | Strict slower (tombstone). Async gap is still single-sample noise — not a product claim. |
| Hot-only PK | 1.80k ops/s | 1.83k ops/s | 1.68k ops/s | Comparable pre-flush. |
| Hot+cold PK | 1.79k ops/s | 1.53k ops/s | 1.36k ops/s | Parquet open cost. |
| Cold-only PK | 1.76k ops/s | 1.24k ops/s | 1.15k ops/s | Parquet open cost. |

Async mirror catch-up: insert 30.2k, update 0.9k, delete 28.4k, restore 24.9k ops/s.

Without warm-up, PG insert once measured ~60k while async hit ~95k on a later
side — that cold-start artifact is why warm-up is required for published runs.

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
measurement: `--all-sides` runs **pg, then async, then strict**, each after
`cargo pgrx stop` + empty DB recreate. Large foreground gaps are still a
**single sample per side** on one machine. Do not treat async > PostgreSQL-only
insert as a product claim until repeated isolated runs agree. For end-to-end
“row is mirrored” cost, add catch-up (or run with the background worker and
measure lag).

Lab note: the storage harness may set `koldstore.async_mirror_max_retained_bytes = 0`
while the worker is off so 10M-row seeding can retain multi-GiB slot WAL until
the post-insert fence. Production keeps the default 1 GiB fail-closed cap.
