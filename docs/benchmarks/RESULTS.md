# Latest benchmark results

Published numbers from the most recent storage comparison run(s). Re-run
`scripts/run-storage-comparison.sh --all-sides --repetitions 6 --update-results` to refresh
this file. Each column is measured alone on a wiped + re-initdb pgrx PostgreSQL
(stop → wipe `~/.pgrx/data-<ver>` → prepare → one side). Methodology:
[README.md](README.md).

**When:** 2026-07-31 UTC (pg 16:51:46Z, async 17:02:42Z)
**Git:** `1eaf5b25abf8` (`1eaf5b25abf86a45ceb237701eea82504faad3a6`)
**Run:** 10000000 rows · `hot_row_limit = 100000` · `max_rows_per_file = 1000000` · `--dml-sample 50000` · `insert_batch_rows = 100000` · `warmup_rows = 1000000` · zstd Parquet · **counterbalanced sequential** isolated wiped server per sample (not parallel) · sides measured: **pg + async** · **single sample per side**

Managed PostgreSQL sizes include hot heap + `koldstore.<table>__cl` + mirror
indexes. Cold Parquet is outside the PostgreSQL data directory. Columns are
**PostgreSQL only** and **PG + KoldStore** (WAL-only capture).

## Main comparison

| Metric | PostgreSQL only | PG + KoldStore |
| --- | --- | --- |
| foreground insert throughput | 47202 ops/s | 101325 ops/s |
| sustainable insert throughput | TODO | TODO |
| sustainable update throughput | TODO | TODO |
| insert p99 latency | 5220.59 ms | 1166.21 ms |
| update p99 latency | 204.61 ms | 115.03 ms |
| hot-query p99 latency | 339 µs | 372 µs |
| cold-query p99 latency | 287 µs | 1.81 ms |
| hot+cold query throughput | 4204 ops/s | 1055 ops/s |
| cold-only query throughput | 4182 ops/s | 653 ops/s |
| cold files fetched/query | — | TODO |
| cold bytes fetched/query | — | TODO |
| peak memory under workload | TODO | TODO |
| peak RSS during flush | — | 553.73 MiB (before=336.16 MiB, after=553.73 MiB) |
| flush duration | — | 136.65 s (72448 rows/s) |
| CPU seconds per 1M operations | TODO | TODO |
| WAL generated per 1M operations | TODO | TODO |
| local bytes written | TODO | TODO |
| VACUUM duration | 227.02 s | 3.21 s |
| local PostgreSQL storage | 5.85 GiB | 72.23 MiB |
| total hot+cold storage | 5.85 GiB | 670.75 MiB |
| peak open file descriptors | TODO | TODO |
| combined backup size | TODO | TODO |
| full query-ready restore time | TODO | TODO |
| mirror backlog after workload | — | TODO |
| backlog drain time | — | TODO |

‡ **Hot+cold query** alternates newest hot PK (`id = <rows>`) and oldest
cold PK (`id = 1`) after flush — **50/50** of the lookup loop.
**Cold-only** repeatedly looks up only `id = 1` (Parquet on managed).
**Hot-only** (before flush) repeatedly looks up `id = <rows>`.
p99 insert = per insert-batch; update = per 1k-row batch; queries = per
PK lookup (`QUERY_LOOPS = 400` after 40 discarded warm-up lookups). See [README.md](README.md).

## Detail (throughput and storage)

| Operation | PostgreSQL only | PG + KoldStore |
| --- | --- | --- |
| insert speed† | 47202 ops/s (21 µs/op) | 101325 ops/s (10 µs/op) |
| update speed† | 63453 ops/s (16 µs/op) | 55350 ops/s (18 µs/op) |
| delete speed† | 36905 ops/s (27 µs/op) | 140874 ops/s (7 µs/op) |
| └ async insert mirror catch-up | — | 32119 ops/s (31 µs/op) |
| └ async update mirror catch-up | — | 1196 ops/s (836 µs/op) |
| └ async delete mirror catch-up | — | 27841 ops/s (36 µs/op) |
| └ async restore mirror catch-up | — | 4387 ops/s (228 µs/op) |
| query hot only (before flush) | 3990 ops/s (251 µs/op) | 3818 ops/s (262 µs/op) |
| query with hot+cold (after flush) | 4204 ops/s (238 µs/op) | 1055 ops/s (948 µs/op) |
| query cold only (after flush) | 4182 ops/s (239 µs/op) | 653 ops/s (1532 µs/op) |
| VACUUM time (after flush) | 227.02 s | 3.21 s |
| dead tuples after workload | 99916 (live=10000000) | 99916 (live=10000000) |
| index storage (hot + __cl) | 414.86 MiB | 11.45 MiB |
| table storage (hot + __cl) | 5.45 GiB | 60.79 MiB |
| └ cold Parquet | — | 598.52 MiB |
| └ hot heap only | 5.45 GiB | 55.81 MiB |
| └ __cl mirror heap | — | 4.98 MiB |
| └ __cl mirror indexes | — | 4.32 MiB |
| PostgreSQL heap + indexes (after flush) | 5.85 GiB | 72.23 MiB |
| total PG backup size | TODO | TODO |
| restore time | TODO | TODO |

† Managed DML records heap WAL in the foreground; catch-up rows appear
separately after `wait_for_async_mirror()`.

## Storage wins at a glance (this run)

KoldStore is a **storage lifecycle** tool. The durable wins after flush are heap
size, index size, and VACUUM time — not universal DML/query acceleration.
Async column below (vs PostgreSQL-only).

| Result | Before → after flush | Tradeoff |
| --- | --- | --- |
| Total footprint (hot + cold) | 5.85 GiB → 670.75 MiB | **89% smaller** |
| └ hot in PostgreSQL (heap + `__cl`) | 5.85 GiB → 72.23 MiB | **99% smaller** |
| └ cold Parquet | — → 599 MiB | outside the database |
| Indexes (hot + `__cl`) | 414.86 MiB → 11.45 MiB | **97% smaller** |
| `VACUUM (FULL, ANALYZE)` | 227.02 s → 3.21 s | **71× faster** |

### Why was delete reported faster before — and is it?

Foreground delete is a single `DELETE … WHERE id BETWEEN …` over
`--dml-sample` rows **before flush**. Managed capture does **not** update the
mirror in that window (catch-up is a separate row).

Managed delete can still land below PostgreSQL-only: one-shot bulk DELETE has
high variance across isolated sides, and the managed table still carries a
logical publication. Do **not** publish “KoldStore makes DELETE faster” from a
single sample.

### Segment object-path layout

Flush keys use `{namespace}/{table}/{folder:03}/segment-{NNNN}-{8hex}.parquet`
(100 segments per folder). Manifest stores the table-relative path. This does
**not** change DML, VACUUM, or Parquet byte size; it only improves listing
hygiene vs a flat `batch-*` / full-UUID layout. Keep the short token for
orphan-retry uniqueness; week/Hive folders are unnecessary while catalog stats
prune reads.

### Why does managed insert look faster than PostgreSQL only?

It is **not** a KoldStore acceleration of `INSERT`. Both columns time the same
kind of work: committed 100k-row batches into the user heap (+ indexes).
Managed capture does **not** update `koldstore.<table>__cl` in that timed
window — that cost is the separate **async insert mirror catch-up** row.

After each timed seed the harness probes PK bounds and logs WAL bytes plus
pre-flush heap/index size. When those footprints match and first batches are
similar, a large late-batch gap is I/O / checkpoint variance — not skipped
indexes or a smaller hot set. Sides run one after another on wiped clusters
(`cargo pgrx stop` + wipe data dir + initdb). Do not treat managed >
PostgreSQL-only insert as a product claim until repeated isolated runs agree.
For end-to-end “row is mirrored” cost, add catch-up (or run with the
background worker and measure lag).

This run’s seed checks (before catch-up):

| Check | PostgreSQL only | PG + KoldStore |
| --- | --- | --- |
| PK coverage | ids `1..10_000_000` | same |
| User heap / indexes | 5.45 GiB / 436.48 MiB | 5.45 GiB / 436.48 MiB |
| WAL during timed insert | 9.06 GiB | 9.06 GiB |
| First / last 100k-row batch | 854 ms / 3905 ms | 857 ms / 956 ms |
| Timed insert wall clock | 211.9 s (47k rows/s) | 98.7 s (101k rows/s) |

End-to-end mirrored cost ≈ foreground + async insert catch-up (~32k rows/s
here), which is slower than PostgreSQL-only foreground alone.

Lab note: the storage harness may set `koldstore.async_mirror_max_retained_bytes = 0`
while the worker is off so 10M-row seeding can retain multi-GiB slot WAL until
the post-insert fence. Production keeps the default 1 GiB health threshold;
crossing it alerts but never blocks apply from draining retained WAL.
