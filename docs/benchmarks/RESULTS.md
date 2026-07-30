# Latest benchmark results

Published numbers from the most recent storage comparison run(s). Re-run
`scripts/run-storage-comparison.sh --all-sides --repetitions 6 --update-results` to refresh
this file. Each column is measured alone on a fresh pgrx PostgreSQL
(stop → recreate DBs → one side). Methodology: [README.md](README.md).

**When:** 2026-07-29 UTC (pg 21:35:28Z, async 21:48:33Z, strict 22:02:02Z)
**Git:** `db9550fd25cc` (`db9550fd25cc14d1226d1f1a0628c136554e824f`)
**Run:** 10000000 rows · `hot_row_limit = 100000` · `max_rows_per_file = 1000000` · `--dml-sample 50000` · `insert_batch_rows = 100000` · `warmup_rows = 1000000` · zstd Parquet · **counterbalanced sequential** isolated fresh server per sample (not parallel) · sides measured: **pg + async + strict** · **single sample per side**

Managed PostgreSQL sizes include hot heap + `koldstore.<table>__cl` + mirror
indexes. Cold Parquet is outside the PostgreSQL data directory. Columns are
**PostgreSQL only**, **PG + KoldStore (async)**, and **PG + KoldStore (strict)**.

## Main comparison

| Metric | PostgreSQL only | PG + KoldStore (async) | PG + KoldStore (strict) |
| --- | --- | --- | --- |
| foreground insert throughput | 39802 ops/s | 67229 ops/s | 21091 ops/s |
| sustainable insert throughput | TODO | TODO | TODO |
| sustainable update throughput | TODO | TODO | TODO |
| insert p99 latency | 5852.87 ms | 8283.65 ms | 10403.31 ms |
| update p99 latency | 117.36 ms | 130.11 ms | 153.04 ms |
| hot-query p99 latency | 592 µs | 356 µs | 452 µs |
| cold-query p99 latency | 386 µs | 2.41 ms | 2.56 ms |
| hot+cold query throughput | 4424 ops/s | 1055 ops/s | 985 ops/s |
| cold-only query throughput | 3117 ops/s | 575 ops/s | 562 ops/s |
| cold files fetched/query | — | TODO | TODO |
| cold bytes fetched/query | — | TODO | TODO |
| peak memory under workload | TODO | TODO | TODO |
| peak RSS during flush | — | 337.86 MiB (before=337.86 MiB, after=177.70 MiB) | 190.12 MiB (before=190.12 MiB, after=182.08 MiB) |
| flush duration | — | 188.14 s (52621 rows/s) | 292.98 s (33791 rows/s) |
| CPU seconds per 1M operations | TODO | TODO | TODO |
| WAL generated per 1M operations | TODO | TODO | TODO |
| local bytes written | TODO | TODO | TODO |
| VACUUM duration | 221.94 s | 5.12 s | 5.96 s |
| local PostgreSQL storage | 5.85 GiB | 72.23 MiB | 72.23 MiB |
| total hot+cold storage | 5.85 GiB | 670.75 MiB | 670.78 MiB |
| peak open file descriptors | TODO | TODO | TODO |
| combined backup size | TODO | TODO | TODO |
| full query-ready restore time | TODO | TODO | TODO |
| mirror backlog after workload | — | TODO | TODO |
| backlog drain time | — | TODO | TODO |

‡ **Hot-only** (before flush) repeatedly looks up `id = <rows>` on the full
heap. **PostgreSQL-only cold-id / hot+cold** use the same full-heap state
**before** `VACUUM FULL`. **Managed cold-only / hot+cold** run after flush
(Parquet) before hot-heap VACUUM — hot+cold is a **50/50** mix of newest hot
PK and oldest cold PK (`id = 1`). Timed INSERT seeds an empty table on every
side; `hot_row_limit` does not shrink the insert working set.
p99 insert = per insert-batch; update = per 1k-row batch; queries = per
PK lookup after discarded warm-up loops. See [README.md](README.md).

## Detail (throughput and storage)

| Operation | PostgreSQL only | PG + KoldStore (async) | PG + KoldStore (strict) |
| --- | --- | --- | --- |
| insert speed† | 39802 ops/s (25 µs/op) | 67229 ops/s (15 µs/op) | 21091 ops/s (47 µs/op) |
| update speed† | 73412 ops/s (14 µs/op) | 54378 ops/s (18 µs/op) | 49825 ops/s (20 µs/op) |
| delete speed† | 105673 ops/s (9 µs/op) | 136293 ops/s (7 µs/op) | 51466 ops/s (19 µs/op) |
| └ async insert mirror catch-up | — | 30773 ops/s (32 µs/op) | — |
| └ async update mirror catch-up | — | 1272 ops/s (786 µs/op) | — |
| └ async delete mirror catch-up | — | 29531 ops/s (34 µs/op) | — |
| └ async restore mirror catch-up | — | 26186 ops/s (38 µs/op) | — |
| query hot only (before flush) | 3204 ops/s (312 µs/op) | 3248 ops/s (308 µs/op) | 2675 ops/s (374 µs/op) |
| query with hot+cold (after flush) | 4424 ops/s (226 µs/op) | 1055 ops/s (948 µs/op) | 985 ops/s (1015 µs/op) |
| query cold only (after flush) | 3117 ops/s (321 µs/op) | 575 ops/s (1740 µs/op) | 562 ops/s (1780 µs/op) |
| VACUUM time (after flush) | 221.94 s | 5.12 s | 5.96 s |
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

Each side used a **fresh** pgrx PostgreSQL, then an **untimed 1M-row warm-up**
(throwaway table → `DROP` → `CHECKPOINT`) before the timed 10M seed.

| Result | PostgreSQL only → async after flush | Tradeoff |
| --- | --- | --- |
| Total footprint (hot + cold) | 5.85 GiB → 670.75 MiB | **89% smaller** |
| └ hot in PostgreSQL (heap + `__cl`) | 5.85 GiB → 72.23 MiB | **99% smaller** |
| └ cold Parquet | — → 598.52 MiB | outside the database |
| Indexes (hot + `__cl`) | 414.86 MiB → 11.45 MiB | **97% smaller** |
| `VACUUM (FULL, ANALYZE)` | 221.94 s → 5.12 s | **43× faster** |

### DML / query (this single sample)

| Operation | PG only | Async foreground | Strict | How to read |
| --- | ---: | ---: | ---: | --- |
| INSERT | 39.8k ops/s | 67.2k ops/s | 21.1k ops/s | Async ≈ heap path (mirror deferred). Strict pays mirror in-txn. Do not claim async accelerates INSERT. |
| UPDATE | 73.4k ops/s | 54.4k ops/s | 49.8k ops/s | Async −26%; strict −32%. Catch-up was 1.3k ops/s — not sustainable throughput. |
| DELETE | 105.7k ops/s | 136.3k ops/s | 51.5k ops/s | Single-sample noise — do not claim DELETE is faster. |
| Hot-only PK | 3.20k ops/s | 3.25k ops/s | 2.68k ops/s | Pre-flush native Index Scan; ≈ PG. |
| Hot+cold PK | 4.42k ops/s | 1.06k ops/s | 0.99k ops/s | Parquet open cost after flush. |
| Cold-only PK | 3.12k ops/s | 0.58k ops/s | 0.56k ops/s | Parquet open cost after flush. |

Async mirror catch-up: insert 30.8k, update 1.3k, delete 29.5k, restore 26.2k ops/s.

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
