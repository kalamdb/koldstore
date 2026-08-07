# Latest benchmark results

> **Draft single-sample refresh (2026-08-06).** One isolated pg + async pair at
> published 10M scale (`flushed = 9,900,000`). Not a 6-rep publication; treat
> absolute query TPS as noisy. Re-run with `--repetitions 6 --update-results` on
> a clean tree before marketing numbers.

Published numbers from the most recent storage comparison run(s). Re-run
`scripts/run-storage-comparison.sh --all-sides --repetitions 6 --update-results` to refresh
this file. Each column is measured alone on a wiped + re-initdb pgrx PostgreSQL
(stop → wipe `~/.pgrx/data-<ver>` → prepare → one side). Methodology:
[README.md](README.md).

**When:** 2026-08-06 UTC (pg 21:32:41Z, async 21:43:04Z)
**Git:** `1ad22d841aa6` (`1ad22d841aa6580453e29dad333b3e1724f0ac99`) — draft stamp (`KOLDSTORE_STORAGE_DRAFT_RESULTS=1`)
**Run:** 10000000 rows · `hot_row_limit = 100000` · `max_rows_per_file = 1000000` · `--dml-sample 50000` · `insert_batch_rows = 100000` · `warmup_rows = 1000000` · zstd Parquet · **counterbalanced sequential** isolated wiped server per sample (not parallel) · sides measured: **pg + async** · **single sample per side**

Managed PostgreSQL sizes include hot heap + `koldstore.<table>__cl` + mirror
indexes. Cold Parquet is outside the PostgreSQL data directory. Columns are
**PostgreSQL only** and **PG + KoldStore** (WAL-only capture).

## Main comparison

| Metric | PostgreSQL only | PG + KoldStore |
| --- | --- | --- |
| foreground insert throughput | 90501 ops/s | 101861 ops/s |
| sustainable insert throughput | TODO | TODO |
| sustainable update throughput | TODO | TODO |
| insert p99 latency | 1590.83 ms | 1220.24 ms |
| update p99 latency | 44.7 ms | 130.47 ms |
| hot-query p99 latency | 337 µs | 348 µs |
| cold-query p99 latency | 301 µs | 2.98 ms |
| hot+cold query throughput | 4095 ops/s | 632 ops/s |
| cold-only query throughput | 4132 ops/s | 353 ops/s |
| cold files fetched/query | — | TODO |
| cold bytes fetched/query | — | TODO |
| peak memory under workload | TODO | TODO |
| peak RSS during flush | — | 842.84 MiB (before=339.44 MiB, after=842.84 MiB) |
| flush duration | — | 140.28 s |
| flush write throughput | — | TODO (re-run harness) |
| flush write bandwidth | — | TODO (re-run harness) |
| changes_since full-drain throughput | — | TODO (re-run harness) |
| changes_since full-drain duration | — | TODO (re-run harness) |
| CPU seconds per 1M operations | TODO | TODO |
| WAL generated per 1M operations | TODO | TODO |
| local bytes written | TODO | TODO |
| VACUUM duration | 211.52 s | 3.57 s |
| local PostgreSQL storage | 5.85 GiB | 72.23 MiB |
| total hot+cold storage | 5.85 GiB | 670.82 MiB |
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
| insert speed† | 90501 ops/s (11 µs/op) | 101861 ops/s (10 µs/op) |
| update speed† | 76902 ops/s (13 µs/op) | 54219 ops/s (18 µs/op) |
| delete speed† | 124872 ops/s (8 µs/op) | 135288 ops/s (7 µs/op) |
| └ async insert mirror catch-up | — | 34179 ops/s (29 µs/op) |
| └ async update mirror catch-up | — | 1976 ops/s (506 µs/op) |
| └ async delete mirror catch-up | — | 30157 ops/s (33 µs/op) |
| └ async restore mirror catch-up | — | 28869 ops/s (35 µs/op) |
| query hot only (before flush) | 3899 ops/s (256 µs/op) | 3737 ops/s (268 µs/op) |
| query with hot+cold (after flush) | 4095 ops/s (244 µs/op) | 632 ops/s (1583 µs/op) |
| query cold only (after flush) | 4132 ops/s (242 µs/op) | 353 ops/s (2835 µs/op) |
| VACUUM time (after flush) | 211.52 s | 3.57 s |
| dead tuples after workload | 99916 (live=10000000) | 99916 (live=10000000) |
| index storage (hot + __cl) | 414.86 MiB | 11.45 MiB |
| table storage (hot + __cl) | 5.45 GiB | 60.79 MiB |
| └ cold Parquet | — | 598.58 MiB |
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
| Total footprint (hot + cold) | 5.85 GiB → 670.82 MiB | **89% smaller** |
| └ hot in PostgreSQL (heap + `__cl`) | 5.85 GiB → 72.23 MiB | **99% smaller** |
| └ cold Parquet | — → 599 MiB | outside the database |
| Indexes (hot + `__cl`) | 414.86 MiB → 11.45 MiB | **97% smaller** |
| `VACUUM (FULL, ANALYZE)` | 211.52 s → 3.57 s | **59× faster** |

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

It should not. Both columns time the same heap `INSERT` path. After warm-up
the harness advances logical slots to the tip, then pins a slot on
PostgreSQL-only for the timed seed (managed already has its async slot) so
both retain seed WAL the same way. Absolute `wal_files` after warm-up is a
segment *pool*, not slot lag — compare tip lag ≈ 0 and seed file growth.
Expect foreground insert ≈ identical; mirror apply is still the separate
**async insert mirror catch-up** row.

Lab note: the storage harness may set `koldstore.async_mirror_max_retained_bytes = 0`
while the worker is off so 10M-row seeding can retain multi-GiB slot WAL until
the post-insert fence. Production keeps the default 1 GiB health threshold;
crossing it alerts but never blocks apply from draining retained WAL.
