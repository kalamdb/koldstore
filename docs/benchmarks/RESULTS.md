# Latest benchmark results

> **Draft single-sample refresh (2026-08-07).** One isolated pg + async pair at
> published 10M scale (`flushed = 9,900,000`). Skipped the multi-hour
> `changes_since` full drain (`KOLDSTORE_STORAGE_SKIP_CHANGES_SINCE=1`). Not a
> 6-rep publication; treat absolute query TPS as noisy. Re-run with
> `--repetitions 6 --update-results` on a clean tree before marketing numbers.

Published numbers from the most recent storage comparison run(s). Re-run
`scripts/run-storage-comparison.sh --all-sides --repetitions 6 --update-results` to refresh
this file. Each column is measured alone on a wiped + re-initdb pgrx PostgreSQL
(stop → wipe `~/.pgrx/data-<ver>` → prepare → one side). Methodology:
[README.md](README.md).

**When:** 2026-08-07 UTC (pg 09:28:37Z, async 09:39:13Z)
**Git:** `b220d79339ac` (`b220d79339ac08a20e3921125be2a7df8f7005a9`) — draft stamp (`KOLDSTORE_STORAGE_DRAFT_RESULTS=1`)
**Run:** 10000000 rows · `hot_row_limit = 100000` · `max_rows_per_file = 1000000` · `--dml-sample 50000` · `insert_batch_rows = 100000` · `warmup_rows = 1000000` · zstd Parquet · **counterbalanced sequential** isolated wiped server per sample (not parallel) · sides measured: **pg + async** · **single sample per side** · `changes_since` drain skipped

Managed PostgreSQL sizes include hot heap + `koldstore.<table>__cl` + mirror
indexes. Cold Parquet is outside the PostgreSQL data directory. Columns are
**PostgreSQL only** and **PG + KoldStore** (WAL-only capture).

## Main comparison

| Metric | PostgreSQL only | PG + KoldStore |
| --- | --- | --- |
| foreground insert throughput | 100214 ops/s | 96867 ops/s |
| sustainable insert throughput | TODO | TODO |
| sustainable update throughput | TODO | TODO |
| insert p99 latency | 1228.34 ms | 2802.8 ms |
| update p99 latency | 35.1 ms | 107.17 ms |
| hot-query p99 latency | 339 µs | 486 µs |
| cold-query p99 latency | 283 µs | 3.02 ms |
| hot+cold query throughput | 4024 ops/s | 854 ops/s |
| cold-only query throughput | 4039 ops/s | 488 ops/s |
| cold files fetched/query | — | TODO |
| cold bytes fetched/query | — | TODO |
| peak memory under workload | TODO | TODO |
| peak RSS during flush | — | 816.2 MiB (before=338.38 MiB, after=816.20 MiB) |
| flush duration | — | 138 s |
| flush write throughput | — | 71741 rows/s |
| flush write bandwidth | — | 4.34 MiB/s |
| changes_since full-drain throughput | — | — |
| changes_since full-drain duration | — | — |
| CPU seconds per 1M operations | TODO | TODO |
| WAL generated per 1M operations | TODO | TODO |
| local bytes written | TODO | TODO |
| VACUUM duration | 142.22 s | 3.86 s |
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
| insert speed† | 100214 ops/s (10 µs/op) | 96867 ops/s (10 µs/op) |
| update speed† | 86487 ops/s (12 µs/op) | 57131 ops/s (18 µs/op) |
| delete speed† | 37487 ops/s (27 µs/op) | 148369 ops/s (7 µs/op) |
| └ async insert mirror catch-up | — | 34854 ops/s (29 µs/op) |
| └ async update mirror catch-up | — | 2019 ops/s (495 µs/op) |
| └ async delete mirror catch-up | — | 30078 ops/s (33 µs/op) |
| └ async restore mirror catch-up | — | 29634 ops/s (34 µs/op) |
| query hot only (before flush) | 3830 ops/s (261 µs/op) | 3768 ops/s (265 µs/op) |
| query with hot+cold (after flush) | 4024 ops/s (249 µs/op) | 854 ops/s (1171 µs/op) |
| query cold only (after flush) | 4039 ops/s (248 µs/op) | 488 ops/s (2047 µs/op) |
| changes_since full drain‡ | — | — |
| VACUUM time (after flush) | 142.22 s | 3.86 s |
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
| `VACUUM (FULL, ANALYZE)` | 142.22 s → 3.86 s | **37× faster** |

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
