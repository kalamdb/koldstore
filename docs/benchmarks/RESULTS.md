# Latest benchmark results

Published numbers from the most recent storage comparison run(s). Re-run
`scripts/run-storage-comparison.sh --all-sides --repetitions 6 --update-results` to refresh
this file. Each column is measured alone on a wiped + re-initdb pgrx PostgreSQL
(stop → wipe `~/.pgrx/data-<ver>` → prepare → one side). Methodology:
[README.md](README.md).

**When:** 2026-08-01 UTC (pg 08:49:29Z, async 09:00:17Z)
**Git:** `328a5c6444b6` (`328a5c6444b60a835285f80e58544255b8b68fcf`)
**Run:** 10000000 rows · `hot_row_limit = 100000` · `max_rows_per_file = 1000000` · `--dml-sample 50000` · `insert_batch_rows = 100000` · `warmup_rows = 1000000` · zstd Parquet · **counterbalanced sequential** isolated wiped server per sample (not parallel) · sides measured: **pg + async** · **single sample per side**

Managed PostgreSQL sizes include hot heap + `koldstore.<table>__cl` + mirror
indexes. Cold Parquet is outside the PostgreSQL data directory. Columns are
**PostgreSQL only** and **PG + KoldStore** (WAL-only capture).

## Main comparison

| Metric | PostgreSQL only | PG + KoldStore |
| --- | --- | --- |
| foreground insert throughput | 100809 ops/s | 100818 ops/s |
| sustainable insert throughput | TODO | TODO |
| sustainable update throughput | TODO | TODO |
| insert p99 latency | 1224.07 ms | 1198.19 ms |
| update p99 latency | 36.07 ms | 113.36 ms |
| hot-query p99 latency | 355 µs | 438 µs |
| cold-query p99 latency | 306 µs | 1.71 ms |
| hot+cold query throughput | 3997 ops/s | 1055 ops/s |
| cold-only query throughput | 4032 ops/s | 662 ops/s |
| cold files fetched/query | — | TODO |
| cold bytes fetched/query | — | TODO |
| peak memory under workload | TODO | TODO |
| peak RSS during flush | — | 821.94 MiB (before=333.20 MiB, after=821.94 MiB) |
| flush duration | — | 141.53 s (69949 rows/s) |
| CPU seconds per 1M operations | TODO | TODO |
| WAL generated per 1M operations | TODO | TODO |
| local bytes written | TODO | TODO |
| VACUUM duration | 158.7 s | 3.24 s |
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
| insert speed† | 100809 ops/s (10 µs/op) | 100818 ops/s (10 µs/op) |
| update speed† | 81791 ops/s (12 µs/op) | 55164 ops/s (18 µs/op) |
| delete speed† | 130331 ops/s (8 µs/op) | 145691 ops/s (7 µs/op) |
| └ async insert mirror catch-up | — | 32662 ops/s (31 µs/op) |
| └ async update mirror catch-up | — | 1689 ops/s (592 µs/op) |
| └ async delete mirror catch-up | — | 28661 ops/s (35 µs/op) |
| └ async restore mirror catch-up | — | 25414 ops/s (39 µs/op) |
| query hot only (before flush) | 3851 ops/s (260 µs/op) | 4076 ops/s (245 µs/op) |
| query with hot+cold (after flush) | 3997 ops/s (250 µs/op) | 1055 ops/s (948 µs/op) |
| query cold only (after flush) | 4032 ops/s (248 µs/op) | 662 ops/s (1511 µs/op) |
| VACUUM time (after flush) | 158.7 s | 3.24 s |
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
| `VACUUM (FULL, ANALYZE)` | 158.7 s → 3.24 s | **49× faster** |

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

It should not. Both columns time the same heap `INSERT` path. The harness:

1. Advances logical slots to the tip after warm-up (lag ≤ 1 WAL segment)
2. Pins a logical slot on PostgreSQL-only for the timed seed (managed already
   has its real async slot)
3. So both retain newly written seed WAL the same way

Absolute `wal_files` after warm-up stays in the tens because PostgreSQL keeps a
recycled segment **pool** near recent write volume — that is not slot lag.
What must match is tip lag ≈ 0, then similar file growth during the seed.

This run:

| Check | PostgreSQL only (pinned) | PG + KoldStore |
| --- | --- | --- |
| Pre-seed wal_files / slot lag | 55 / 0 B | 58 / ~16 MiB (1 segment) |
| Timed insert | **100809 ops/s** | **100818 ops/s** |
| WAL written | 9.06 GiB | 9.06 GiB |
| WAL files during seed | 55 → 581 (+526) | 58 → 582 (+524) |
| Batch p50 / p99 | 971 / 1224 ms | 972 / 1198 ms |
| Checkpoint write/sync | 81 s / 1.5 s | 81 s / 1.3 s |

Foreground insert is a wash. Mirror apply is still the separate catch-up row
(~33k rows/s). Product wins remain storage size and VACUUM, not INSERT speed.

Lab note: the storage harness may set `koldstore.async_mirror_max_retained_bytes = 0`
while the worker is off so 10M-row seeding can retain multi-GiB slot WAL until
the post-insert fence. Production keeps the default 1 GiB health threshold;
crossing it alerts but never blocks apply from draining retained WAL.
