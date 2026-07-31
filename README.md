# KoldStore

> **Keep hot data in PostgreSQL. Move historical rows to Parquet. Shrink the PostgreSQL heap and indexes. Query one table.**

KoldStore is an open-source PostgreSQL tiered-storage extension for application tables that grow forever: messages, audit logs, AI history, notifications, events, and IoT data. By moving historical rows out of PostgreSQL, it reduces the primary heap and index size. In the published benchmark, the smaller hot table also made `VACUUM (FULL, ANALYZE)`—a whole-table rewrite—substantially faster.

Your table remains a normal PostgreSQL heap table. KoldStore keeps the active working set in PostgreSQL, flushes older rows into compressed Parquet on storage you control, and transparently reads hot and cold rows through the original table.

**No replacement database. No proprietary archive format. No application query rewrite.**

> [!WARNING]
> **KoldStore is in early development and is not production-ready.** The core manage, flush, manifest, hot/cold query, and built-in auto-flush scheduling flow works. Recovery, backup/restore, compaction, and schema evolution are still being hardened.

⭐ **Star the repository to follow the project as it moves toward the first production-ready release.**

<p align="center">
  <a href="https://github.com/kalamdb/koldstore/releases"><img src="https://img.shields.io/github/v/release/kalamdb/koldstore?display_name=tag&amp;label=release" alt="Release" /></a>
  <a href="https://hub.docker.com/r/jamals86/pg-koldstore"><img src="https://img.shields.io/docker/pulls/jamals86/pg-koldstore" alt="Docker Pulls" /></a>
  <a href="https://github.com/kalamdb/koldstore/actions/workflows/ci-tests.yml"><img src="https://github.com/kalamdb/koldstore/actions/workflows/ci-tests.yml/badge.svg" alt="CI Tests" /></a>
  <img src="https://img.shields.io/badge/PostgreSQL-15%E2%80%9318-336791" alt="PostgreSQL 15-18" />
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/rust-1.96%2B-orange.svg" alt="Rust 1.96+" /></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License" /></a>
</p>

<p align="center">
  <img
    src="docs/assets/koldstore-demo.gif"
    alt="KoldStore moves historical PostgreSQL rows to Parquet while queries continue using the original table"
    width="900"
  />
</p>

```text
Hot rows  → PostgreSQL heap
Old rows  → Parquet / object storage
Queries   → same PostgreSQL table
```



## What is tiered storage?

**Tiered storage is a data management strategy that assigns data to different
storage media based on performance, frequency of access, and cost.** KoldStore
applies that strategy to rows in one PostgreSQL table:


| Tier     | Where rows live                                    | Optimized for                                                   |
| -------- | -------------------------------------------------- | --------------------------------------------------------------- |
| **Hot**  | PostgreSQL heap and native indexes                 | Active data, low-latency reads, and normal transactional writes |
| **Cold** | Compressed Parquet on filesystem or object storage | Historical data, lower storage cost, and longer retention       |


Applications continue to query the original PostgreSQL table; `KoldMergeScan`
combines visible rows from both tiers. Placement is controlled by the table's
flush policy—currently a hot-row limit with sequence-ordered eviction—rather
than by automatically measuring how often each row is accessed.

## Why KoldStore?

KoldStore extends PostgreSQL instead of replacing it. Applications keep using the same SQL, drivers, ORMs, transactions, replication, and operational tooling while PostgreSQL gains a transparent cold-storage layer for historical rows.

- Keeps the hot working set small so the PostgreSQL heap, indexes, and backup set stay manageable
- Stores history as open Apache Parquet on filesystem, S3/MinIO, GCS, or Azure Blob
- Avoids partition explosion and proprietary archive lock-in
- Adopts incrementally on existing tables — no schema redesign required



### Good fit today

- Messages and chat history
- Audit logs and event streams
- AI memory and model outputs
- Notifications
- User activity and IoT telemetry



### Not a good fit yet

- Payment ledgers and account balances
- Inventory or other highly mutable cold state
- FK-heavy relational models that need global uniqueness across hot + cold
- Workloads that need cold rows to stay as fast as B-tree point lookups



## Compared with other approaches


| Approach                    | What you keep                                 | Tradeoff                                                  |
| --------------------------- | --------------------------------------------- | --------------------------------------------------------- |
| **KoldStore**               | Same PostgreSQL table, SQL, drivers, and ORMs | Older rows move to open Parquet; hot heap stays small     |
| Bigger disk / partitions    | Familiar ops                                  | History still inflates heap, indexes, and backups         |
| Time-series or analytics DB | Columnar scan performance                     | New system, new query model, app migration                |
| Custom table AM / fork      | Deeper engine control                         | Leaves stock PostgreSQL storage and tooling               |
| Proprietary archive tier    | Managed cold storage                          | Vendor format lock-in                                     |




## Storage and whole-table maintenance wins at a glance

KoldStore is a **storage lifecycle tool**, not a universal query accelerator. After older rows are flushed, PostgreSQL keeps a smaller hot working set and smaller indexes; cold data lives in zstd Parquet outside the primary heap. The maintenance figure below is specifically `VACUUM (FULL, ANALYZE)`, which rewrites the whole table. It does not measure routine autovacuum, which was disabled for the benchmark.




| Result                              | Before → after flush | Tradeoff             |
| ----------------------------------- | -------------------- | -------------------- |
| Total footprint (hot + cold)        | 5.85 GiB → 671 MiB   | **89% smaller**      |
| └ hot in PostgreSQL (heap + `__cl`) | 5.85 GiB → 72 MiB    | **99% smaller**      |
| └ cold Parquet                      | — → 599 MiB          | outside the database |
| Indexes (hot + `__cl`)              | 415 MiB → 11.5 MiB   | **97% smaller**      |
| `VACUUM (FULL, ANALYZE)`            | 174.36 s → 3.59 s    | **49× faster**       |


Sample: 10M wide rows, `hot_row_limit = 100000`, `--dml-sample 50000`,
`--warmup-rows 1000000`, `max_rows_per_file = 1000000` (local PG16.13
`release-pg`, 2026-07-31, single pgrx instance). Each side gets a fresh pgrx
server, an untimed 1M warm-up, then the timed run. Managed PostgreSQL sizes
include the hot heap **and** `koldstore.<table>__cl` plus its indexes. Full
tables: [docs/benchmarks/RESULTS.md](docs/benchmarks/RESULTS.md).

### Latest UPDATE verification

Post-optimization PostgreSQL 16 smoke measurements put WAL-capture foreground
UPDATE at heap parity on both tested statement shapes:


| UPDATE workload                    | PostgreSQL only | KoldStore (WAL) | Difference       |
| ---------------------------------- | --------------- | --------------- | ---------------- |
| Single-row pgbench throughput      | 26,482 ops/s    | 26,152 ops/s    | **1.25% lower**  |
| Single-row pgbench p95             | 0.211 ms        | 0.213 ms        | **0.95% higher** |
| 1k-row batch foreground throughput | 77,166 ops/s    | 76,030 ops/s    | **1.47% lower**  |
| Async mirror catch-up              | —               | 49,358 ops/s    | deferred work    |


The single-row run used 10k seeded rows, four clients, and five seconds with the
background worker enabled. The batch run used 100k rows and a 50k-row UPDATE
sample. Mirror and source row counts matched after catch-up.

These are focused single-run verification measurements, not replacement 10M
publication results. Release publication requires six clean-tree,
counterbalanced samples plus worker-on backlog and drain metrics. See the
[benchmark methodology](docs/benchmarks/README.md).

### Published 10M-row snapshot

This storage-scale run reports foreground DML separately from mirror catch-up.
It is a clean-tree single sample (draft publication); release publication still
prefers six counterbalanced repetitions. KoldStore commits the source heap
first; a database worker applies committed WAL afterward. Query phases use the
fair harness order (PG cold lookups before `VACUUM FULL`; managed cold after
flush).


| Operation           | PostgreSQL only | KoldStore (WAL) | Trade-off                                          |
| ------------------- | --------------- | --------------- | -------------------------------------------------- |
| INSERT              | 60,928 ops/s    | 92,097 ops/s    | noise / order — not a product win (same full-heap seed) |
| UPDATE              | 68,449 ops/s    | 29,892 ops/s    | single sample; **56% lower**                       |
| DELETE              | 122,535 ops/s   | 132,737 ops/s   | single-sample — do not claim faster                |
| Hot-only PK lookup  | 3,894 ops/s     | 2,392 ops/s     | single-sample noise (pre-flush full heap)          |
| Hot+cold PK lookup  | 4,043 ops/s     | 821 ops/s       | **80% slower** (Parquet vs full-heap baseline)     |
| Cold-only PK lookup | 4,085 ops/s     | 496 ops/s       | **88% slower** (Parquet vs full-heap baseline)     |


Async mirror catch-up measured 28,758 INSERT, 814 UPDATE, 21,812 DELETE, and
18,313 restore operations per second in this run. The focused UPDATE catch-up
result in the verification table above is 49,358 ops/s. Timed INSERT seeds an
empty table to 10M on every side — `hot_row_limit` does not make managed INSERT
faster. Full methodology:
[docs/benchmarks/](docs/benchmarks/README.md).

Managed tables use committed-WAL mirror capture only. Foreground DML writes the
heap; a database worker applies PK-only WAL with a 100 ms polling interval and
bounded immediate retry bursts. Call `koldstore.wait_for_async_mirror()` for a
strong read boundary; `flush_table` fences automatically. Authoritative mirror
`seq` is allocated only by the serialized applier and is the exclusive
`changes_since` cursor. `CREATE EXTENSION` and the first managed table create
the publication and slot automatically; only `wal_level=logical` requires
administrator setup.

## How it works

1. KoldStore registers the table and creates a small latest-state change-log mirror (one metadata row per primary key) fed by committed WAL.
2. A built-in database worker auto-flushes when hot rows exceed `hot_row_limit` (per-table `auto_flush`, default `true`). You can also call `flush_table` manually.
3. Flush moves older rows to Parquet and prunes them from the hot heap when safe.
4. `SELECT` on the original table uses `KoldMergeScan` so the newest visible row wins.

Details: [Architecture](docs/architecture.md) · [Capture](docs/architecture/mirror-capture-modes.md) · [Manage](docs/architecture/manage-table.md) · [Flush](docs/architecture/flushing-table.md) · [Scan](docs/architecture/scanning-table.md) · [Scheduling](docs/operations/scheduling.md)

```mermaid
flowchart TD
  App[Application / ORM] --> T[Original PostgreSQL table]
  T --> Scan[KoldMergeScan]
  Scan --> Hot[Hot PG heap]
  Scan --> Cold[Manifest → Parquet / S3]
```



For example, assume messages `1` and `2` have been flushed to Parquet while
the newer message `3` remains in PostgreSQL. The application still issues one
normal query against `messages`:

```sql
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT id, body
FROM messages
WHERE id IN (1, 3);
```

Captured from PostgreSQL 15 after flushing rows `1` and `2` while row `3`
remained hot:

```text
Custom Scan (KoldMergeScan) on messages (actual rows=2 loops=1)
  Filter: (id = ANY ('{1,3}'::bigint[]))
  Rows Removed by Filter: 1
  Hot Plan: Bitmap Heap Scan
  Mirror Tombstones: 0
  Mirror Overrides: 0
  Emit path: merge_stream
  Peak Hot Batch Rows: 1
  Seen Keys: 1
  Result rows: 3
  Candidate segments: 1
  Segments pruned by scope: 0
  Segments pruned by catalog index: 0
  Parquet segments opened: 1
  Row groups read: 1
  Row groups skipped: 0 of 1
  Bytes fetched: 1.5 kB
  Manifest: readme_capture/messages/manifest.json, source=catalog, 0.002 ms
  Cold storage: type=filesystem, base=/tmp/koldstore-readme-explain-storage
  Cold segments: considered=1, pruned_scope=0, pruned_catalog_index=0, pruned_bloom=0, opened=1
  Cold row groups: total=1, selected=1, skipped=0, bloom_filters_fetched=0
  Cold projection: id, body
  Parquet segment: readme_capture/messages/001/segment-0001-6afccda7.parquet, 1672 bytes, 2 rows, 2.897 ms
    Parquet I/O: footer-first, range_gets=3, bytes_read=1498, 89.6% of object
    Row groups: total=1, selected=[0], skipped=0, stats_pruned=false
    Bloom: not_requested
```

KoldStore merges both sources and resolves newer mirror versions before rows
reach the rest of the PostgreSQL plan. Here `Result rows: 3` is the internal
merged candidate count; the SQL filter removes row `2`, producing the two
final rows reported on the first line.

## Required preload

Add KoldStore to `shared_preload_libraries` **before** managing tables:

```conf
shared_preload_libraries = 'koldstore'
```

This is required so queries always include both hot and cold rows. Without
shared preload, managed `SELECT`s can silently fall back to hot-only heap
scans after flush and miss cold rows. `session_preload_libraries` is not
sufficient — restart PostgreSQL after changing the list (reload is not enough).

Confirm with:

```sql
SHOW shared_preload_libraries;       -- must include koldstore
SELECT koldstore.preload_status();   -- loaded_via_shared_preload = true
```

Details: [Quickstart](docs/quickstart.md#0-shared-preload-required) · [SQL API](docs/sql-api.md).

## Try it in five minutes

Published release images ship PostgreSQL 16 with `koldstore` **shared-preloaded**,
`wal_level=logical`, and `CREATE EXTENSION` applied on first init:

```bash
docker pull jamals86/pg-koldstore:latest
docker run --rm -e POSTGRES_PASSWORD=postgres -p 5432:5432 jamals86/pg-koldstore:latest
psql postgres://postgres:postgres@127.0.0.1:5432/koldstoredb
```

Confirm preload and WAL level (required for manage_table / hot+cold reads):

```sql
SHOW shared_preload_libraries;       -- must include koldstore
SHOW wal_level;                      -- must be logical
SELECT koldstore.preload_status();
```

```sql
CREATE EXTENSION IF NOT EXISTS koldstore;

SELECT koldstore.register_storage(
  name         => 'local-dev',
  storage_type => 'filesystem',
  base_path    => '/tmp/koldstore-demo',
  credentials  => '{}'::jsonb,
  config       => '{}'::jsonb
);

CREATE TABLE messages (
  id bigint PRIMARY KEY,
  body text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE messages SET (
  koldstore_enabled = true,
  koldstore_storage = 'local-dev',
  koldstore_hot_row_limit = 1000,
  koldstore_min_flush_rows = 1,
  koldstore_max_rows_per_file = 1000
);

INSERT INTO messages (id, body)
SELECT gs, 'row ' || gs FROM generate_series(1, 1012) AS gs;

-- Fence so the WAL applier has assigned authoritative mirror seq values.
SELECT koldstore.wait_for_async_mirror();

-- Change feed: exclusive seq cursor over hot mirror + cold Parquet metadata.
-- One row per primary key; op is 1=insert, 2=update, 3=delete.
-- Page with LIMIT and advance since_seq from the highest seq you consumed.
SELECT seq, op, pk, deleted, source
FROM koldstore.changes_since(
  table_name => 'messages'::regclass,
  since_seq  => 0,
  limit_rows => 100
);

-- Or rewind to the newest N changes (KalamDB last_rows); delivered oldest→newest.
SELECT seq, op, pk, deleted, source
FROM koldstore.changes_since(
  table_name => 'messages'::regclass,
  since_seq  => 0,
  limit_rows => 1000,
  last_rows  => 50
);

-- Optional: run a policy flush now. Otherwise the built-in worker auto-flushes
-- when hot rows exceed hot_row_limit.
SELECT koldstore.flush_table(table_name => 'messages'::regclass);

-- After flush, the same cursor still returns flushed latest-state from cold.
SELECT seq, op, pk, deleted, source
FROM koldstore.changes_since('messages'::regclass, 0, 100);

SELECT count(*) FROM messages;  -- still 1012 via KoldMergeScan
SELECT jsonb_pretty(koldstore.describe_table(table_name => 'messages'::regclass));
```

`since_seq = 0` means from the start of retained history. A positive cursor older
than the retained cold/hot floor raises a retention-gap error. Mirror inspection,
job UUIDs, `EXPLAIN`, shared/user tables, and storage backends:
[docs/quickstart.md](docs/quickstart.md) · [SQL API](docs/sql-api.md) ·
[Change API](docs/roadmap.md#change-api-changes_since).

Auto-flush runs on the built-in database worker (`koldstore.flush_check_interval_seconds`). To control flushes yourself (for example with `pg_cron`), disable it with `SELECT koldstore.set_table_auto_flush('messages'::regclass, false)` and schedule `koldstore.flush_table`: [docs/operations/scheduling.md](docs/operations/scheduling.md).

To build from this repo instead, use `docker/run.sh` (compiles the extension).

## Requirements

- PostgreSQL 15–18
- `shared_preload_libraries` must include `koldstore` (see [Required preload](#required-preload))
- Managed tables need a primary key
- Supported column types today: `boolean`, integer types, `real`, `double precision`, `text`, `varchar`, `uuid`, `jsonb`, `timestamptz`
- Local development uses `pgrx`; Docker is for packaging and smoke checks



## Limitations

- Not production-ready
- Cold storage is not WAL-protected — back up PostgreSQL and the cold prefix together
- `UNIQUE` / foreign keys are enforced on **hot rows only** after flush ([details](docs/limitations.md#unique-and-foreign-key-constraints))
- PostgreSQL indexes cover hot rows only
- Unavailable cold storage fails the query instead of returning partial hot-only results
- Export/import, compaction, schema evolution, and PK changes are still being built

Full list: [docs/limitations.md](docs/limitations.md).

## Roadmap

Priority after the 0.1 hot/cold baseline:

1. **Scoped storage** — store each `scope_column` value under its own cold folder (`{namespace}/{table}/{scopeId}/…`), so tenant/user data stays physically separated and easier to prune, backup, or delete independently
2. **FILE datatype** — KalamDB-style column type that stores file payloads in cold storage rather than in the heap (hot rows keep a compact reference; upload/fetch use the table’s cold backend)
3. **Stream Table Changes** — stream changes to Kalam gateway for Websocket real-time notifications
4. **Compaction** — combine small cold segments into larger files to cut object-store chatter and improve scan efficiency
5. **Backup / export** — first-class dump and restore that understands KoldStore: coordinated PostgreSQL + cold-object backups, and table/scope archive export/import of managed hot+cold data

Also planned: faster cold PK lookups, and time-based / predicate flush policies.

Tracked in [docs/roadmap.md](docs/roadmap.md).

## Contributing

KoldStore is early. Stars, issues, and PRs all help shape the first production-ready release.

Good ways to help:

1. Try the Docker demo and file issues when something breaks or is unclear
2. Share a workload that fits (or does not fit) the guidance above
3. Improve docs, tests, or cold-path performance

Development loop and crate layout:

```bash
cargo nextest run --workspace --no-default-features \
  --exclude e2e --exclude examples --exclude storage-comparison \
  --exclude pg-koldstore-benchmarks --exclude koldstore-memory-tests \
  --exclude stress
cargo pgrx install -p pg_koldstore --no-default-features --features "pg16 s3"
scripts/run-pg-e2e.sh 16
```

- [Development guide](docs/development.md)
- [Crate architecture](docs/architecture/crate-architecture.md)
- [SQL API](docs/sql-api.md)
- [Code of conduct](CODE_OF_CONDUCT.md)



## License

Apache License 2.0. Copyright 2026 KalamDB.

See [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).
