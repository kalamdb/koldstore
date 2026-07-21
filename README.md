# KoldStore

> **Keep hot data in PostgreSQL. Move historical rows to Parquet. Query one table.**

KoldStore is an open-source PostgreSQL tiered-storage extension for application tables that grow forever: messages, audit logs, AI history, notifications, events, and IoT data.

Your table remains a normal PostgreSQL heap table. KoldStore keeps the active working set in PostgreSQL, flushes older rows into compressed Parquet on storage you control, and transparently reads hot and cold rows through the original table.

**No replacement database. No proprietary archive format. No application query rewrite.**

> [!WARNING]
> **KoldStore is in early development and is not production-ready.** The core manage, flush, manifest, hot/cold query, and built-in auto-flush scheduling flow works. Recovery, backup/restore, compaction, and schema evolution are still being hardened.

⭐ **Star the repository to follow the project as it moves toward the first production-ready release.**

<p align="center">
  <a href="https://github.com/kalamdb/koldstore/releases"><img src="https://img.shields.io/github/v/release/kalamdb/koldstore?display_name=tag&amp;label=release" alt="Release" /></a>
  <a href="https://github.com/kalamdb/koldstore/pkgs/container/pg-koldstore"><img src="https://img.shields.io/badge/docker-ghcr.io%2Fkalamdb%2Fpg--koldstore-2496ED?logo=docker&amp;logoColor=white" alt="Docker image" /></a>
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

| Tier | Where rows live | Optimized for |
| --- | --- | --- |
| **Hot** | PostgreSQL heap and native indexes | Active data, low-latency reads, and normal transactional writes |
| **Cold** | Compressed Parquet on filesystem or object storage | Historical data, lower storage cost, and longer retention |

Applications continue to query the original PostgreSQL table; `KoldMergeScan`
combines visible rows from both tiers. Placement is controlled by the table's
flush policy—currently a hot-row limit with sequence-ordered eviction—rather
than by automatically measuring how often each row is accessed.

## Why KoldStore?

KoldStore extends PostgreSQL instead of replacing it. Applications keep using the same SQL, drivers, ORMs, transactions, replication, and operational tooling while PostgreSQL gains a transparent cold-storage layer for historical rows.

- Keeps the hot working set small so indexes, VACUUM, and backups stay manageable
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

| Approach | What you keep | Tradeoff |
| --- | --- | --- |
| **KoldStore** | Same PostgreSQL table, SQL, drivers, and ORMs | Older rows move to open Parquet; hot heap stays small |
| Bigger disk / partitions | Familiar ops | History still inflates heap, indexes, VACUUM, and backups |
| Time-series or analytics DB | Columnar scan performance | New system, new query model, app migration |
| Custom table AM / fork | Deeper engine control | Leaves stock PostgreSQL storage and tooling |
| Proprietary archive tier | Managed cold storage | Vendor format lock-in |

## Storage wins at a glance

KoldStore is a **storage lifecycle tool**, not a universal query accelerator. After older rows are flushed, PostgreSQL keeps a smaller hot working set; cold data lives in zstd Parquet outside the primary heap.

<p align="center">
  <img src="docs/assets/benchmark-storage-wins.svg" alt="After flush: 72 MiB hot in PostgreSQL plus 599 MiB cold Parquet (671 MiB total) versus 5.85 GiB all-in-PostgreSQL; indexes 97% smaller; VACUUM FULL 43× faster" width="900" />
</p>

| Result | Before → after flush | Tradeoff |
| --- | --- | --- |
| Total footprint (hot + cold) | 5.85 GiB → 671 MiB | **89% smaller** |
| └ hot in PostgreSQL (heap + `__cl`) | 5.85 GiB → 72 MiB | **99% smaller** |
| └ cold Parquet | — → 599 MiB | outside the database |
| Indexes (hot + `__cl`) | 415 MiB → 11.5 MiB | **97% smaller** |
| `VACUUM (FULL, ANALYZE)` | 149.29 s → 3.44 s | **43× faster** |

Sample: 10M wide rows, `hot_row_limit = 100000`, `--dml-sample 50000`,
`--warmup-rows 1000000` (local PG16.13 `release-pg`, 2026-07-20). Each side
gets a fresh pgrx server, an untimed 1M warm-up, then the timed run. Managed
PostgreSQL sizes include the hot heap **and** `koldstore.<table>__cl` plus its
indexes. Full tables: [docs/benchmarks/RESULTS.md](docs/benchmarks/RESULTS.md).

### Latest UPDATE verification

Post-optimization PostgreSQL 16 smoke measurements put async foreground UPDATE
at heap parity on both tested statement shapes:

| UPDATE workload | PostgreSQL only | KoldStore async | Difference |
| --- | ---: | ---: | ---: |
| Single-row pgbench throughput | 26,482 ops/s | 26,152 ops/s | **1.25% lower** |
| Single-row pgbench p95 | 0.211 ms | 0.213 ms | **0.95% higher** |
| 1k-row batch foreground throughput | 77,166 ops/s | 76,030 ops/s | **1.47% lower** |
| Async mirror catch-up | — | 49,358 ops/s | deferred work |

The single-row run used 10k seeded rows, four clients, and five seconds with the
background worker enabled. The batch run used 100k rows and a 50k-row UPDATE
sample. Mirror and source row counts matched after catch-up. Strict single-row
UPDATE measured 0.294 ms p95 versus its 0.208 ms heap baseline (**1.41×**),
inside the separate 2.00× strict consistency gate.

These are focused single-run verification measurements, not replacement 10M
publication results. Release publication requires six clean-tree,
counterbalanced samples plus worker-on backlog and drain metrics. See the
[benchmark methodology](docs/benchmarks/README.md).

### Published 10M-row snapshot (historical)

This pre-optimization storage-scale run reports foreground DML separately from
mirror catch-up. It was a dirty-tree single sample and remains here for 10M-row
storage context until the guarded repeated publication run replaces it. Async
commits the source heap first; strict mode includes mirror maintenance in the
application transaction.

| Operation | PostgreSQL only | KoldStore async | Trade-off |
| --- | ---: | ---: | ---: |
| INSERT | 94,302 ops/s | 107,030 ops/s | ≈ within noise (not a product win) |
| UPDATE | 69,239 ops/s | 52,446 ops/s | historical pre-optimization single sample; **24% lower** |
| DELETE | 119,350 ops/s | 179,882 ops/s | single-sample — do not claim faster |
| Hot-only PK lookup | 1,800 ops/s | 1,825 ops/s | ≈ same |
| Hot+cold PK lookup | 1,793 ops/s | 1,529 ops/s | **15% slower** |
| Cold-only PK lookup | 1,762 ops/s | 1,242 ops/s | **30% slower** |

Async mirror catch-up measured 30,173 INSERT, 914 UPDATE, 28,417 DELETE, and
24,906 restore operations per second in that historical run. The current
focused UPDATE catch-up result above is 49,358 ops/s. Published runs warm up
before timing so cold-start after install does not fake an async insert win.
Full methodology and strict-mode column:
[docs/benchmarks/](docs/benchmarks/README.md).

Managed tables support two mirror paths. The default `strict` mode writes the
latest-state mirror in the heap transaction for immediate consistency. The
opt-in `async` mode commits the heap first; a database worker applies committed
PK-only WAL with a 100 ms polling interval and bounded immediate retry bursts,
while an explicit fence remains available for strong reads. Async UPDATE uses a
direct set-based update with a conflict-safe insert-missing fallback; application
DML does not run a worker-kick trigger. This reduces foreground overhead while
reporting catch-up as separate work. `CREATE EXTENSION` and the first async
`manage_table` create the publication and slot automatically; only
`wal_level=logical` requires administrator setup.

## How it works

1. `manage_table` registers the table and creates a small latest-state change-log mirror (one metadata row per primary key). Choose strict transactional capture or opt-in async committed-WAL capture.
2. A built-in database worker auto-flushes when hot rows exceed `hot_row_limit` (per-table `auto_flush`, default `true`). You can also call `flush_table` manually.
3. Flush moves older rows to Parquet and prunes them from the hot heap when safe.
4. `SELECT` on the original table uses `KoldMergeScan` so the newest visible row wins.

Details: [Architecture](docs/architecture.md) · [Capture modes](docs/architecture/mirror-capture-modes.md) · [Manage](docs/architecture/manage-table.md) · [Flush](docs/architecture/flushing-table.md) · [Scan](docs/architecture/scanning-table.md) · [Scheduling](docs/operations/scheduling.md)

```mermaid
flowchart TD
  App[Application / ORM] --> T[Original PostgreSQL table]
  T --> Scan[KoldMergeScan]
  Scan --> Hot[Hot PG heap]
  Scan --> Cold[Manifest → Parquet / S3]
```

## Try it in five minutes

Published release images ship PostgreSQL 16 with `koldstore` already installed:

```bash
docker pull ghcr.io/kalamdb/pg-koldstore:latest
docker run --rm -e POSTGRES_PASSWORD=postgres -p 5432:5432 ghcr.io/kalamdb/pg-koldstore:latest
psql postgres://postgres:postgres@127.0.0.1:5432/koldstoredb
```

The same tags are also published to Docker Hub as `jamals86/pg-koldstore`
(`linux/amd64` and `linux/arm64`).
```sql
CREATE EXTENSION IF NOT EXISTS koldstore;

SELECT koldstore.register_storage(
  name         => 'local-dev',
  storage_type => 'filesystem',
  base_path    => '/koldstore/data',
  credentials  => '{}'::jsonb,
  config       => '{}'::jsonb
);

CREATE TABLE messages (
  id bigint PRIMARY KEY,
  body text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO messages (id, body)
SELECT gs, 'row ' || gs FROM generate_series(1, 1012) AS gs;

SELECT koldstore.manage_table(
  table_name         => 'messages',
  storage            => 'local-dev',
  hot_row_limit      => 100000,
  min_flush_rows     => 1000,
  max_rows_per_file  => 1000,
  migration_order_by => 'id',
  auto_flush         => true   -- default; set false to drive flush yourself
);

-- Optional: force a flush now. Otherwise the built-in worker auto-flushes
-- when hot rows exceed hot_row_limit.
SELECT koldstore.flush_table(table_name => 'messages');

SELECT count(*) FROM messages;  -- still 1012 via KoldMergeScan
SELECT jsonb_pretty(koldstore.describe_table(table_name => 'messages'));
```

Mirror inspection, job UUIDs, `EXPLAIN`, shared/user tables, and storage backends: [docs/quickstart.md](docs/quickstart.md).

Auto-flush runs on the built-in database worker (`koldstore.flush_check_interval_seconds`). To control flushes yourself (for example with `pg_cron`), set `auto_flush => false` and schedule `koldstore.flush_table`: [docs/operations/scheduling.md](docs/operations/scheduling.md).

To build from this repo instead, use `docker/run.sh` (compiles the extension).

## Requirements

- PostgreSQL 15–18
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

Grouped after the 0.1 hot/cold baseline:

- **Operations** — time-based / predicate flush policies, coordinated backup/restore, import/export
- **Query path** — faster cold PK lookups, streaming `KoldMergeScan`, broader planner pushdown
- **Change APIs** — public `changes_since` / change-cursor SQL on the existing `__cl` mirror
- **Storage** — compaction, deleted-index in manifest, storage file datatype

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
scripts/run-pg-e2e.sh 16 --mode strict
scripts/run-pg-e2e.sh 16 --mode async
```

- [Development guide](docs/development.md)
- [Crate architecture](docs/architecture/crate-architecture.md)
- [SQL API](docs/sql-api.md)
- [Code of conduct](CODE_OF_CONDUCT.md)

## License

Apache License 2.0. Copyright 2026 KalamDB.

See [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).
