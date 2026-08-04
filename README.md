# KoldStore

> **An open research project exploring transparent hot/cold storage for PostgreSQL application tables.**

**Keep active rows in PostgreSQL. Move historical rows to Parquet. Query the original table.**

⭐ **Star the repository to follow the experiments, benchmarks, design decisions, and progress toward a production-ready release.**

<p align="center">
  <a href="https://github.com/kalamdb/koldstore/releases"><img src="https://img.shields.io/github/v/release/kalamdb/koldstore?display_name=tag&label=release" alt="Release" /></a>
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
Hot rows  → PostgreSQL heap and native indexes
Old rows  → Compressed Parquet on object storage
Queries   → The original PostgreSQL table
```

---

## The research question

PostgreSQL application tables often grow indefinitely:

- Messages and conversation history
- Audit logs and events
- AI prompts, outputs, and tool calls
- Notifications and activity feeds
- IoT and user telemetry

Only a small part of that data normally needs PostgreSQL-level latency. The rest must remain accessible, but it does not always need to occupy the primary heap, indexes, replicas, backups, and expensive database storage.

KoldStore explores a simple question:

> **Can PostgreSQL keep only the active working set while transparently extending the same table into open, low-cost object storage?**

The goal is to preserve the PostgreSQL interface developers already use:

- The same table
- The same SQL
- The same drivers and ORMs
- Normal PostgreSQL transactions for hot data
- Open Parquet files for historical data

**No replacement database. No proprietary archive format. No application query rewrite.**

> [!WARNING]
> **KoldStore is an early-stage research project and is not production-ready.**
>
> The core manage, migration, WAL capture, flush, manifest, hot/cold query, change-feed, and built-in scheduling flows work. Recovery, coordinated backup and restore, compaction, schema evolution, and several cold-read optimizations are still being hardened.

---

## Why follow the project?

KoldStore publishes both successful results and current trade-offs.

This repository is where we are exploring:

- How much PostgreSQL heap and index storage can be removed safely
- How hot and cold rows can appear as one logical table
- How PostgreSQL planner paths should represent object-storage access
- How to avoid opening Parquet when hot rows already answer a query
- How to accelerate cold primary-key and ordered `LIMIT` queries
- How committed WAL can maintain a latest-state mirror asynchronously
- How flushing can remain resumable and crash-safe
- How updates, deletes, tombstones, and restored rows interact with immutable files
- How backup, restore, compaction, and schema evolution should work
- Where transparent tiered storage is useful—and where it is the wrong solution

Benchmark regressions and unresolved correctness questions are treated as project results, not hidden implementation details.

---

## Project status

| Area | Current status |
|---|---|
| Existing-table adoption | Working |
| Clean application-table schema | Working |
| Committed-WAL mirror capture | Working |
| Manual and automatic flushing | Working |
| Filesystem storage | Working |
| S3 / MinIO, GCS, Azure Blob | Implemented / being hardened |
| Transparent hot/cold `SELECT` | Working |
| Manifest and catalog pruning | Working |
| `changes_since` cursor | Working |
| Cold point-lookups | Working, slower than native B-tree |
| Ordered progressive cold reads | Active research |
| Compaction | Planned |
| Coordinated backup and restore | Planned |
| Schema evolution | Planned |
| Production readiness | Not yet |

See the [roadmap](docs/roadmap.md) and current [limitations](docs/limitations.md).

---

## What is tiered storage?

Tiered storage places data on different storage media according to its performance and retention requirements.

KoldStore applies that model to rows in one PostgreSQL table:

| Tier | Where rows live | Optimized for |
|---|---|---|
| **Hot** | PostgreSQL heap and native indexes | Active data, transactional writes, low-latency reads |
| **Cold** | Compressed Parquet on filesystem or object storage | Historical data, lower cost, long retention |

Applications continue querying the original table.

`KoldMergeScan` combines visible rows from PostgreSQL with matching Parquet segments. Row placement is currently controlled by an explicit flush policy—primarily a hot-row limit with sequence-ordered eviction.

KoldStore does **not** currently measure row-access frequency automatically.

---

## What KoldStore is—and is not

KoldStore is a **storage lifecycle experiment for PostgreSQL application data**.

It is not intended to replace:

- PostgreSQL indexes for low-latency point lookups
- An OLAP engine for large analytical workloads
- An immutable financial ledger
- A globally distributed database
- A transactional object store

### Good fit today

- Messages and chat history
- Audit logs and event streams
- AI history, tool calls, and model outputs
- Notifications and activity feeds
- Append-heavy user or tenant history
- IoT and application telemetry
- Tables where older rows become mostly immutable

### Not a good fit yet

- Payment ledgers and account balances
- Inventory or frequently mutated historical state
- FK-heavy relational models requiring global hot+cold enforcement
- Workloads where cold rows must match native B-tree lookup latency
- Systems that cannot tolerate object-storage availability affecting queries

---

## Why not use existing approaches?

| Approach | What remains familiar | Main trade-off |
|---|---|---|
| **KoldStore** | Original PostgreSQL table, SQL, drivers, and ORMs | Cold reads use Parquet and object storage |
| Bigger PostgreSQL disk | Existing operations | History still expands heaps, indexes, replicas, and backups |
| Native partitioning | PostgreSQL-native lifecycle control | History remains inside PostgreSQL and partition counts can grow |
| Archive tables and scripts | Simple components | Application queries and lifecycle logic become custom |
| Analytics or time-series database | Strong columnar scanning | Additional system, migration, and query model |
| Custom table access method or fork | Deep engine control | Greater PostgreSQL integration and compatibility burden |
| Proprietary archive tier | Managed experience | Vendor-specific storage format and lifecycle |

KoldStore deliberately uses open Parquet so that historical files may later be consumed by DuckDB, Spark, DataFusion, Databricks, and other compatible engines.

---

## Current benchmark results

KoldStore is primarily a **storage lifecycle tool**, not a universal query accelerator.

After historical rows are flushed:

- The PostgreSQL hot heap becomes smaller.
- Native indexes cover fewer rows.
- Historical data moves into compressed Parquet.
- Queries that reach cold rows pay additional decoding and storage latency.

### Storage and whole-table maintenance

| Result | Before → after flush | Result |
|---|---:|---:|
| Total footprint, hot + cold | 5.85 GiB → 671 MiB | **89% smaller** |
| Hot PostgreSQL footprint, heap + `__cl` | 5.85 GiB → 72 MiB | **99% smaller** |
| Cold Parquet | — → 599 MiB | Outside PostgreSQL |
| Hot indexes, including `__cl` | 415 MiB → 11.5 MiB | **97% smaller** |
| `VACUUM (FULL, ANALYZE)` | 158.7 s → 3.24 s | **49× faster** |

The maintenance result specifically measures `VACUUM (FULL, ANALYZE)`, which rewrites the whole table. It does not represent normal autovacuum behavior; autovacuum was disabled for this benchmark.

Sample configuration:

```text
Rows:                 10,000,000
Hot row limit:        100,000
Maximum rows/file:    1,000,000
PostgreSQL:           16.13
Run date:             2026-08-01
Compression:          zstd Parquet
```

Managed PostgreSQL sizes include the hot heap and the table-specific `koldstore.<table>__cl` mirror plus their indexes.

Full results:

- [Benchmark results](docs/benchmarks/RESULTS.md)
- [Methodology and reproduction guide](docs/benchmarks/README.md)

### Latest UPDATE verification

Post-optimization PostgreSQL 16 smoke measurements put committed-WAL foreground UPDATE close to ordinary heap performance for the tested workloads:

| UPDATE workload | PostgreSQL only | KoldStore WAL | Difference |
|---|---:|---:|---:|
| Single-row pgbench throughput | 26,482 ops/s | 26,152 ops/s | **1.25% lower** |
| Single-row pgbench p95 | 0.211 ms | 0.213 ms | **0.95% higher** |
| 1k-row batch foreground throughput | 77,166 ops/s | 76,030 ops/s | **1.47% lower** |
| Async mirror catch-up | — | 49,358 ops/s | Deferred work |

These are focused single-run verification measurements, not final publication results.

Release publication requires six clean-tree counterbalanced samples, including worker-enabled backlog and drain metrics.

<details>
<summary><strong>Published 10M-row draft snapshot</strong></summary>

This storage-scale run reports foreground DML separately from asynchronous mirror catch-up.

| Operation | PostgreSQL only | KoldStore WAL | Observed trade-off |
|---|---:|---:|---|
| INSERT | 100,809 ops/s | 100,818 ops/s | Approximately identical |
| UPDATE | 81,791 ops/s | 55,164 ops/s | Single sample, **33% lower** |
| DELETE | 130,331 ops/s | 145,691 ops/s | Single sample; no speedup claim |
| Hot-only PK lookup | 3,851 ops/s | 4,076 ops/s | Approximately the same |
| Hot+cold PK lookup | 3,997 ops/s | 1,055 ops/s | **74% slower** |
| Cold-only PK lookup | 4,032 ops/s | 662 ops/s | **84% slower** |

Async mirror catch-up measured:

```text
INSERT:   32,662 operations/s
UPDATE:    1,689 operations/s
DELETE:   28,661 operations/s
Restore:  25,414 operations/s
```

The slower cold lookup results are important: Parquet is not a replacement for PostgreSQL B-tree indexes. Improving selective cold access without increasing CPU spikes or object-store reads is one of the active research areas.

</details>

---

## How it works today

1. KoldStore registers the table without adding system columns to the application schema.
2. A database worker reads committed WAL and maintains a table-specific latest-state mirror.
3. Each mirror row represents the newest known operation for one primary key.
4. A flush job selects older eligible rows using authoritative mirror sequence values.
5. Rows and mirror metadata are written to Parquet.
6. Cold visibility is published through KoldStore catalogs and manifests.
7. Eligible rows are removed from the PostgreSQL heap only after cold publication succeeds.
8. Queries against the original table use `KoldMergeScan` to resolve hot rows, cold rows, updates, and tombstones.

```mermaid
flowchart TD
  App[Application / ORM] --> Table[Original PostgreSQL table]
  Table --> Scan[KoldMergeScan]

  Scan --> Hot[Hot PostgreSQL heap]
  Scan --> Catalog[KoldStore catalog]
  Catalog --> Cold[Parquet / object storage]

  WAL[Committed WAL] --> Mirror[Latest-state mirror]
  Mirror --> Flush[Flush worker]
  Flush --> Cold
```

Managed tables use committed-WAL mirror capture.

Foreground DML writes the PostgreSQL heap normally. A database worker processes committed WAL afterward and allocates authoritative mirror `seq` values.

Use:

```sql
SELECT koldstore.wait_for_async_mirror();
```

when a strong mirror boundary is required. `flush_table` performs the required fence automatically.

The extension creates its publication and logical replication slot when needed. PostgreSQL must be configured with:

```conf
wal_level = logical
```

Architecture details:

- [Architecture overview](docs/architecture.md)
- [Mirror capture](docs/architecture/mirror-capture-modes.md)
- [Managing tables](docs/architecture/manage-table.md)
- [Flushing](docs/architecture/flushing-table.md)
- [Scanning](docs/architecture/scanning-table.md)
- [Scheduling](docs/operations/scheduling.md)

---

## Try it in five minutes

Published Docker images include PostgreSQL 16 with:

- `koldstore` shared-preloaded
- `wal_level=logical`
- The extension created during initial database setup

```bash
docker pull jamals86/pg-koldstore:latest

docker run --rm \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  jamals86/pg-koldstore:latest

psql postgres://postgres:postgres@127.0.0.1:5432/koldstoredb
```

Confirm the environment:

```sql
SHOW shared_preload_libraries;
SHOW wal_level;
SELECT koldstore.preload_status();
```

Create a storage location:

```sql
CREATE EXTENSION IF NOT EXISTS koldstore;

SELECT koldstore.register_storage(
  name         => 'local-dev',
  storage_type => 'filesystem',
  base_path    => '/tmp/koldstore-demo',
  credentials  => '{}'::jsonb,
  config       => '{}'::jsonb
);
```

Create and manage a normal PostgreSQL table:

```sql
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
```

Insert data:

```sql
INSERT INTO messages (id, body)
SELECT gs, 'row ' || gs
FROM generate_series(1, 1012) AS gs;
```

Wait for authoritative mirror sequence allocation:

```sql
SELECT koldstore.wait_for_async_mirror();
```

Read the latest-state change feed:

```sql
SELECT seq, op, pk, deleted, source
FROM koldstore.changes_since(
  table_name => 'messages'::regclass,
  since_seq  => 0,
  limit_rows => 100
);
```

Run a flush manually:

```sql
SELECT koldstore.flush_table(
  table_name => 'messages'::regclass
);
```

The same table remains queryable:

```sql
SELECT count(*) FROM messages;
```

Inspect its state:

```sql
SELECT jsonb_pretty(
  koldstore.describe_table(
    table_name => 'messages'::regclass
  )
);
```

More examples:

- [Quickstart](docs/quickstart.md)
- [SQL API](docs/sql-api.md)
- [Change API](docs/roadmap.md#change-api-changes_since)

---

## Requirements

- PostgreSQL 15–18
- `shared_preload_libraries` must include `koldstore`
- `wal_level` must be `logical`
- Managed tables require a primary key
- Local development uses `pgrx`
- Docker is used for packaging and smoke testing

Supported column types today:

- `boolean`
- `smallint`, `integer`, `bigint`
- `real`, `double precision`
- `text`, `varchar`
- `uuid`
- `jsonb`
- `timestamptz`

---

## Current limitations

These limitations are part of the research scope, not minor production caveats:

- KoldStore is not production-ready.
- Cold storage is not protected by PostgreSQL WAL.
- PostgreSQL and the cold-storage prefix must be backed up together.
- `UNIQUE` and foreign-key constraints cover hot rows only after flush.
- Native PostgreSQL indexes cover hot rows only.
- Cold primary-key lookups are slower than native B-tree lookups.
- Unavailable cold storage fails the query instead of returning partial hot-only results.
- Export/import, compaction, schema evolution, and primary-key changes remain under development.
- Cross-tier snapshot and recovery behavior is still being hardened.

See the complete [limitations document](docs/limitations.md).

---

## Research agenda

### 1. Progressive cold reads

Avoid opening Parquet merely because cold data exists.

The intended behavior is:

```text
Read the most promising hot candidate
        ↓
Compare it with cold catalog bounds
        ↓
Hot provably wins?
   ├── yes → return it without opening Parquet
   └── no  → open only competitive cold row groups
```

This includes:

- Ordered `LIMIT` queries
- Adaptive hot batches
- PostgreSQL pathkey preservation
- Segment and row-group frontiers
- Late payload materialization
- Reduced CPU spikes during selective reads

### 2. Faster cold primary-key lookup

Investigate:

```text
Catalog min/max pruning
    → Parquet Bloom filter
    → Page index
    → Selected row decode
```

The goal is not to claim Parquet can become a B-tree. The goal is to avoid scanning irrelevant files and row groups.

### 3. Scoped storage

Store each configured `scope_column` value under its own cold path:

```text
{namespace}/{table}/{scopeId}/...
```

This makes tenant and user data easier to prune, back up, export, delete, and place in different storage policies.

### 4. Compaction

Combine small immutable segments into larger optimized files to reduce object-store requests, footer reads, catalog entries, overlapping ranges, and version-resolution work.

### 5. Coordinated backup and restore

Design first-class operations that understand both PostgreSQL state and cold Parquet objects.

### 6. Schema evolution

Research safe behavior for adding columns, removing columns, type widening, defaults, older Parquet schemas, and incompatible segment rewrites.

### 7. FILE datatype

Explore a KalamDB-style logical file column:

```sql
attachment koldstore.file
```

The row would retain compact metadata and permissions while the file payload lives in object storage.

### 8. Change streaming

Stream authoritative table changes to the Kalam gateway for resumable WebSocket subscriptions and historical replay.

See the full [roadmap](docs/roadmap.md).

---

## Contributing to the research

KoldStore is early enough that workloads, measurements, and critical feedback can still change its direction.

Useful contributions include:

1. Reproduce the benchmark and publish your environment and results.
2. Test a real table and describe where the model succeeds or fails.
3. Share PostgreSQL plans that read more cold data than expected.
4. Improve planner costing, Parquet reads, storage backends, or crash recovery.
5. Add correctness tests for concurrent updates, deletes, flushes, and scans.
6. Review design documents and challenge unsafe assumptions.
7. Improve documentation and operational visibility.

The project especially welcomes reports where KoldStore performs poorly. Understanding the boundaries is part of making the design trustworthy.

### Development loop

```bash
cargo nextest run --workspace --no-default-features \
  --exclude e2e \
  --exclude examples \
  --exclude storage-comparison \
  --exclude pg-koldstore-benchmarks \
  --exclude koldstore-memory-tests \
  --exclude stress

cargo pgrx install \
  -p pg_koldstore \
  --no-default-features \
  --features "pg16 s3"

scripts/run-pg-e2e.sh 16
```

Project documentation:

- [Development guide](docs/development.md)
- [Architecture](docs/architecture.md)
- [Crate architecture](docs/architecture/crate-architecture.md)
- [Benchmark methodology](docs/benchmarks/README.md)
- [SQL API](docs/sql-api.md)
- [Roadmap](docs/roadmap.md)
- [Code of conduct](CODE_OF_CONDUCT.md)

---

## Follow the project

KoldStore is investigating a difficult boundary between PostgreSQL transactions and immutable object storage.

The project is moving toward a first production-ready release, but getting there requires honest work on planner integration, snapshots, recovery, compaction, schema evolution, and operational tooling.

⭐ **Star the repository to follow new experiments, benchmark publications, architectural decisions, and release progress.**

Opening an issue with a real PostgreSQL storage problem is equally valuable.

---

## License

Apache License 2.0.

Copyright 2026 KalamDB.

See [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
