# KoldStore

> **An open research project exploring transparent tiered storage for PostgreSQL application tables.**

Keep active rows in PostgreSQL. Move historical rows to compressed Parquet on filesystem or object storage. Continue querying supported hot and cold data through the original table.

KoldStore includes a working experimental PostgreSQL extension, reproducible benchmarks, architecture decisions, and active research into query execution, storage lifecycle, recovery, tenant-aware operation, and versioned data branches.

<p align="center">
  <img
    src="https://img.shields.io/badge/project-open%20research-6f42c1"
    alt="Project: Open Research"
  />
  <img
    src="https://img.shields.io/badge/release-developer%20preview-orange"
    alt="Release: Developer Preview"
  />
  <a href="https://github.com/kalamdb/koldstore/actions/workflows/ci-tests.yml">
    <img
      src="https://github.com/kalamdb/koldstore/actions/workflows/ci-tests.yml/badge.svg"
      alt="CI Tests"
    />
  </a>
  <img
    src="https://img.shields.io/badge/PostgreSQL-15%E2%80%9318-336791"
    alt="PostgreSQL 15–18"
  />
  <a href="https://www.rust-lang.org/">
    <img
      src="https://img.shields.io/badge/Rust-1.96%2B-orange.svg"
      alt="Rust 1.96+"
    />
  </a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0">
    <img
      src="https://img.shields.io/badge/license-Apache%202.0-blue.svg"
      alt="Apache License 2.0"
    />
  </a>
</p>

> [!WARNING]
> **KoldStore is an open research project with a working experimental implementation. It is not production-ready.**
>
> The current implementation can manage existing tables, capture committed changes, flush historical rows to Parquet, and query supported hot and cold data through the original table.
>
> Direct cold-row DML, compaction, coordinated backup and recovery, broader schema evolution, and several execution optimizations remain under active research.

## Why KoldStore?

PostgreSQL application tables often grow because history is retained for years:

- Messages and conversations
- Audit and compliance records
- Notifications and activity feeds
- Application events
- Completed workflow history
- AI prompts, outputs, traces, and tool calls
- Tenant or user history
- Selected IoT and telemetry workloads

In many systems, only a small recent working set requires native PostgreSQL latency. Historical rows still matter, but keeping them in the PostgreSQL heap also keeps their indexes, maintenance cost, replicas, and backup footprint hot.

KoldStore researches a different storage lifecycle:

```text
Original PostgreSQL table
├── Hot working set → PostgreSQL heap and native indexes
└── Historical rows → Parquet on filesystem or object storage
```

Applications continue using the original table for supported reads.

**PostgreSQL stays the interface. Parquet becomes the history.**

<p align="center">
  <img
    src="docs/assets/koldstore-demo.gif"
    alt="KoldStore moves historical PostgreSQL rows to Parquet while queries continue using the original table"
    width="900"
  />
</p>

## Research questions and current evidence

KoldStore is organized around concrete research questions rather than only a feature checklist.

| Research question | Current evidence |
|---|---|
| **Can columnar cold storage materially reduce PostgreSQL footprint?** | Strong reductions demonstrated in controlled benchmarks |
| **Can applications keep the PostgreSQL table and SQL interface?** | Working experimental `KoldMergeScan` implementation |
| **Can hot-only queries stay close to native PostgreSQL?** | Promising results for selected paths; progressive and mixed scans remain active work |
| **Can current row state be resolved across mutable hot data and immutable cold history?** | Baseline implemented with committed WAL, latest-state mirror metadata, versions, and tombstones |
| **Can tenant-aware cold storage simplify scaling and operations?** | Architecture defined; scoped object layout and routing remain incomplete |
| **Can backup and recovery remain coherent across PostgreSQL and object storage?** | Design work exists; coordinated tooling remains incomplete |
| **Can immutable cold generations support lightweight branches?** | Research concept; branch metadata and isolation are not implemented |

## What works today?

| Capability | Status |
|---|---|
| Manage existing PostgreSQL tables | ✅ Working |
| Keep application-visible schemas clean | ✅ Working |
| Query supported hot and cold rows through the original table | 🧪 Experimental |
| Native PostgreSQL foreground writes | ✅ Working |
| Committed-WAL latest-state mirror | ✅ Working, asynchronous |
| Manual and automatic flushing | ✅ Working |
| Row-limit and age-based lifecycle policies | ✅ Working |
| Parquet writing with zstd compression | ✅ Working |
| Local filesystem storage | ✅ Working |
| S3/MinIO, GCS, and Azure Blob | 🧪 Integration hardening |
| Segment and row-group pruning | 🧪 Working baseline |
| Latest-state `changes_since` cursor | 🧪 Working baseline |
| Durable migration and flush jobs | ✅ Working baseline |
| PostgreSQL 15–18 | ✅ Supported |
| Standard DML against cold-only rows | ❌ Not supported yet |
| Global hot+cold `UNIQUE` and foreign keys | ❌ Not supported |
| Compaction | 🚧 In development |
| Coordinated backup, restore, and PITR | 🚧 In development |
| Broad schema evolution | 🚧 In development |
| Tenant-scoped object paths | 🚧 Planned |
| Versioned data branches | 🔬 Research |

## Try it

The preview Docker image includes PostgreSQL 16 with KoldStore preloaded and logical WAL enabled.

```bash
docker pull jamals86/pg-koldstore:latest

docker run --rm \
  --name koldstore \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  jamals86/pg-koldstore:latest
```

Connect:

```bash
psql postgres://postgres:postgres@127.0.0.1:5432/koldstoredb
```

Create the extension and register local cold storage:

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

Create a normal PostgreSQL table:

```sql
CREATE TABLE messages (
  id         bigint PRIMARY KEY,
  body       text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
```

Enable tiered storage:

```sql
ALTER TABLE messages SET (
  koldstore_enabled           = true,
  koldstore_storage           = 'local-dev',
  koldstore_hot_row_limit     = 1000,
  koldstore_min_flush_rows    = 1000,
  koldstore_max_rows_per_file = 10000
);
```

Applications still query the original table:

```sql
SELECT *
FROM messages
WHERE id = 42;
```

Flush eligible history:

```sql
SELECT koldstore.flush_table(
  table_name => 'messages'
);
```

Inspect the managed table:

```sql
SELECT jsonb_pretty(
  koldstore.describe_table(table_name => 'messages')
);
```

See the [five-minute quickstart](docs/quickstart.md) for a complete reproducible walkthrough.

## How it works

```mermaid
flowchart LR
  App[Application / ORM] --> Table[Original PostgreSQL table]

  Table --> Planner[PostgreSQL planner]
  Planner --> Hot[Native PostgreSQL hot access]
  Planner --> Merge[KoldMergeScan]

  Merge --> Hot
  Merge --> Catalog[KoldStore catalog]
  Catalog --> Cold[Parquet / object storage]

  WAL[Committed PK-only WAL] --> Worker[Mirror worker]
  Worker --> Mirror[Latest-state mirror]
  Mirror --> Flush[Flush jobs]
  Flush --> Cold
```

1. The source table remains a normal PostgreSQL heap.
2. PostgreSQL continues handling foreground transactions and hot indexes.
3. A database worker consumes committed primary-key changes from logical WAL.
4. Flush jobs write eligible historical rows to immutable Parquet segments.
5. Cold visibility is published before matching hot rows are removed.
6. `KoldMergeScan` combines current hot state, cold history, and mirror metadata for supported reads.

KoldStore does not replace PostgreSQL with another database engine, and Parquet remains an open storage format.

Architecture documentation:

- [Architecture overview](docs/architecture.md)
- [Managing tables](docs/architecture/manage-table.md)
- [Mirror capture](docs/architecture/mirror-capture.md)
- [Flushing](docs/architecture/flushing-table.md)
- [Scanning managed tables](docs/architecture/scanning-table.md)
- [Crate architecture](docs/architecture/crate-architecture.md)

## Read semantics

For a managed table, KoldStore tries to avoid cold work when PostgreSQL or catalog bounds prove that cold rows cannot affect the result.

```text
Native PostgreSQL candidate
          ↓
Can cold data affect the answer?
          ├── No  → return without opening Parquet
          └── Yes → inspect only candidate cold segments
```

The intended long-term execution model is progressive:

```text
PostgreSQL requests the next row
          ↓
Compare the hot candidate with cold metadata bounds
          ↓
Open only cold segments that can still affect the result
          ↓
Stop reading when PostgreSQL stops requesting rows
```

Progressive ordered reads, bounded Arrow streaming, late payload materialization, and lower cold-read CPU usage remain active research areas.

## Write and consistency semantics

Foreground `INSERT`, `UPDATE`, and `DELETE` operate on the PostgreSQL heap.

Committed primary-key changes are then applied asynchronously to the latest-state mirror through logical WAL.

Use the explicit consistency fence before work that must observe all committed source changes in the mirror:

```sql
SELECT koldstore.wait_for_async_mirror();
```

Important boundaries:

- Standard DML does not currently mutate a row that exists only in cold storage.
- An unfenced update or delete can temporarily leave an older cold version visible.
- The mirror stores the latest state per primary key; it is not an append-only event history.
- Primary-key mutation is not supported for managed tables.
- PostgreSQL-native indexes remain attached only to hot rows.

These are preview limitations, not hidden compatibility guarantees.

## Benchmark snapshot

The current storage comparison uses:

- PostgreSQL 16
- 10 million rows
- A 100,000-row hot limit
- Approximately 9.9 million flushed rows
- zstd-compressed Parquet

| Metric | PostgreSQL only | PostgreSQL + KoldStore |
|---|---:|---:|
| Total hot + cold footprint | 5.85 GiB | 670.75 MiB |
| PostgreSQL-resident footprint | 5.85 GiB | 72.23 MiB |
| Index footprint | 414.86 MiB | 11.45 MiB |
| `VACUUM (FULL, ANALYZE)` | 158.7 s | 3.24 s |
| Foreground insert throughput | 100,809 rows/s | 100,818 rows/s |
| Foreground update throughput | 81,791 rows/s | 55,164 rows/s |
| Hot point-query p99 | 355 µs | 438 µs |
| Cold point-query p99 | 306 µs | 1.71 ms |

The storage and maintenance reductions are the intended benefit.

KoldStore is not currently a universal query accelerator:

- Cold point lookups are slower than native PostgreSQL B-tree lookups.
- Mixed hot/cold reads remain slower than native heap/index access.
- Update overhead remains material.
- Flush memory and CPU behavior are active optimization areas.

These are controlled benchmark results, not guarantees. Hardware, schema shape, row width, compression, object storage, file sizing, and access patterns can materially change the outcome.

- [Latest benchmark results](docs/benchmarks/RESULTS.md)
- [Benchmark methodology and reproduction](docs/benchmarks/README.md)

## Good fit

KoldStore is being designed for append-heavy or history-heavy application tables where old rows become less active but must remain available.

Examples:

- Messages and conversations
- Audit and compliance logs
- AI prompts, outputs, traces, and tool calls
- Notifications and activity feeds
- Application events
- Completed workflow history
- User or tenant history
- Selected IoT and telemetry workloads

A strong candidate usually has:

```text
large historical share
+ small active working set
+ long retention
+ infrequent cold access
+ operational control over PostgreSQL
```

## Not a good fit yet

KoldStore should not currently be used for:

- Payment ledgers, balances, or inventory state
- Tables whose historical rows remain frequently mutable
- FK-heavy models requiring hot+cold referential integrity
- Schemas requiring global hot+cold non-primary-key uniqueness
- Workloads requiring native B-tree latency for archived rows
- Global vector or full-text search over cold history
- Environments that cannot install and preload a native PostgreSQL extension
- Systems that cannot tolerate cold-storage availability affecting a query

See the full [limitations](docs/limitations.md).

## How KoldStore differs

KoldStore is not intended to be:

- A replacement for PostgreSQL
- A data warehouse
- A general analytics engine
- A generic Parquet import/export function
- A time-series-only database
- A proprietary archive format

Its focus is the lifecycle of ordinary PostgreSQL application-history tables:

```text
keep PostgreSQL as the application interface
+ keep the active working set hot
+ move historical rows to open Parquet
+ preserve supported original-table reads
```

## Requirements

- PostgreSQL 15, 16, 17, or 18
- `shared_preload_libraries = 'koldstore'`
- `wal_level = logical`
- A primary key on every managed table
- Supported PostgreSQL column types
- Filesystem or configured object storage

`shared_preload_libraries` is required for correct managed-table reads. Without the planner hook, PostgreSQL could otherwise execute an incomplete hot-only plan after rows have been flushed.

Read the following before managing a table:

- [Quickstart](docs/quickstart.md)
- [SQL API](docs/sql-api.md)
- [Limitations](docs/limitations.md)
- [Backup and operations](docs/backup-and-operations.md)

## Research roadmap

Near-term implementation research:

1. Preserve native PostgreSQL performance for hot-dominant queries.
2. Finish progressive, bounded Arrow and Parquet reads.
3. Improve cold point lookups with statistics, Bloom filters, and page indexes.
4. Add compaction for obsolete versions, tombstones, and small files.
5. Complete direct cold-row DML semantics.
6. Deliver coordinated backup, restore, export, and recovery tooling.
7. Expand schema-evolution support.
8. Add tenant-scoped object paths and lifecycle operations.

Longer-term research:

- Lightweight branches over shared immutable cold history
- Per-tenant export, restore, deletion, and migration
- Easier rebalancing of active tenant working sets
- Cold vector side indexes
- Alternative cold formats
- Cross-engine access to published cold data

See the full [roadmap](docs/roadmap.md).

## Research principles

KoldStore is guided by several principles:

1. **PostgreSQL remains the application interface.**
2. **The active working set belongs in PostgreSQL.**
3. **Historical storage should use open formats.**
4. **Correctness is more important than transparent-looking magic.**
5. **Cold data should not be opened merely because it exists.**
6. **Benchmarks must publish tradeoffs, not only wins.**
7. **Negative results are valuable research output.**

## Help shape the project

KoldStore is early enough that real workloads and negative results can still change the architecture.

The most valuable contribution may be a workload report rather than code.

Useful information includes:

- PostgreSQL version
- Table schema
- Total table and index size
- Monthly growth
- Estimated hot-working-set size
- Percentage of queries reading historical rows
- Update and delete behavior
- Storage backend
- A sanitized `EXPLAIN (ANALYZE, FORMAT JSON)` plan

Contribution areas include:

- PostgreSQL planner and executor integration
- Logical WAL decoding
- Rust
- Arrow and Parquet
- Object storage
- Crash recovery
- Benchmarks
- Testing
- Documentation
- Operational tooling

Useful contribution paths:

- Reproduce the published benchmarks
- Test a real application-history table
- Share plans that open too much cold data
- Improve planner integration and progressive reads
- Add concurrency, recovery, and integrity tests
- Challenge assumptions in the architecture documents
- Improve installation and operator documentation

Links:

- [Open issues](https://github.com/kalamdb/koldstore/issues)
- [Contribution guide](CONTRIBUTING.md)
- [Development guide](docs/development.md)
- [Crate architecture](docs/architecture/crate-architecture.md)
- [Code of conduct](CODE_OF_CONDUCT.md)

⭐ **Star the repository to follow experiments, benchmarks, architectural decisions, and progress toward a production-ready implementation.**

## Suggested GitHub description

> Open research project exploring tiered storage for PostgreSQL application tables—keep active rows in Postgres, move history to Parquet, and query both through the original table.

## License

Apache License 2.0.

Copyright 2026 KalamDB.
