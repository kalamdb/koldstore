# KoldStore

> **An open research project building transparent tiered storage for PostgreSQL application tables.**

Keep active rows in PostgreSQL. Move historical rows to compressed Parquet on object storage. Continue querying the original table.

⭐ **Star the repository to follow the experiments, benchmarks, and progress toward a production-ready release.**

<p align="center">
  <a href="https://github.com/kalamdb/koldstore/releases"><img src="https://img.shields.io/github/v/release/kalamdb/koldstore?display_name=tag&label=release" alt="Release" /></a>
  <a href="https://hub.docker.com/r/jamals86/pg-koldstore"><img src="https://img.shields.io/docker/pulls/jamals86/pg-koldstore" alt="Docker Pulls" /></a>
  <a href="https://github.com/kalamdb/koldstore/actions/workflows/ci-tests.yml"><img src="https://github.com/kalamdb/koldstore/actions/workflows/ci-tests.yml/badge.svg" alt="CI Tests" /></a>
  <img src="https://img.shields.io/badge/PostgreSQL-15%E2%80%9318-336791" alt="PostgreSQL 15-18" />
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/rust-1.96%2B-orange.svg" alt="Rust 1.96+" /></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License" /></a>
</p>

> [!WARNING]
> **KoldStore is an experimental research project and is not production-ready.**
>
> The current prototype can manage existing tables, capture committed changes, flush historical rows, and query hot and cold data through the original table. Recovery, compaction, coordinated backup and restore, schema evolution, and several query optimizations are still under development.

## What is KoldStore?

KoldStore is a PostgreSQL extension researching whether ordinary application tables can use two storage tiers:

| Tier | Storage | Intended use |
|---|---|---|
| **Hot** | PostgreSQL heap and native indexes | Active rows, transactional writes, low-latency reads |
| **Cold** | Parquet on filesystem or object storage | Historical rows, compression, and long-term retention |

Applications keep using:

- The same PostgreSQL table
- The same SQL
- Existing drivers and ORMs
- PostgreSQL for normal transactional writes

`KoldMergeScan` combines visible rows from the PostgreSQL heap with matching cold Parquet segments.

**No replacement database. No proprietary archive format. No application query rewrite.**

<p align="center">
  <img
    src="docs/assets/koldstore-demo.gif"
    alt="KoldStore moves historical PostgreSQL rows to Parquet while queries continue using the original table"
    width="900"
  />
</p>

## Research goals

KoldStore is testing six main hypotheses:

| Research goal | Current state |
|---|---|
| **Reduce storage with columnar compression** | Demonstrated in controlled benchmarks |
| **Keep PostgreSQL as the application interface** | Working experimental implementation |
| **Minimize overhead for hot-row workloads** | Promising results; still active research |
| **Preserve DML semantics across tiers** | Working baseline using WAL, mirror state, and tombstones |
| **Support multi-tenant storage by design** | Architecture defined; implementation continuing |
| **Improve backup and restore efficiency** | Research target; coordinated tooling not yet complete |
| **Explore versioned data branches over tiered storage** | Research concept; branch metadata and isolation model not yet implemented |

### Smaller storage

Historical rows are written to zstd-compressed Parquet while only the configured working set remains inside PostgreSQL.

Current 10-million-row benchmark:

| Result | Before → after flush |
|---|---:|
| Total hot + cold footprint | 5.85 GiB → 671 MiB |
| PostgreSQL-resident footprint | 5.85 GiB → 72 MiB |
| Hot indexes | 415 MiB → 11.5 MiB |
| `VACUUM (FULL, ANALYZE)` | 158.7 s → 3.24 s |

These are controlled benchmark results, not a guarantee for every schema or workload.

- [Full benchmark results](docs/benchmarks/RESULTS.md)
- [Benchmark methodology](docs/benchmarks/README.md)

### PostgreSQL compatibility

Existing tables can be managed without adding KoldStore columns to the application-visible schema.

Applications continue querying the original table:

```sql
SELECT *
FROM messages
WHERE id = 42;
```

KoldStore decides whether the answer requires:

```text
PostgreSQL only
Cold Parquet only
Both tiers
```

### Hot-path performance

A central research goal is that moving historical rows to another tier should not materially slow queries that only need hot rows.

The intended execution model is:

```text
Try the PostgreSQL hot path
          ↓
Check whether cold data can affect the answer
          ↓
No  → return without opening Parquet
Yes → open only relevant cold segments
```

Selected foreground INSERT and UPDATE tests are currently close to ordinary PostgreSQL performance. Mixed and cold-only point lookups remain slower than native B-tree lookups.

Progressive ordered reads, adaptive batching, late materialization, and lower CPU usage are active research areas.

### DML across storage tiers

KoldStore does not modify Parquet files in place during foreground transactions.

Applications continue issuing normal PostgreSQL DML:

```sql
INSERT INTO messages ...;
UPDATE messages ...;
DELETE FROM messages ...;
```

Committed WAL maintains a latest-state mirror. Newer hot rows and delete tombstones override older cold versions during reads.

```text
PostgreSQL DML
      ↓
Committed-WAL mirror
      ↓
Hot/cold version resolution
      ↓
Future background compaction
```

The current implementation supports the baseline semantics. Long-term compaction and cleanup of obsolete cold versions are still being developed.

### Multi-tenant tiered storage

KoldStore is being designed to organize cold data by tenant or user scope:

```text
{namespace}/{table}/{scopeId}/...
```

The goal is to support efficient:

- Tenant pruning
- Independent retention policies
- Tenant export and deletion
- Regional storage placement
- Scope-level backup and restore

The architecture supports scoped storage concepts, but complete tenant lifecycle tooling remains in progress.

### Versioned branches over tiered storage

**Research objective:** Investigate whether immutable cold-storage segments can support lightweight, isolated branches of PostgreSQL table history without copying the complete database.

The proposed model treats published Parquet segments as immutable shared history. A branch would reference an existing parent generation and store only the changes created after the branch point.

```text
Main branch
├── Shared immutable Parquet history
├── Current PostgreSQL hot rows
│
├── Experiment branch
│   └── Branch-specific changes and new cold segments
│
└── Testing branch
    └── Branch-specific changes and new cold segments
```

### Backup and recovery

Reducing the PostgreSQL heap and indexes may make database backups, replicas, restores, and maintenance operations smaller and faster.

However, cold objects are outside PostgreSQL WAL.

Today, PostgreSQL and the cold-storage prefix must be backed up together. Coordinated snapshots, manifest-generation boundaries, point-in-time recovery across both tiers, and first-class restore tooling remain research objectives.

## What works today?

The current experimental implementation supports:

- Existing-table management
- Clean application schemas
- Committed-WAL mirror capture
- Manual and automatic flushing
- Parquet writing
- Filesystem storage
- S3/MinIO, GCS, and Azure Blob integrations under hardening
- Transparent hot/cold `SELECT`
- Catalog and row-group pruning
- Latest-state updates and delete tombstones
- `changes_since` cursors
- Durable migration and flush jobs
- PostgreSQL 15–18

## How it works

```mermaid
flowchart LR
  App[Application / ORM] --> Table[Original PostgreSQL table]
  Table --> Scan[KoldMergeScan]

  Scan --> Hot[Hot PostgreSQL heap]
  Scan --> Catalog[KoldStore catalog]
  Catalog --> Cold[Parquet / object storage]

  WAL[Committed WAL] --> Mirror[Latest-state mirror]
  Mirror --> Flush[Flush worker]
  Flush --> Cold
```

1. KoldStore registers an existing PostgreSQL table.
2. A worker reads committed WAL and maintains latest-state metadata.
3. Flush jobs write eligible historical rows to Parquet.
4. Cold visibility is published before rows are removed from the hot heap.
5. `KoldMergeScan` resolves hot rows, cold rows, newer versions, and tombstones.

Architecture details:

- [Architecture](docs/architecture.md)
- [Mirror capture](docs/architecture/mirror-capture-modes.md)
- [Managing tables](docs/architecture/manage-table.md)
- [Flushing](docs/architecture/flushing-table.md)
- [Scanning](docs/architecture/scanning-table.md)

## Good fit

KoldStore currently targets append-heavy application tables where older rows become less active but must remain accessible:

- Messages and conversations
- Audit logs
- AI prompts, outputs, and tool calls
- Notifications and activity feeds
- Application events
- User or tenant history
- IoT telemetry

## Not a good fit yet

- Payment ledgers and account balances
- Frequently mutated historical state
- FK-heavy models requiring global hot+cold constraint enforcement
- Workloads requiring cold rows to match native B-tree latency
- Systems that cannot tolerate object-storage availability affecting queries

## Try it

The Docker image includes PostgreSQL 16 with KoldStore preloaded and logical WAL enabled:

```bash
docker pull jamals86/pg-koldstore:latest

docker run --rm \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  jamals86/pg-koldstore:latest

psql postgres://postgres:postgres@127.0.0.1:5432/koldstoredb
```

Create a table and enable tiered storage:

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
  koldstore_hot_row_limit = 1000
);
```

See the [five-minute quickstart](docs/quickstart.md) for the complete example.

## Requirements

- PostgreSQL 15–18
- `shared_preload_libraries = 'koldstore'`
- `wal_level = logical`
- A primary key on every managed table

See the [SQL API](docs/sql-api.md) for supported types and configuration.

## Current limitations

- Not production-ready
- Cold objects are not protected by PostgreSQL WAL
- PostgreSQL and cold storage require coordinated backups
- PostgreSQL indexes cover hot rows only
- `UNIQUE` and foreign keys are enforced on hot rows only after flush
- Cold point lookups are slower than native PostgreSQL indexes
- Object-storage failure causes the query to fail
- Compaction, schema evolution, export/import, and PK changes remain incomplete

See the full [limitations](docs/limitations.md).

## Research roadmap

Current research priorities:

1. Preserve native PostgreSQL performance for hot-only queries
2. Progressive ordered reads without unnecessary Parquet access
3. Faster cold lookups using statistics, Bloom filters, and page indexes
4. Multi-tenant scoped storage and lifecycle operations
5. Compaction of obsolete cold versions and tombstones
6. Coordinated backup, restore, and point-in-time recovery
7. Schema evolution across Parquet generations

See the full [roadmap](docs/roadmap.md).

## Contributing

KoldStore is early enough that real workloads and negative results can still change the architecture.

Useful contributions include:

- Reproducing the benchmarks
- Sharing PostgreSQL plans that access too much cold data
- Testing real application-history tables
- Improving planner integration and Parquet reads
- Adding concurrency and recovery tests
- Challenging assumptions in the design documents

- [Development guide](docs/development.md)
- [Crate architecture](docs/architecture/crate-architecture.md)
- [Code of conduct](CODE_OF_CONDUCT.md)

⭐ **Star the repository to follow new experiments, benchmark results, architectural decisions, and release progress.**

## License

Apache License 2.0.

Copyright 2026 KalamDB.
