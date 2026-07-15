# KoldStore

> **Keep hot data in PostgreSQL. Move historical rows to Parquet. Query one table.**

KoldStore is an open-source PostgreSQL extension for application tables that grow forever: messages, audit logs, AI history, notifications, events, and IoT data.

Your table remains a normal PostgreSQL heap table. KoldStore keeps the active working set in PostgreSQL, flushes older rows into compressed Parquet on storage you control, and transparently reads hot and cold rows through the original table.

**No replacement database. No proprietary archive format. No application query rewrite.**

> [!WARNING]
> **KoldStore is in early development and is not production-ready.** The core manage, flush, manifest, and hot/cold query flow works. Recovery, backup/restore, compaction, schema evolution, and background scheduling are still being hardened.

⭐ **Star the repository to follow the project as it moves toward the first production-ready release.**

<p align="center">
  <a href="https://github.com/kalamdb/koldstore/releases"><img src="https://img.shields.io/github/v/release/kalamdb/koldstore?display_name=tag&amp;label=release" alt="Release" /></a>
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
  <img src="docs/assets/benchmark-storage-wins.svg" alt="After flush: heap and indexes 99% smaller, indexes 97% smaller, VACUUM FULL 95% faster" width="900" />
</p>

| Result | Before → after flush | Win |
| --- | --- | --- |
| PostgreSQL heap + indexes | 5.85 GiB → 73 MiB | **99% smaller** |
| Indexes | 415 MiB → 11.5 MiB | **97% smaller** |
| `VACUUM (FULL, ANALYZE)` | 71.8 s → 5.16 s | **93% faster** |

Sample: 10M wide rows, `hot_row_limit = 100000` (local PG16.13 `release-pg`).

Managed tables still maintain a latest-state change-log mirror on every
`INSERT` / `UPDATE` / `DELETE` (same transaction as the heap write). Statement-level
capture cut that cost sharply on bulk DML — median managed **UPDATE ~48×** and
**DELETE ~3×** faster vs the prior capture SQL on a local 5k-row sample; bulk
INSERT ~1.9×. Cold PK lookups remain slower than pure B-tree probes today (~119
ops/s managed hot+cold vs ~1.5k heap on this 10M run). Full methodology and
tables: [docs/benchmarks/](docs/benchmarks/README.md).

## How it works

1. `manage_table` registers the table and creates a small latest-state change-log mirror (one metadata row per primary key; commits/rolls back with the original transaction).
2. `flush_table` moves older rows to Parquet and prunes them from the hot heap when safe.
3. `SELECT` on the original table uses `KoldMergeScan` so the newest visible row wins.

Details: [Architecture](docs/architecture.md) · [Manage](docs/architecture/manage-table.md) · [Flush](docs/architecture/flushing-table.md) · [Scan](docs/architecture/scanning-table.md)

```mermaid
flowchart TD
  App[Application / ORM] --> T[Original PostgreSQL table]
  T --> Scan[KoldMergeScan]
  Scan --> Hot[Hot PG heap]
  Scan --> Cold[Manifest → Parquet / S3]
```

## Try it in five minutes

Published release images ship PostgreSQL 16 with `koldstore` and `pg_cron` already installed:

```bash
docker pull jamals86/pg-koldstore:latest
docker run --rm -e POSTGRES_PASSWORD=postgres -p 5432:5432 jamals86/pg-koldstore:latest
psql postgres://postgres:postgres@127.0.0.1:5432/koldstore
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

INSERT INTO messages (id, body)
SELECT gs, 'row ' || gs FROM generate_series(1, 1012) AS gs;

SELECT koldstore.manage_table(
  table_name        => 'messages',
  storage           => 'local-dev',
  hot_row_limit     => 1000,
  min_flush_rows    => 1,
  max_rows_per_file => 500,
  migration_order_by => 'id'
);

SELECT koldstore.flush_table(table_name => 'messages');

SELECT count(*) FROM messages;  -- still 1012 via KoldMergeScan
SELECT jsonb_pretty(koldstore.describe_table(table_name => 'messages'));
```

Mirror inspection, job UUIDs, `EXPLAIN`, shared/user tables, and storage backends: [docs/quickstart.md](docs/quickstart.md).

Schedule periodic flush with pg_cron: [docs/operations/scheduling.md](docs/operations/scheduling.md).

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

- **Operations** — smart flush scheduler, coordinated backup/restore, import/export
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
cargo test --workspace
cargo pgrx install -p pg_koldstore --no-default-features --features pg16
scripts/run-pg-e2e.sh 16
```

- [Development guide](docs/development.md)
- [Crate architecture](docs/architecture/crate-architecture.md)
- [SQL API](docs/sql-api.md)
- [Code of conduct](CODE_OF_CONDUCT.md)

## License

Apache License 2.0. Copyright 2026 KalamDB.

See [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).
