# Architecture

pg-koldstore is a PostgreSQL extension for normal heap tables. PostgreSQL
remains the transaction, locking, and hot-row authority. KoldStore adds a
change-log mirror, cold Parquet segments, and a `KoldMergeScan` custom scan so
SQL, MVCC, permissions, and RLS stay PostgreSQL-owned.

## Workflow documentation

These documents describe **what the code does today**, including serde
boundaries at each step:

| Workflow | Document |
|----------|----------|
| Register a table for hot/cold management | [manage-table](architecture/manage-table.md) |
| Move mirror rows to Parquet and prune hot | [flushing-table](architecture/flushing-table.md) |
| `SELECT` through hot + cold merge | [scanning-table](architecture/scanning-table.md) |
| `INSERT` / `UPDATE` / `DELETE` capture | [dml-table](architecture/dml-table.md) |

## Contributor layout

See [crate architecture](architecture/crate-architecture.md) for the layered
Rust crate layout and dependency graph.

## Decisions

| ADR | Topic |
|-----|--------|
| [ADR-001](decisions/001-layered-crate-architecture.md) | Layered crate architecture |
| [ADR-002](decisions/002-footer-derived-catalog-stats.md) | Footer-derived catalog segment stats (accepted, deferred) |

## Supplementary notes

- [Change-log mirror and transactions](architecture/change-log-mirror-and-transactions.md) — mirror contract and transaction boundaries (clean-schema default)
- [Existing table migration](architecture/existing-table-migration-and-flush.md) — ordering rules for populated-table backfill (pointer to manage/flush workflows)

## Core design choices

### Clean-schema mirror (no heap system columns)

Managed user tables keep application columns only. Sequence and delete state
live in `koldstore.{table}__cl` and in cold Parquet metadata (`seq`, `deleted`).
See [dml-table](architecture/dml-table.md).

### Custom scan instead of an external query engine

KoldMergeScan materializes a hot+cold winner set via SQL at scan start, then
serves rows from a buffer. See [scanning-table](architecture/scanning-table.md).

### Manifest and catalog

`koldstore.manifest` tracks sync state and O(1) row counters. Object-store
`manifest.json` is written on flush finalize. Cold segment metadata lives in
`koldstore.segments`. See [flushing-table](architecture/flushing-table.md).

### Operational boundaries

Object storage is not part of PostgreSQL WAL. Operators must back up cold
artifacts together with PostgreSQL base backups and validate manifest identity
before PITR cutover.
