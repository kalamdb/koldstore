# ADR-001: Layered Crate Architecture

## Status

Accepted

## Date

2026-07-06

## Context

Most domain logic (~10k LOC) lived in `pg_koldstore` while seven library crates
totaled ~4k LOC. Contributors had to search a single large extension crate to
find flush, migrate, DML, and catalog behavior. Testing PG-free logic required
building the full extension.

## Decision

Split pg-kalam into layered crates:

- **Foundation:** `koldstore-common` (shared types, no internal deps).
- **Primitives:** `koldstore-catalog`, `koldstore-schema`, `koldstore-storage`,
  `koldstore-parquet`.
- **Building blocks:** `koldstore-manifest`, `koldstore-mirror`, `koldstore-merge`,
  `koldstore-jobs`, `koldstore-setup`.
- **Workflows:** `koldstore-flush`, `koldstore-migrate`.
- **Integration:** `pg_koldstore` only — `pgrx`, SPI, hooks, merge scan FFI,
  thin SQL wrappers.

Naming choices:

- `common` over `core` for the foundation crate.
- `schema` for migrated-table schema registry (`koldstore.schemas` rows).
- `setup` for extension install DDL (internal tables/indexes).
- `catalog` remains explicit for cold-data bookkeeping.
- Merge scan stays in the extension (PostgreSQL custom scan + C FFI).

## Alternatives Considered

### Single fat extension crate

- Pros: fewer crates, simpler workspace.
- Rejected: hard to navigate, poor testability, discourages contributors.

### `merge-scan` as separate crate with `pgrx` feature

- Pros: isolates custom scan code.
- Rejected for now: tight coupling to planner/executor; extension adapter is
  sufficient until reuse is needed.

### Fold catalog into common

- Pros: fewer crates.
- Rejected: catalog is domain-specific metadata, not generic shared types.

## Consequences

- PG-free logic is unit-testable per crate.
- Contributors locate code by crate responsibility.
- `pg_koldstore` shrinks to a thin PostgreSQL boundary.
- Workspace churn during migration; dependency direction must be enforced.
- Documentation and cleanup standards apply on every extraction (see
  `docs/architecture/crate-architecture.md`).
