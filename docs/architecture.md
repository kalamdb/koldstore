# Architecture

pg-koldstore adds tiered storage to normal PostgreSQL heap tables. Tiered
storage places data on different storage media according to performance,
access, and cost needs. In KoldStore, the hot tier is the PostgreSQL heap and
its native indexes; the cold tier is compressed Parquet on a configured
filesystem or object store.

PostgreSQL remains the transaction, locking, and hot-row authority. KoldStore
adds a change-log mirror, cold Parquet segments, and a `KoldMergeScan` custom
scan so SQL, MVCC, permissions, and RLS stay PostgreSQL-owned. Applications
query the original table across both tiers. Tier placement is policy-driven:
today, a hot-row limit selects older mirror sequence values for flush rather
than measuring row access frequency automatically.

## Workflow documentation

These documents describe **what the code does today**, including serde
boundaries at each step:

| Workflow | Document |
|----------|----------|
| Register a table for hot/cold management | [manage-table](architecture/manage-table.md) |
| Mirror capture (WAL apply) | [mirror-capture](architecture/mirror-capture.md) |
| Move mirror rows to Parquet and prune hot | [flushing-table](architecture/flushing-table.md) |
| `SELECT` through hot + cold merge | [scanning-table](architecture/scanning-table.md) |
| `INSERT` / `UPDATE` / `DELETE` capture | [dml-table](architecture/dml-table.md) |
| Jobs, worker, and automatic flush | [jobs-and-scheduler](architecture/jobs-and-scheduler.md) |

## Contributor layout

See [crate architecture](architecture/crate-architecture.md) for the layered
Rust crate layout and dependency graph.

## Decisions

| ADR | Topic |
|-----|--------|
| [ADR-001](decisions/001-layered-crate-architecture.md) | Layered crate architecture |
| [ADR-002](decisions/002-footer-derived-catalog-stats.md) | Footer-derived packed segment and row-group stats (implemented) |
| [ADR-003](decisions/003-optional-async-mirror-capture.md) | Historical capture ADR (superseded by current [mirror capture](architecture/mirror-capture.md)) |
| [ADR-004](decisions/004-segment-publication-protocol.md) | Pending-to-active segment publication protocol |
| [ADR-005](decisions/005-async-apply-progress-and-health.md) | Async UPDATE apply, worker progress, and retained-WAL health |

## Cases

Design notes for correctness edge cases (proposed or landed):

| Case | Topic |
|------|--------|
| [async-flush-prune-race](cases/async-flush-prune-race.md) | Concurrent async DML vs post-flush hot/mirror prune |

## Core design choices

### Clean-schema mirror (no heap system columns)

Managed user tables keep application columns only. Sequence and delete state
live in `koldstore.{table}__cl` and in cold Parquet metadata (`seq`, `deleted`).
Committed primary-key-only WAL is applied to the mirror by a database worker,
with an explicit consistency fence for strong reads. UPDATE uses a direct set-based update
for existing mirror keys and a conflict-safe insert-missing fallback for keys
already pruned by flush. The worker drains bounded batches in short retry
bursts and always yields between bursts.
See [dml-table](architecture/dml-table.md) and
[mirror capture](architecture/mirror-capture.md).

### Custom scan instead of an external query engine

KoldMergeScan streams hot pages and cold segment groups through an exact
winner resolver, retaining PK identities (not full row images) for the scan.
See [scanning-table](architecture/scanning-table.md).

### Manifest and catalog

`koldstore.manifest` tracks sync state and O(1) row counters. Object-store
export is folder-sharded (`manifest.json` root + content-addressed folder shards)
and is written on flush finalize. Cold segment metadata lives in
`koldstore.cold_segments`. See [flushing-table](architecture/flushing-table.md).

### Operational boundaries

Object storage is not part of PostgreSQL WAL. Operators must back up cold
artifacts together with PostgreSQL base backups and validate manifest identity
before PITR cutover. For async capture, retained WAL is health telemetry that
must alert operators without disabling the applier; PostgreSQL disk and logical
slot-retention controls remain independent hard safeguards.
