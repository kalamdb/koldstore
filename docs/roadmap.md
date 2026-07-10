# Roadmap

KoldStore 0.1 focuses on reliable hot/cold table management, sequence-ordered
flushes, and correct `KoldMergeScan` reads. The following features are deferred
until after that baseline is stable.

## Near-term product surface

- **Smart flush scheduler** — built-in scheduling that triggers flushes
  automatically without relying on `pg_cron` (operators can keep using
  `pg_cron` + `koldstore.flush_table` until this lands).
- **Improve `KoldMergeScan`** — prioritize cold PK point-lookup latency
  (backend footer/reader cache, cold-native emit without JSON merge), then
  remaining streaming polish, bounded-memory execution, rescans, and broader
  planner pushdown. See [performance](performance.md).
- **Finish change-log APIs** — public `changes_since` / change-cursor SQL on
  top of the latest-state `__cl` mirror (see README “In Development”).
- **Storage file datatype** — upload and fetch files directly from registered
  cold storage backends.
- **Import / export** — table-level archive import and export of managed data.
- **Backup / restore** — coordinated PostgreSQL + cold-storage backup and
  restore workflows.

## Storage layout and pruning

- **Footer-derived catalog segment stats** — stop hand-maintaining
  `indexed_bounds` during encode; after Parquet write, extract min/max from
  footer chunk statistics into `column_stats` (catalog still owns
  prune-before-open). Accepted in
  [ADR-002](decisions/002-footer-derived-catalog-stats.md); schedule after
  cold PK scan wins. `byte_size` already comes from publish metadata only.
- Operator-configurable `pruning_columns` and `bloom_filter_columns`.
- Segment compaction and small-file combining.
- Size-aware segment writing based on `target_file_size_mb`.
- Configurable `flush_order_by`; flush selection is always ordered by mirror
  `seq` today.

## Table management and flush policy

- `koldstore.alter_table` for changing managed-table settings after
  registration.
- Time- or age-based flush triggers such as `max_flush_interval`.
- Background scheduling, richer retry controls, and operational policy tuning.

## Query execution

- User-scoped cold-segment loading and parallel custom-scan execution.
- Additional predicate, projection, and ordering pushdown.

## Other post-0.1 work

- Segment lifecycle tooling, validation, and repair automation.
- Explicit cold-row DML APIs (`hydrate_pk`, `update_row`, `delete_row`).
- Broader schema evolution and primary-key change support.
- Production hardening, observability, and performance tuning.
