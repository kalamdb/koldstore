# Quickstart: Validate Clean Schema Change-Log Mirrors

**Feature**: `002-clean-schema-change-log`

This guide describes the local validation flow after implementation. It is intentionally pgrx-first and does not require Docker for correctness tests.

## Prerequisites

- Rust workspace dependencies installed.
- pgrx configured for the PostgreSQL versions under test.
- Local object-storage-compatible path or service configured as required by existing e2e helpers.

## 1. Run Fast Rust Unit Tests

```bash
cargo test \
  -p koldstore-core \
  -p koldstore-catalog \
  -p koldstore-merge \
  -p koldstore-parquet
```

Expected:

- Mirror and PK-shape domain tests pass.
- Row-events-only and system-column-only tests are removed or rewritten.
- Delete-marker merge behavior passes.

## 2. Run Extension SQL Regression Tests

```bash
cargo pgrx test --manifest-path crates/pg_koldstore/Cargo.toml
```

Expected:

- Clean-schema migration creates no user-table KoldStore columns.
- `koldstore.row_events` is not a required default catalog table.
- Per-table `__cl` mirrors preserve source primary-key shape.
- DML upserts mirror rows correctly.
- Managed primary-key alteration is rejected or documented as unsupported.
- Demigration drops mirror/capture artifacts.

## 3. Validate Greenfield and Existing-Table Lifecycles

```bash
cargo test -p e2e --test greenfield_matrix
cargo test -p e2e --test migrate_existing_matrix
```

Expected:

- Greenfield tables remain clean after enablement.
- Populated tables receive mirror rows for all existing primary keys.
- Registration does not flush or delete base rows.
- Backfill does not overwrite newer committed DML state.

## 4. Validate DML and Flush Semantics

```bash
cargo test -p e2e --test flush_to_cold
cargo test -p e2e --test flush_matrix
cargo test -p e2e --test merge_scan_results
```

Expected:

- INSERT/UPDATE/DELETE/reinsert produce one latest mirror row per primary key.
- `rows:1000` policy keeps at most 1000 pending hot mirror rows by flushing the oldest rows by `seq`.
- `duration:1d` policy flushes mirror rows whose `changed_at` is older than 1 day.
- If `interval:86400` remains supported, it behaves as a duration alias in seconds, not elapsed time since the last flush.
- Flush does not clean mirror rows with `seq` above the captured cutoff.
- Flush writes live cold records and required cold delete markers.
- Mirror cleanup happens only after manifest visibility commits.
- Logical reads do not resurrect deleted cold rows.
- Reinsert with a newer sequence wins over older delete markers.

## 5. Validate Change Feed From Mirror/Cold Metadata

```bash
cargo test -p e2e --test change_feed
```

Expected:

- `changes_since` does not require `koldstore.row_events`.
- Before flush, `changes_since` reads rows from the table-specific `__cl` mirror.
- After flush and safe mirror cleanup, `changes_since` can read flushed cold records that include mirror metadata.
- Repeated changes to one primary key return latest-state feed rows, not every intermediate event.

## 6. Validate Demigration

```bash
cargo test -p e2e --test demigrate_matrix
cargo test -p e2e --test full_lifecycle
```

Expected:

- The original table contains current logical rows after demigration.
- The table-specific mirror is dropped.
- DML capture and merge management are disabled.
- The user table remains free of KoldStore internal columns.

## 7. Documentation Check

Inspect README and generated docs:

- Known limitations mention that altering primary-key values or definitions on managed tables is not implemented yet.
- Default migration docs describe clean-schema mirror behavior, not user-table system columns.
- Flush policy docs say `rows:N` is the default hot-row-limit policy evaluated from mirror state.
- Flush policy docs say duration policies select rows by mirror `changed_at` row age.
- Change-feed docs say `changes_since` uses mirror/cold metadata, not `koldstore.row_events`.
