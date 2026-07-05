# Test Plan Contract: Clean Schema Change-Log Mirrors

**Feature**: `002-clean-schema-change-log`

## Local Verification Policy

Use local pgrx-managed PostgreSQL for default verification. Do not make tests under `tests/` depend on Docker or Docker Compose.

## Unit Tests

- Primary-key shape captures names, order, type identity, typmod, collation, domain identity, and non-nullability.
- Mirror operation enum maps exactly to `1`, `2`, `3`.
- Mirror latest-state transitions cover insert, update, delete, and reinsert.
- Flush cutoff excludes newer mirror rows.
- `rows:1000` policy evaluation uses mirror pending-count and oldest-sequence ordering.
- `duration:1d` policy evaluation uses mirror `changed_at` row age.
- Delete-marker cold records hide older cold live rows.
- `changes_since` returns latest-state rows from mirror/cold metadata by mirror sequence.
- Removed row-events/system-column models have no remaining required default consumers.

## SQL Regression Tests

Add or rewrite tests in `crates/pg_koldstore/tests/`:

- Enabling a table does not add `_seq`, `_deleted`, `_commit_seq`, `_user_id`, or any KoldStore column to the user table.
- The mirror has the same primary-key columns and PK metadata as the source table.
- Single-column and composite primary keys work.
- INSERT, UPDATE, DELETE, and reinsert upsert exactly one mirror row.
- DELETE keeps a tombstone mirror row.
- Populated-table enablement initializes mirror rows without flushing/deleting base rows.
- Backfill does not overwrite newer committed DML state.
- Flush is blocked or skipped while mirror initialization is incomplete.
- `rows:1000` keeps at most 1000 pending hot mirror rows by flushing the oldest rows by `seq`.
- `duration:1d` flushes mirror rows whose `changed_at` is older than 1 day.
- If `interval:86400` remains supported, it is tested as a row-age alias for `duration:1d`, not as time since last flush.
- A policy-triggered flush captures the selected mirror rows/cutoff and does not clean rows outside that selected set.
- The extension default does not create or require `koldstore.row_events`.
- `changes_since(table, cursor, limit)` reads unflushed mirror rows before flush.
- `changes_since(table, cursor, limit)` reads flushed cold mirror metadata after mirror cleanup.
- `changes_since` returns latest-state changes and does not claim every intermediate event for repeated updates to one key.
- Old system-column guard tests are removed or rewritten because user-table internal columns no longer exist.
- Managed primary-key alteration is rejected or documented as unsupported.
- Demigration drops mirror/capture artifacts and leaves the user table clean.

## E2E Tests

Update local e2e tests under `tests/e2e/`:

- Greenfield migration matrix validates clean schema and mirror creation.
- Existing-table migration matrix validates populated mirror initialization.
- Flush tests validate cold live records, cold delete markers, mirror cleanup, and failure behavior.
- Merge tests validate delete-marker masking and reinsert winning by newer sequence.
- Change-feed tests validate mirror/cold-backed `changes_since` without `koldstore.row_events`.
- Demigration tests validate current logical data remains and table-specific artifacts are removed.
- Full lifecycle test covers enable -> DML -> flush -> delete -> reinsert -> demigrate.

## Documentation Checks

- README limitations mention that altering primary-key values or definitions on managed tables is not implemented yet.
- README and quickstart no longer claim user-table system columns are part of the default contract after this feature lands.
