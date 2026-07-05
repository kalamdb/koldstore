# Research: Clean Schema Change-Log Mirrors

**Feature**: `002-clean-schema-change-log`  
**Date**: 2026-07-05

## Decision: Use One Per-Table Latest-State Mirror

Use a table-specific mirror in the `koldstore` schema for every managed user table. For `public.messages`, the default mirror is `koldstore.messages__cl`. The mirror stores the source table primary key plus `seq`, `op`, `changed_at`, and nullable `commit_lsn`.

**Rationale**: This keeps the business table clean while preserving the hot replay/flush state previously stored in user-table system columns. A latest-state mirror matches the current product need: decide the newest state for each primary key and flush it safely. It also makes demigration cleanup obvious because the table-specific artifact can be dropped.

**Alternatives considered**:

- Keep `_seq`, `_deleted`, `_commit_seq` on the user table: rejected because it breaks the clean-schema goal.
- Use a single global row-events table: rejected because the feature needs latest state, not full history, and the user explicitly wants `koldstore.row_events` removed as a required artifact.
- Store full row payload in the mirror: rejected because the mirror should not require `row_data`; flush can read live base rows for live operations and only needs PK plus metadata for delete markers.

## Decision: Replace Old Development-Time Design Without Compatibility Migration

Remove old code and tests that exist only for the internal-column or global row-events design. Do not build an old-to-new migration path for already-managed development tables.

**Rationale**: The extension is still in development. Supporting two storage-management designs would increase implementation risk, keep noisy artifacts alive, and make tests less direct. The new clean-schema design should become the default contract.

**Alternatives considered**:

- Add a compatibility mode for old system-column tables: rejected as unnecessary scope for a pre-1.0 extension.
- Write a migration from old system columns to mirrors: rejected because there is no production compatibility requirement and it would distract from the clean default.

## Decision: Preserve Primary-Key Shape Exactly in the Mirror

Mirror primary-key columns must preserve the base table primary-key names, order, PostgreSQL data types, type modifiers, collations, domain identity where applicable, and primary-key-required non-nullability.

**Rationale**: The mirror is keyed by the same logical identity as the source table. Any type or ordering drift can break DML upserts, flush joins, cold PK hints, and demigration. Preserving type modifiers and collations is needed for keys such as constrained varchar, text with non-default collation, and domain-backed identifiers.

**Alternatives considered**:

- Normalize all mirror keys to JSON: rejected because it loses native uniqueness/index behavior and type fidelity.
- Store only a stable PK hash: rejected because the mirror must expose exact PK columns for joins, flush, cleanup, and operator diagnostics.

## Decision: Initialize Populated Tables Into the Mirror, Then Flush Later

For tables that already have rows, enablement creates the mirror and change capture first, initializes mirror rows as live `op = 1`, blocks flush until initialization reaches a safe complete cutoff, and leaves base rows hot during registration. Normal flush policy moves eligible rows later, using the default row-limit policy unless the operator configures a duration policy.

**Rationale**: Registration should not depend on object storage availability and should not delete user rows. Backfilling the mirror first gives pg-koldstore a complete hot replay state before any cleanup can happen. Flush remains the only path that writes cold artifacts and removes hot rows after the cold visibility boundary commits.

**Alternatives considered**:

- Flush existing rows during registration from oldest to newest: rejected because it makes migration depend on object storage and increases failure blast radius.
- Only mirror rows left hot after a registration-time flush: rejected for the same reason and because it complicates partial-failure recovery.
- Require empty tables only: rejected because existing-table adoption is a core user scenario.

## Decision: Reject Primary-Key Alteration for This Feature

Altering primary-key values or primary-key definitions on managed tables is not implemented in this feature and must be rejected or documented as unsupported.

**Rationale**: Primary-key changes require coordinated old-key tombstones, new-key mirror entries, cold hint updates, and possibly constraint revalidation. That is a separate feature. Rejecting or clearly documenting the limitation prevents stale cold state and hidden logical duplicates.

**Alternatives considered**:

- Treat a PK update as delete old + insert new: deferred because it needs end-to-end coverage across mirror, flush, cold hints, and merge.
- Support `ALTER TABLE ... DROP/ADD PRIMARY KEY`: deferred because it changes mirror schema and cold identity.

## Decision: Persist Cold Delete Markers From Mirror Tombstones

Flush writes delete-marker cold records for eligible mirror tombstones when needed to mask older cold rows. Delete markers include the primary key and KoldStore metadata; non-key payload values are not authoritative.

**Rationale**: Once mirror tombstones are cleaned up, older cold live rows must not reappear. Cold delete markers are the durable representation of deletion state after hot replay state is removed.

**Alternatives considered**:

- Keep tombstones forever in the mirror: rejected because it keeps hot replay state unbounded.
- Omit delete markers when row payload is unavailable: rejected because delete correctness depends on metadata, not payload.

## Decision: Keep Commit LSN Nullable and Do Not Recreate Commit Sequence

The mirror includes nullable `commit_lsn` for diagnostics/recovery integration, while ordering uses Snowflake-style `seq`. The old `_commit_seq`/row-events path is retired from the clean-schema default.

**Rationale**: The requested mirror schema includes `commit_lsn PG_LSN NULL` and `seq BIGINT NOT NULL`. Keeping `commit_lsn` nullable avoids blocking DML in cases where an LSN is unavailable or not meaningful at trigger time. Reintroducing `_commit_seq` would preserve the old design noise under a different table.

**Alternatives considered**:

- Keep `_commit_seq` in mirror for change feed compatibility: rejected because row-events/change-feed compatibility is not part of this feature.
- Make `commit_lsn` mandatory: rejected because it can force unnecessary coupling to PostgreSQL WAL timing.

## Decision: Evaluate Row-Limit and Duration Policies From Mirror State

Flush policy evaluation reads the table/scope change-log mirror. `rows:N` is the default policy: `N` is the hot-row limit, and when pending mirror rows exceed that limit pg-koldstore selects the oldest pending rows by mirror `seq`. A duration policy such as `duration:1d` selects mirror rows whose `changed_at` is older than the configured duration. If the implementation keeps an `interval:S` spelling, it is a duration alias in seconds, not a timer since the last flush. Policy evaluation captures a stable eligible set or sequence cutoff so concurrent DML is not flushed or cleaned by the wrong attempt.

**Rationale**: The user table no longer has `_seq`, so policy decisions must come from mirror state. The default experience should be simple row-limit retention, while duration retention gives operators the natural "flush rows older than 1d/5d" behavior. Capturing the selected set/cutoff prevents concurrent DML from being cleaned by the wrong flush.

**Alternatives considered**:

- Count rows in the user table: rejected because it cannot see delete tombstones and would ignore mirror state.
- Treat `interval:S` as elapsed time since the last flush: rejected because the user intent is row age, for example rows older than 1d or 5d.
- Flush all mirror rows at cleanup time without a stable eligible set/cutoff: rejected because concurrent changes could be removed before they are durably cold.

## Decision: Migrate `changes_since` to the Mirror/Cold Latest-State Model

`koldstore.changes_since(table_name regclass, since_commit_seq bigint, limit_rows integer DEFAULT 1000)` should read unflushed rows from the table-specific mirror and flushed mirror metadata from cold records. In clean-schema mode, the existing `since_commit_seq` argument is treated as a last-seen mirror `seq` cursor unless the public signature is renamed before release.

**Rationale**: The old global `koldstore.row_events` table is retired, but callers still need a way to observe changed primary keys. Because the mirror is latest-state only, the feed is also latest-state: if one primary key changed multiple times before the consumer polls, the feed returns the latest available state, not every intermediate event.

**Alternatives considered**:

- Keep `koldstore.row_events` only for `changes_since`: rejected because it keeps the old artifact alive.
- Make `changes_since` scan only the hot mirror table: rejected because flushed mirror rows may be removed after safe cold persistence.
- Promise a full ordered event history from the mirror: rejected because the mirror intentionally stores latest state, not history.

## Decision: Update Tests by Replacing Legacy Assertions

Remove or rewrite tests that assert user-table system columns, global row events, hot tombstone columns, or direct writes to internal columns. Add mirror-focused SQL regression and e2e coverage for clean schema, PK fidelity, DML upserts, populated-table initialization, flush cleanup, cold delete markers, and demigration.

**Rationale**: Tests should describe the new product contract. Keeping old tests as active expectations would conflict with the clean-schema design.

**Alternatives considered**:

- Keep old tests behind a feature flag: rejected because no legacy compatibility mode is planned.
