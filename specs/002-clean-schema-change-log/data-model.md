# Data Model: Clean Schema Change-Log Mirrors

**Feature**: `002-clean-schema-change-log`  
**Date**: 2026-07-05

## Overview

Clean-schema pg-koldstore models a managed table as:

```text
user table             = application-owned heap table with no KoldStore columns
change-log mirror      = one latest-state mirror row per primary key
cold record            = base columns + KoldStore metadata persisted during flush
logical current row    = newest state by primary key; delete-marker winners are hidden
```

`koldstore.row_events` and user-table system columns are not part of the clean-schema default.

## Entity Relationship

```text
koldstore.storage
  -> koldstore.schemas / managed table metadata
  -> per-table change-log mirror
  -> koldstore.manifest
  -> koldstore.segments
  -> koldstore.cold_pk_hints
  -> latest-state changes_since from mirror + cold metadata

user table
  -> unchanged application columns and primary key
  -> DML capture into mirror
```

## User Table

Application-owned PostgreSQL heap table.

| Attribute | Rules |
|-----------|-------|
| Table name | Original table name remains the application-facing relation. |
| Columns | Must remain unchanged by clean-schema enablement. |
| Primary key | Required; preserved on the user table. |
| Scope column | Required for user-scoped tables and must be application-owned. |
| KoldStore internals | `_seq`, `_deleted`, `_commit_seq`, `_user_id`, and similar columns are not added. |

Validation:

- Reject enablement without a primary key.
- Reject user-scoped enablement without an existing valid scope column.
- Reject or document unsupported primary-key value/definition alteration.
- No old-format migration from existing development-time managed tables is required.

## Primary Key Shape

Typed representation of source table primary-key metadata.

| Field | Rules |
|-------|-------|
| `column_name` | Preserve exact PostgreSQL identifier. |
| `ordinal` | Preserve primary-key order. |
| `type_oid` / `type_name` | Preserve PostgreSQL type identity. |
| `typmod` | Preserve type modifier, such as constrained varchar length. |
| `collation` | Preserve collation for collatable key types. |
| `domain_identity` | Preserve domain identity where applicable instead of silently flattening. |
| `not_null` | Mirror key column is non-null because it participates in the primary key. |

## Change-Log Mirror

One internal table per enabled user table, stored in the `koldstore` schema.

| Field | Type | Rules |
|-------|------|-------|
| Source PK columns | Same as user table PK | Same names, order, PostgreSQL data types, typmods, collations, domain identity where applicable, and non-nullability. |
| `seq` | `BIGINT NOT NULL` | New Snowflake-style effect id for every mirror state change. |
| `op` | `SMALLINT NOT NULL` | `1 = INSERT`, `2 = UPDATE`, `3 = DELETE`. |
| `changed_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Commit-time or statement-time change timestamp. |
| `commit_lsn` | `PG_LSN NULL` | Optional diagnostic/recovery LSN. |

Validation:

- Primary key of the mirror is the same ordered PK column set.
- Mirror contains at most one row per source primary key.
- Mirror must not contain `row_data` or `cold_segment_id`.
- Default name is `<table_name>__cl`, for example `koldstore.messages__cl`; metadata records the actual mirror identity.

## Mirror Entry

Latest-state row for one primary key.

| State | Meaning |
|-------|---------|
| `op = 1` | Latest visible hot state is insert/reinsert. |
| `op = 2` | Latest visible hot state is update. |
| `op = 3` | Latest state is delete tombstone. |

Transitions:

```text
missing -> INSERT -> op=1
op=1   -> UPDATE -> op=2
op=2   -> UPDATE -> op=2 with newer seq
op=1/2 -> DELETE -> op=3
op=3   -> INSERT -> op=1 with newer seq
```

Rules:

- Every committed DML effect gets a newer `seq`.
- Mirror mutations commit or roll back with the user-table statement.
- Rollback must leave no committed mirror change.

## Mirror Initialization

Populated-table enablement phase.

| State | Meaning |
|-------|---------|
| `not_started` | Metadata exists but no complete mirror state is available. |
| `capturing` | DML capture is active; backfill may be scanning existing rows. |
| `complete` | Every pre-existing row has a live mirror entry unless newer DML superseded it. |
| `failed` | Initialization failed and must be retried or rolled back. |

Rules:

- Establish mirror and capture before rows are eligible for flush cleanup.
- Insert existing rows into the mirror as `op = 1`.
- Do not flush or delete base rows during registration.
- Backfill must not overwrite newer committed DML state.
- Flush is blocked or skipped until a complete safe cutoff exists.

## Cold Record

Record persisted to Parquet during flush.

| Field | Meaning |
|-------|---------|
| Source PK columns | Merge identity. |
| Base table columns | Live row values for insert/update states. |
| `seq` | Mirror sequence being flushed. |
| `op` | Mirror operation value. |
| `changed_at` | Mirror change timestamp. |
| `deleted` | Derived delete marker; true when `op = 3`. |
| `schema_version` | Managed schema version for reader coercion. |

Rules:

- Live records include the base row values.
- Delete-marker records require PK and KoldStore metadata; non-key payload values are non-authoritative.
- Cold records are immutable after manifest visibility.

## Managed Table Metadata

Catalog state that links a user table to its mirror and cold storage.

| Field | Rules |
|-------|-------|
| `table_oid` | Source table identity. |
| `mirror_relation` | Actual mirror relation identity. |
| `primary_key_shape` | Ordered PK metadata used for mirror creation and validation. |
| `table_type` | `shared` or `user`. |
| `scope_column` | Existing application-owned scope column for user tables. |
| `storage_id` | Registered storage binding. |
| `flush_policy` | Optional policy. |
| `last_flush_seq` | Last successfully flushed mirror sequence for row-limit policy evaluation. |
| `last_flush_at` | Last successful flush time for observability and job scheduling, not duration row selection. |
| `initialization_state` | Mirror initialization state. |
| `schema_version` | Managed schema/cold schema version. |

## Flush Policy State

Per table/scope policy state for policies such as `rows:1000` or `duration:1d`.

| Field | Rules |
|-------|-------|
| `row_limit` | Parsed from `rows:N`; default policy that keeps at most `N` pending hot mirror rows by flushing the oldest entries by `seq`. |
| `duration_threshold` | Parsed from `duration:S`; optional policy that selects mirror rows whose `changed_at` is older than `S`. |
| `interval_seconds` | Compatibility spelling for duration in seconds if the public syntax keeps `interval:S`. |
| `last_flush_seq` | Highest mirror sequence durably represented by the last successful row-limit flush. |
| `last_flush_at` | Timestamp of last successful flush for observability/scheduling only. |
| `pending_count` | Count of mirror rows newer than `last_flush_seq` or otherwise still hot. |

Rules:

- Policy evaluation reads the change-log mirror, not the user table.
- `rows:N` is the default policy and selects the oldest pending latest-state mirror rows by `seq` when the table/scope exceeds the configured hot-row limit.
- `duration:S` selects rows by mirror `changed_at` age. `interval:S`, if retained, is a duration alias and not elapsed time since the last flush.
- When policy evaluation selects rows, capture a stable eligible set or `seq` cutoff for that flush.
- Concurrent mirror rows outside the selected set remain for a later flush.

## Flush Cutoff

Stable sequence boundary for a flush attempt.

Rules:

- Includes only mirror rows with `seq <= cutoff`.
- Excludes mirror rows newer than the cutoff from cleanup.
- Cleanup happens only after Parquet and manifest commit succeed.
- Cleanup may remove matching mirror rows only when no longer needed for hot replay.

## Latest-State Change Feed

`koldstore.changes_since` output derived from mirror/cold state.

| Field | Rules |
|-------|-------|
| `cursor` | The current `since_commit_seq` argument is interpreted as last-seen mirror `seq` in clean-schema mode unless renamed before release. |
| `source_hot` | Unflushed rows in the table-specific change-log mirror. |
| `source_cold` | Flushed cold records that include mirror metadata. |
| `ordering` | Return rows ordered by `seq`, bounded by `limit_rows`. |
| `scope` | Apply table and user-scope filtering before returning changes. |

Rules:

- Does not read `koldstore.row_events`.
- Returns latest available state per primary key, not every intermediate mutation.
- Delete states are returned with `op = 3`.
- If the cursor predates retained mirror/cold metadata, return a clear unsupported-range or gap error.

## Demigration Operation

Table-management exit path.

Rules:

- Default demigration preserves current logical rows in the original user table.
- Drop table-specific DML capture.
- Drop the table-specific mirror.
- Remove or deactivate metadata.
- Leave no user-table internal KoldStore columns because clean-schema mode never added them.
