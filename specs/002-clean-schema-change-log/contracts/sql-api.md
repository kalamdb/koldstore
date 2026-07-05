# SQL API Contract: Clean Schema Default

**Feature**: `002-clean-schema-change-log`

## Table Enablement

`koldstore.migrate_table` remains the public entry point for enabling a normal PostgreSQL table.

```sql
SELECT koldstore.migrate_table(
  table_name   => 'chat.messages',
  table_type   => 'shared',
  storage_name => 'local-minio',
  flush_policy => 'rows:1000'
);
```

User-scoped tables must provide an existing application-owned scope column:

```sql
SELECT koldstore.migrate_table(
  table_name   => 'chat.messages',
  table_type   => 'user',
  storage_name => 'local-minio',
  flush_policy => 'rows:1000',
  scope_column => 'user_id'
);
```

### Enablement Behavior

1. Validate the source table has a primary key.
2. Validate all primary-key columns and flush-relevant columns are supported.
3. Reject user-scoped tables without a valid existing scope column.
4. Create one table-specific change-log mirror in `koldstore`.
5. Preserve the source table schema exactly; do not add KoldStore columns.
6. Register table metadata, mirror identity, primary-key shape, storage binding, scope, and flush policy.
7. Enable DML capture into the mirror.
8. For populated tables, initialize mirror entries for existing rows without flushing or deleting base rows during registration.

### Removed Default Behavior

Clean-schema enablement does not:

- Add `_seq`, `_deleted`, `_commit_seq`, `_user_id`, or other KoldStore columns to the user table.
- Create `koldstore.row_events` as a required default artifact.
- Support migration from old development-time managed tables that used internal user-table columns.

## DML Behavior

Applications continue to use ordinary SQL:

```sql
INSERT INTO chat.messages (id, user_id, body) VALUES (...);
UPDATE chat.messages SET body = 'updated' WHERE id = ...;
DELETE FROM chat.messages WHERE id = ...;
```

Managed DML effects:

| User operation | Mirror effect |
|----------------|---------------|
| INSERT | Upsert PK into mirror with `op = 1` and new `seq`. |
| UPDATE | Upsert PK into mirror with `op = 2` and new `seq`. |
| DELETE | Upsert/keep PK tombstone with `op = 3` and new `seq`. |
| INSERT after DELETE | Update existing mirror tombstone to `op = 1` and new `seq`. |

Primary-key value updates and primary-key definition changes on managed tables are not implemented and must be rejected or documented as unsupported.

## Flush Control

Existing flush functions remain the public control surface:

```sql
SELECT koldstore.flush_table('chat.messages'::regclass, force => true);
SELECT koldstore.flush_pending();
SELECT koldstore.set_flush_policy('chat.messages'::regclass, 'rows:1000');
SELECT koldstore.set_flush_policy('chat.messages'::regclass, 'duration:1d');
```

Flush selects eligible rows from mirror sequence state, not from internal columns on the user table.

### Flush Policy Semantics

For the default row-limit policy:

```text
rows:1000
```

pg-koldstore evaluates policy state from the table/scope change-log mirror:

1. `rows:1000` means keep at most 1000 pending hot mirror rows for the table/scope.
2. When the table/scope exceeds that limit, pg-koldstore selects the oldest pending mirror rows by `seq`.
3. The flush may process only the selected rows or rows at or below the captured row-limit cutoff.
4. Mirror rows outside the selected set remain pending for a later flush.

For a duration policy:

```text
duration:1d
```

pg-koldstore selects mirror rows whose `changed_at` is older than the configured duration. If the current public syntax keeps `interval:86400`, it is treated as a duration alias in seconds, not elapsed time since the last flush.

## Demigration

```sql
SELECT koldstore.demigrate_table(
  table_name => 'chat.messages'::regclass,
  rehydrate  => true,
  drop_cold  => false
);
```

Demigration must:

1. Preserve current logical table contents in the original user table.
2. Drop table-specific DML capture.
3. Drop the table-specific change-log mirror.
4. Remove or deactivate metadata.
5. Leave the user table schema clean.

`drop_system_columns` is not part of the clean-schema default because no system columns are added.

## Change Feed

The public change-feed function is migrated away from `koldstore.row_events`:

```sql
koldstore.changes_since(
  table_name regclass,
  since_commit_seq bigint,
  limit_rows integer DEFAULT 1000
) RETURNS SETOF koldstore.change_event
```

Clean-schema behavior:

1. Read unflushed latest-state changes from the table-specific change-log mirror.
2. Read flushed latest-state changes from cold records that contain mirror metadata.
3. Treat the current `since_commit_seq` argument as a last-seen mirror `seq` cursor unless the public signature is renamed before release.
4. Return rows with `seq > cursor`, ordered by `seq`, limited by `limit_rows`.
5. Return the latest available state per primary key, not every intermediate event.
6. Return deletes as `op = 3`.
7. Do not require or read `koldstore.row_events`.

Any future full-history event feed must be specified separately.
