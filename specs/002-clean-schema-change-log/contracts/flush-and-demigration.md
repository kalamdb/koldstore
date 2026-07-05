# Flush and Demigration Contract

**Feature**: `002-clean-schema-change-log`

## Populated-Table Registration

Registration of a populated table must not flush or delete base rows.

Required sequence:

1. Create table-specific mirror.
2. Register metadata with initialization state.
3. Enable DML capture into the mirror.
4. Initialize mirror entries for existing rows as `op = 1`.
5. Ensure initialization does not overwrite newer committed DML state.
6. Mark initialization complete only after a safe cutoff exists.
7. Allow normal flush policy to process eligible rows afterward.

Flush must skip or block the table/scope while mirror initialization is incomplete.

## Flush Selection

Flush chooses eligible mirror rows using a stable policy-selected set or sequence cutoff:

```text
eligible = mirror rows selected by the active policy at evaluation time
```

Rows outside the selected set or newer than the captured cutoff must not be cleaned from the mirror.

For `rows:N` policies:

1. Read the table/scope mirror and policy metadata.
2. Compute pending rows as mirror rows still hot for that table/scope.
3. Treat `N` as the default hot-row limit.
4. When `pending_count > N`, select the oldest pending rows by `seq` until the table/scope is back within the limit.
5. Capture the selected primary keys and mirror `seq` values, or a sequence cutoff that represents the same stable set.
6. Do not include mirror rows outside that selected set, even if they commit before cleanup.

For `duration:S` policies, select mirror rows whose `changed_at` is older than the configured duration, such as `duration:1d` or `duration:5d`. If an `interval:S` spelling remains, it is a duration alias in seconds and must not mean elapsed time since the last flush.

## Cold Records

Flush writes records that include:

- source primary-key columns
- base-table columns for live insert/update states
- `seq`
- operation value
- change timestamp
- delete marker state
- schema version

For `op = 3`, flush writes a delete-marker cold record. Delete-marker records need the primary key and KoldStore metadata; non-key base-table values are non-authoritative.

## Commit and Cleanup Boundary

Cleanup may run only after:

1. Parquet final object is readable and validated.
2. Manifest visibility boundary is committed.
3. Local cold metadata is updated.

Cleanup may remove:

- matching base-table live rows that were flushed
- matching mirror rows that are no longer needed for hot replay

Cleanup must not remove:

- mirror rows newer than the cutoff
- rows from a failed or uncommitted flush
- mirror tombstones that have not been durably represented when needed to mask older cold rows

## Merge Rules

Logical reads compare hot rows and cold records by primary key and sequence. The newest sequence-bearing state wins. If the winner is a delete marker, the logical row is hidden.

## Latest-State Change Feed

`changes_since` uses the same mirror/cold metadata model:

```text
candidate changes = mirror rows where seq > cursor
                  + cold records with mirror metadata where seq > cursor
```

The feed resolves to the latest available state per primary key and orders returned rows by `seq`. It does not read `koldstore.row_events` and does not promise every intermediate mutation.

## Demigration

Default demigration must:

1. Read the current logical hot+cold state.
2. Preserve that state in the original user table.
3. Disable DML capture and merge-scan management.
4. Drop the table-specific mirror.
5. Remove or deactivate metadata.
6. Leave the user table without KoldStore internal columns.

Failure during demigration must leave the previous managed state retryable.
