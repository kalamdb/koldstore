# Change-Log Mirror and Transaction Workflow

## Status

Accepted for the clean-schema default (`002-clean-schema-change-log`).

## Context

Clean-schema mode keeps user tables free of KoldStore system columns (`_seq`,
`_deleted`, `_commit_seq`, `_user_id`). Instead, each managed table gets an
internal **latest-state change-log mirror** in the `koldstore` schema.

For `public.messages`, the default mirror is `koldstore.messages__cl`. The
mirror stores:

- the same primary-key columns as the user table;
- `seq` — Snowflake-style effect id for ordering and flush cutoffs;
- `op` — `1 = INSERT`, `2 = UPDATE`, `3 = DELETE`;
- `changed_at` — timestamp stamped at capture time;
- `commit_lsn` — optional PostgreSQL WAL LSN for diagnostics.

The mirror is **not** a full event-history log. It records at most one row per
primary key: the latest committed hot state for that key.

## Mirror Shape

```sql
CREATE TABLE koldstore.messages__cl (
  tenant_id uuid NOT NULL,
  id bigint NOT NULL,
  seq bigint NOT NULL,
  op smallint NOT NULL,
  changed_at timestamptz NOT NULL DEFAULT now(),
  commit_lsn pg_lsn NULL,
  PRIMARY KEY (tenant_id, id)
);
```

The user table schema is unchanged. Capture, flush policy, and change-feed
cursors all read mirror state instead of internal user-table columns.

## Capture Mechanism

Enablement installs three **AFTER ROW** triggers on the user table:

| Trigger | Fires on | Mirror effect |
|---------|----------|---------------|
| `*_insert_capture` | `INSERT` | Upsert PK with `op = 1` and new `seq` |
| `*_update_capture` | `UPDATE` | Upsert PK with `op = 2` and new `seq` |
| `*_delete_capture` | `DELETE` | Upsert PK with `op = 3` and new `seq` |

All three call one PL/pgSQL function (for example
`koldstore.messages__cl_capture()`). Each branch performs an upsert:

```sql
INSERT INTO koldstore.messages__cl (tenant_id, id, seq, op, changed_at, commit_lsn)
VALUES (NEW.tenant_id, NEW.id, SNOWFLAKE_ID(), 1, now(), pg_current_wal_lsn())
ON CONFLICT (tenant_id, id) DO UPDATE
SET seq = EXCLUDED.seq,
    op = EXCLUDED.op,
    changed_at = EXCLUDED.changed_at,
    commit_lsn = EXCLUDED.commit_lsn;
```

Rules enforced by capture:

- Every DML effect allocates a **fresh** `SNOWFLAKE_ID()` for `seq`.
- Reinsert after delete uses the INSERT branch and overwrites the tombstone back
  to `op = 1`.
- Primary-key value updates are rejected; managed tables do not support PK
  mutation.
- The mirror must not store `row_data`, `cold_segment_id`, or old system-column
  names.

## Transaction Workflow

Mirror capture runs **during statement execution**, not at `COMMIT`. Each
affected row fires its AFTER ROW trigger in the **same transaction** as the
user DML.

```text
BEGIN
  |
  +-- INSERT row A  --> trigger upserts mirror for PK A (uncommitted)
  |
  +-- UPDATE row A  --> trigger overwrites mirror for PK A (uncommitted)
  |
  +-- DELETE row B  --> trigger upserts tombstone for PK B (uncommitted)
  |
COMMIT  --> mirror rows become visible to other sessions
```

Or, on failure:

```text
BEGIN
  +-- DML + mirror upserts ...
ROLLBACK  --> user-table and mirror changes disappear together
```

### Visibility

| Observer | When mirror changes are visible |
|----------|----------------------------------|
| Same transaction | Immediately after each statement's trigger runs |
| Other transactions | Only after the writing transaction commits |
| On rollback | Never — mirror state reverts with the user table |

There is no separate capture step at commit time. `COMMIT` only publishes rows
the triggers already wrote inside the transaction.

### Same primary key, multiple effects in one transaction

Because the mirror is latest-state only, intermediate effects on the **same PK**
are collapsed. Only the final `op` and `seq` survive after commit.

Example:

```sql
BEGIN;
INSERT INTO messages (tenant_id, id, body) VALUES ('t1', 1, 'a');
UPDATE messages SET body = 'b' WHERE tenant_id = 't1' AND id = 1;
DELETE FROM messages WHERE tenant_id = 't1' AND id = 1;
COMMIT;
```

Inside the transaction the mirror row for `(t1, 1)` is rewritten three times.
After commit, other sessions see **one** row:

| Column | Value |
|--------|-------|
| `op` | `3` (DELETE tombstone) |
| `seq` | `S₃` — Snowflake id from the DELETE trigger |
| `changed_at` | timestamp from the DELETE trigger |

The `seq` values from the INSERT and UPDATE (`S₁`, `S₂`) are overwritten and
are not recoverable from the mirror.

### Multiple primary keys in one transaction

Different PKs keep **separate** mirror rows. Each effect still gets its own
`seq`.

```sql
BEGIN;
INSERT INTO messages (tenant_id, id, body) VALUES ('t1', 1, 'a');  -- S₁, op=1
INSERT INTO messages (tenant_id, id, body) VALUES ('t1', 2, 'b');  -- S₂, op=1
UPDATE messages SET body = 'c' WHERE tenant_id = 't1' AND id = 1;  -- S₃, op=2
COMMIT;
```

After commit:

| PK | `seq` | `op` |
|----|-------|------|
| `(t1, 1)` | `S₃` | `2` |
| `(t1, 2)` | `S₂` | `1` |

Assuming Snowflake ids increase over time in the session, `S₂ < S₃`, so the
final update to PK 1 happened after the insert of PK 2.

### Multi-row statements

A bulk `INSERT ... VALUES (...), (...)` or `UPDATE ... WHERE` that touches N
rows fires the trigger **once per row**. Each row gets its own `SNOWFLAKE_ID()`
and mirror upsert in statement order.

## Ordering Semantics

Clean-schema mode uses mirror `seq` as the primary ordering key. The old
`_commit_seq` commit-order cursor and `koldstore.row_events` append-only feed are
retired from the default path.

| Question | How to answer it |
|----------|------------------|
| Which PK changed later across a table? | Compare final mirror `seq` values. |
| What is the latest state of one PK? | Read the single mirror row for that PK. |
| What intermediate steps happened to one PK in one transaction? | **Not stored.** Only the final `op` and `seq` remain. |
| Flush cutoff / hot-row limit | Select oldest pending mirror rows by `seq`. |
| Duration flush policy | Select mirror rows whose `changed_at` is older than the threshold. |
| `changes_since` cursor | Treat `since_commit_seq` as a last-seen mirror `seq`; return rows with `seq > cursor` ordered by `seq`. |

`seq` gaps are allowed. Snowflake ids are monotonic effect identifiers, not a
dense commit-order sequence.

`changed_at` is set with `now()` at trigger time. It supports duration-based
flush policy but is not the primary global ordering key.

`commit_lsn` is nullable and optional. It records `pg_current_wal_lsn()` when
available for diagnostics and recovery correlation. It does not drive flush or
change-feed ordering.

## State Transitions Per Primary Key

```text
missing  --INSERT-->  op=1
op=1     --UPDATE-->  op=2  (new seq)
op=2     --UPDATE-->  op=2  (new seq)
op=1/2   --DELETE-->  op=3  (tombstone, new seq)
op=3     --INSERT-->  op=1  (reinsert, new seq)
```

DELETE keeps a tombstone mirror row (`op = 3`) until flush cleanup removes it
after safe cold persistence.

## Relationship to Flush and Change Feed

**Flush** reads eligible rows from the mirror, not from user-table system
columns:

1. Policy evaluation scans pending mirror rows (`rows:N` by `seq`, or
   `duration:1d` by `changed_at`).
2. The job captures a stable `seq` cutoff or row set at evaluation time.
3. Flush writes base-table columns plus mirror metadata to cold Parquet.
4. Mirror and base rows are cleaned only after cold visibility is committed.
5. Rows newer than the cutoff stay hot.

**`changes_since`** returns latest-state deltas from unflushed mirror rows and
flushed cold records that carry mirror metadata. It does **not** replay every
intermediate event. Consumers that need full per-mutation history require a
separate future feature.

## Contrast With the Old System-Column Design

| Aspect | Old design | Clean-schema mirror |
|--------|------------|---------------------|
| Hot state tracking | `_seq`, `_deleted` on user table | Mirror `seq`, `op` in `koldstore.*__cl` |
| Commit-order cursor | `_commit_seq` under advisory lock | Not used; mirror `seq` only |
| Change feed source | `koldstore.row_events` | Per-table mirror + cold metadata |
| User table schema | Gains system columns | Unchanged |
| History | Row events were append-only | Mirror is latest-state only |

See [existing-table-migration-and-flush.md](./existing-table-migration-and-flush.md)
for the previous system-column migration and flush job model. That document
describes the legacy path; new clean-schema work replaces system-column capture
with per-table mirrors.

## Invariants

- At most one mirror row per managed primary key.
- Every committed DML effect on a PK assigns a strictly newer `seq` than the
  previous committed state for that PK.
- Mirror mutations commit or roll back with the user-table statement that caused
  them.
- Other transactions never observe partial mirror state from an uncommitted
  transaction.
- Flush and `changes_since` never require `koldstore.row_events` in clean-schema
  mode.

## Related Specs

- `specs/002-clean-schema-change-log/data-model.md`
- `specs/002-clean-schema-change-log/contracts/change-log-mirror.md`
- `specs/002-clean-schema-change-log/contracts/sql-api.md`
