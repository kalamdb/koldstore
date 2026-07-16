# Strict Mirror Capture

Strict mode is the default. Heap mutation, mirror mutation, and row-counter
deltas share the application transaction so reads see a consistent latest-state
mirror immediately after commit.

See also the [mode comparison](mirror-capture-modes.md) and
[async mode](mirror-capture-async.md).

## Enable

Omit the argument (default) or set it explicitly:

```sql
SELECT koldstore.manage_table(
  table_name          => 'public.events',
  storage             => 'archive',
  hot_row_limit       => 100000,
  mirror_capture_mode => 'strict'
);
```

## How capture works

`manage_table` installs three `AFTER ... FOR EACH STATEMENT` triggers that use
transition tables:

| Event | Mirror write |
| --- | --- |
| INSERT | `ON CONFLICT` for small batches; `MERGE` for large batches |
| UPDATE | Direct update of existing mirror rows (`NEW` transition table) |
| DELETE | Direct update/delete of existing mirror rows (`OLD` transition table) |

A separate `BEFORE UPDATE OF <primary key>` trigger rejects primary-key changes
so the mirror cannot be left pointing at a stale key.

## Transaction coupling

The source heap mutation, mirror mutation, and row-counter delta run in the same
user transaction:

- An error aborts all three.
- A successful statement is immediately visible through `KoldMergeScan`.
- There is no logical slot, publication, or background applier.

## Operations notes

- No `wal_level=logical` requirement.
- No WAL retention risk beyond normal PostgreSQL behavior.
- Primary-key updates remain unsupported (same as async).
- Prefer strict when the application reads the mirror/cold overlay in the same
  flow that just wrote the heap.

## Test contract

The main change-log E2E suite runs the same behavior matrix in strict mode:
insert, update, delete, reinsert, rollback, no-op PK assignment, rejected PK
mutation, bulk update/delete, latest-state uniqueness, and row-counter accuracy.
Run with `scripts/run-pg-e2e.sh 16 --mode strict`.
