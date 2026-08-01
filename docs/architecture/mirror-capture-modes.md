# Mirror Capture (WAL-Only)

KoldStore keeps each managed table's latest-state mirror in sync with its
PostgreSQL heap through **one** capture path: committed WAL applied by a
serialized database worker.

```text
PostgreSQL heap
    -> committed WAL
    -> database-scoped KoldStore applier
    -> latest-state __cl mirror
    -> Parquet cold storage
```

There is no strict / trigger capture mode. Foreground DML writes the heap only;
the applier allocates authoritative `seq` values while processing already-committed
changes.

## Contract

| Property | Behavior |
| --- | --- |
| Capture path | Committed WAL through a logical slot |
| Foreground commit | Waits for the heap write only |
| Mirror visibility | Bounded lag; call `koldstore.wait_for_async_mirror()` for an exact boundary |
| Rollback | Aborted WAL is never decoded |
| Setup | `wal_level=logical`; KoldStore creates publication and slot |
| WAL retention | Slot retains WAL until acknowledged |
| Ordering cursor | Exclusive `seq > last_seq` for `changes_since`, flush, and winner selection |

`applied_lsn` is an internal recovery checkpoint only. It is not the public
changes cursor.

## Activation

`manage_table` always configures WAL capture:

1. Require `wal_level=logical` and provision slot/publication.
2. Create the `__cl` mirror **without** INSERT/UPDATE/DELETE capture triggers
   (PK-update guard only).
3. Under the database apply lock and a short source-table lock: publish the
   table, record `activation_lsn`, set `initialization_state` to `backfilling`
   (or complete immediately for empty tables).
4. Snapshot-backfill into `__cl` with snowflake `seq`. Concurrent DML hits WAL.
5. Set `catching_up`, apply catch-up with
   `next_id_after(max(durable_watermark, max(mirror.seq), prune_floor))`.
6. Mark `complete` / `active`, release the apply lock, ensure the worker.

Existing active tables do not lose changes while another table activates: the
slot retains WAL for the duration of the apply lock.

## Sequence invariant

Authoritative mirror `seq` values are allocated only on the WAL apply path.
Allocation is always above a durable `seq_high_watermark` on
`koldstore.async_mirror_state`, covering worker restart, PostgreSQL restart,
clock regression, crash/retry, and flush prune fences.

## Consistency fence

Heap-only PostgreSQL reads see committed source changes immediately. Merge
scans that depend on a new mirror version may need:

```sql
SELECT koldstore.wait_for_async_mirror();
```

`flush_table` fences automatically. Operations that need an exact mirror
boundary must fence explicitly or internally.

## Retained-WAL safety

`koldstore.async_mirror_max_retained_bytes` is a health threshold, not apply
admission. Crossing it marks `async_mirror_status().retention.ok` false while
the consumer keeps draining. PostgreSQL disk monitoring and slot-retention
settings are independent hard safeguards.

## Related

- [mirror-capture-async.md](mirror-capture-async.md) — apply worker details
- [ADR-003](../decisions/003-optional-async-mirror-capture.md) — historical
  (strict/async dual mode; superseded by WAL-only)
- [ADR-005](../decisions/005-async-apply-progress-and-health.md)
- [Manage table](manage-table.md)
- [Flushing table](flushing-table.md)
