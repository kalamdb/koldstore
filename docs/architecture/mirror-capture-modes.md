# Mirror Capture Modes

KoldStore keeps a managed table's latest-state mirror in sync with its
PostgreSQL heap in one of two modes, selected once at `manage_table`:

| Mode | Doc | Default? | Summary |
| --- | --- | --- | --- |
| **Strict** | [mirror-capture-strict.md](mirror-capture-strict.md) | Yes | Mirror writes run in the user transaction |
| **Async** | [mirror-capture-async.md](mirror-capture-async.md) | Opt-in | Committed WAL is applied by a background worker |

Both modes write the same mirror schema and Parquet format. They differ only in
**when** a committed heap change is copied to the mirror.

## Choose a mode

| Property | Strict | Async |
| --- | --- | --- |
| Capture path | Statement trigger in the user transaction | Committed WAL through a logical slot |
| Foreground commit | Waits for heap and mirror writes | Waits for the heap write only |
| Mirror visibility | Immediate, including read-your-writes | Normally within ~100 ms; fence for an exact boundary |
| Rollback | Heap and mirror roll back together | Aborted WAL is never decoded |
| Setup | No replication setup | `wal_level=logical`; KoldStore creates publication and slot |
| WAL retention risk | None beyond normal PostgreSQL | Slot retains WAL until acknowledged |
| Best fit | Strong consistency and simple operations | Insert-heavy workloads with a controlled catch-up point |

Use **strict** when application code may write and then immediately depend on the
mirror/cold overlay, or when logical-slot operations are undesirable.

Use **async** when a short mirror lag is acceptable. A database-scoped worker
applies committed WAL continuously; call the consistency fence before operations
that need a precise read boundary.

## Stored configuration

The selected mode is stored in `koldstore.schemas.options` as
`mirror_capture_mode`. A missing property means `strict`. Async mode is
persisted as:

```json
{
  "mirror_capture_mode": "async"
}
```

Changing modes on an already-managed table is not a public operation today.

## Related

- [ADR-003: Optional async mirror capture](../decisions/003-optional-async-mirror-capture.md)
- [DML table](dml-table.md)
- [Manage table](manage-table.md)
- [Flushing table](flushing-table.md)
