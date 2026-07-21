# Mirror Capture Modes

KoldStore keeps a managed table's latest-state mirror in sync with its
PostgreSQL heap in one of two modes, selected once at `manage_table`:

| Mode | Doc | Default? | Summary |
| --- | --- | --- | --- |
| **Strict** | [mirror-capture-strict.md](mirror-capture-strict.md) | Yes | Mirror writes run in the user transaction |
| **Async** | [mirror-capture-async.md](mirror-capture-async.md) | Opt-in | Committed WAL is applied by a background worker |

Both modes write the same mirror schema and Parquet format. They differ in
**when** a committed heap change reaches the mirror, which transaction owns the
write, and how missing mirror rows are handled during replay.

## Choose a mode

| Property | Strict | Async |
| --- | --- | --- |
| Capture path | Statement trigger in the user transaction | Committed WAL through a logical slot |
| Foreground commit | Waits for heap and mirror writes | Waits for the heap write only |
| Mirror visibility | Immediate, including read-your-writes | Normally within ~100 ms; fence for an exact boundary |
| UPDATE mirror plan | Direct transition-table UPDATE; missing rows fail the source statement | Direct batched UPDATE for existing rows plus insert-missing fallback for flush-pruned rows |
| Rollback | Heap and mirror roll back together | Aborted WAL is never decoded |
| Setup | No replication setup | `wal_level=logical`; KoldStore creates publication and slot |
| WAL retention risk | None beyond normal PostgreSQL | Slot retains WAL until acknowledged |
| Backlog scheduling | Not applicable | Bounded batches; up to four immediate pending ticks, then a latch yield |
| Best fit | Strong consistency and simple operations | Write-heavy workloads with bounded lag and controlled consistency points |

Use **strict** when application code may write and then immediately depend on the
mirror/cold overlay, or when logical-slot operations are undesirable.

Use **async** when a short mirror lag is acceptable. A database-scoped worker
applies committed WAL continuously; call the consistency fence before operations
that need a precise read boundary.

## Performance contracts

The modes intentionally have different acceptance gates:

- Async foreground hot UPDATE p95 must remain within **1.10×** of a regular
  heap for the same isolated small-statement workload. Deferred apply is also
  measured separately; foreground parity alone is not a sustainable-throughput
  claim.
- Strict hot UPDATE p95 may be up to **2.00×** the heap baseline because the
  mirror write is part of the source transaction. This is the explicit cost of
  immediate consistency.
- Worker-on async release testing must show bounded backlog at the supported
  load and a documented drain time after load stops.

The executable gates live in `benchmarks/src/verdict.rs`; the benchmark runner
selects them with `--mirror-capture-mode async|strict`. Publication methodology
is documented in [benchmarks](../benchmarks/README.md).

## Retained-WAL safety model

`koldstore.async_mirror_max_retained_bytes` is a health threshold, not apply
admission. Crossing it marks `async_mirror_status().retention.ok` false while
the consumer keeps draining. PostgreSQL disk monitoring and slot-retention
settings are independent hard safeguards; a slot invalidated by
`max_slot_wal_keep_size` requires recovery/rebuild rather than ordinary retry.

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
- [ADR-005: Async apply progress and retained-WAL health](../decisions/005-async-apply-progress-and-health.md)
- [DML table](dml-table.md)
- [Manage table](manage-table.md)
- [Flushing table](flushing-table.md)
