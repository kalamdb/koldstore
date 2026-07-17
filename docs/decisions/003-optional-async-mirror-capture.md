# ADR-003: Optional WAL-Backed Async Mirror Capture

## Status

Accepted

## Date

2026-07-15

## Context

Every managed heap mutation must eventually update the latest-state `__cl`
mirror so flush and hot/cold merge can select the winning key state. The strict
statement-trigger implementation gives atomic read-your-writes behavior, but it
also puts mirror index probes, sequence generation, and mirror WAL on the
foreground DML path. On the previous 10M-row benchmark, managed INSERT was 35%
slower than the plain heap, above the product acceptance target of at most 10%.

PostgreSQL cannot transparently run two data-modifying branches of the same
transaction in parallel. Moving the mirror write to an independent transaction
also cannot preserve strict read-your-writes semantics. A separate consistency
model is therefore required to move this cost out of foreground latency.

Requirements:

- keep strict behavior as a zero-configuration default;
- never mirror aborted source transactions;
- avoid a capture gap when activating a populated table;
- bound decoder memory and allocation by batch size;
- never acknowledge source WAL before the mirror effect is durable;
- keep flush strongly consistent with all already-committed source DML; and
- prevent internal flush deletes from being captured as user deletes.

## Decision

Add `mirror_capture_mode => 'strict' | 'async'` to `manage_table`, with `strict`
as the default and backward-compatible interpretation for missing catalog data.

Async mode uses one empty `koldstore_async_mirror` publication and one
deterministically named pgoutput logical slot per database. `manage_table`
initializes with strict triggers, completes any backfill, then transactionally
adds only the source primary-key columns to the publication and drops the three
DML capture triggers. The PK mutation guard remains installed.

`CREATE EXTENSION` creates the empty publication. The first async
`manage_table` creates or reuses the deterministic slot in an autonomous
one-shot worker before table/catalog writes. This is required because
PostgreSQL forbids creating a logical slot in a transaction that has performed
writes. `wal_level=logical` and its server restart remain the only manual
administrator prerequisite.

Expose `koldstore.wait_for_async_mirror()` as an explicit strong-consistency
fence. It peeks committed pgoutput v1 changes, parses them in Rust, and applies
8,192-row set-based mirror batches. Mirror writes and
`koldstore.async_mirror_state.applied_lsn` commit together. The next fence
advances the slot to that durable checkpoint before decoding more WAL.
`flush_table` invokes the applier before flush selection. Its internal source
cleanup temporarily uses PostgreSQL's `DoNotReplicateId` replication origin.

One dynamic worker per database polls every 100 ms, avoids reopening logical
decoding at an unchanged WAL position, and applies committed WAL when it moves.
An async-only statement trigger ensures the worker exists without performing
mirror work in the source transaction, including after PostgreSQL restart.
Strong reads retain the explicit fence. Logical apply and manual fences share a
database advisory lock so only one consumer touches the slot at a time.

## Alternatives Considered

### Keep strict capture only

- Pros: one consistency model, no slot operations, immediate mirror state.
- Cons: cannot meet the foreground INSERT target on the measured workload.
- Rejected as the only mode; retained as the default mode.

### Rewrite strict triggers or the trigger body in C/Rust

- Pros: less PL/pgSQL overhead and allocation while preserving strict semantics.
- Cons: profiling and the trigger rewrite showed that the remaining foreground
  cost is principally the second table/index/WAL write. A language change does
  not remove that work or make PostgreSQL run it in parallel.
- Rejected as the primary path to the 10% INSERT target.

### Full-row logical publication

- Pros: the applier receives a complete row image without consulting the heap.
- Cons: the mirror only needs keys and operation metadata. A wide-row experiment
  generated about 7.3 GiB of logical-decoding temporary data at the comparable
  insert phase, versus at most 449 MiB with PK-only publication.
- Rejected in favor of primary-key-only publication.

### Advance the logical slot before committing mirror changes

- Pros: simpler one-call consumption flow.
- Cons: a crash after advance and before mirror commit permanently loses the
  source effect.
- Rejected; durable state precedes acknowledgement.

### Caller-only or externally scheduled catch-up

- Pros: no persistent worker lifecycle.
- Cons: mirror lag and WAL retention depend on application discipline; an easy
  setup should become useful without a separate scheduler.
- Rejected in favor of a bounded-lag database worker while retaining the fence.

## Consequences

Positive:

- The final 10M-row benchmark measured async managed INSERT at 98,454 ops/s
  versus 98,978 ops/s for PostgreSQL, 1% slower and within the 10% target.
- Foreground DML no longer performs the mirror heap/index write.
- Only committed transactions reach the mirror, and apply allocations remain
  bounded by a fixed batch.
- Publication, slot, and low-lag apply setup are automatic and idempotent.
- Strict mode and existing serialized options retain their old semantics.

Trade-offs:

- Async mirror state normally trails commits by approximately one worker poll;
  callers must still fence before a strong mirror/cold-overlay read. Flush
  fences automatically.
- Catch-up is real deferred work and must be reported separately. In the same
  run, INSERT catch-up was 28,881 ops/s and UPDATE catch-up was 1,170 ops/s,
  making UPDATE apply the clearest remaining optimization target.
- Operators must enable logical decoding and monitor retained WAL. KoldStore
  creates the publication/slot and schedules ordinary catch-up.
- `koldstore.disable_async_mirror()` removes the database slot, publication,
  and checkpoint after all async tables are unmanaged.
- `TRUNCATE` remains unsupported for async managed tables.
- Changing capture mode after a table is managed is not yet a public operation.

## References

- [Mirror capture modes](../architecture/mirror-capture-modes.md)
- [DML workflow](../architecture/dml-table.md)
- [10M-row benchmark](../benchmarks/README.md)
- [Case: async flush prune race](../cases/async-flush-prune-race.md) (proposed
  pre-prune fence; selection fence alone is not sufficient)
