# ADR-005: Async Apply Progress and Retained-WAL Health

## Status

Accepted; amends [ADR-003](003-optional-async-mirror-capture.md)

## Date

2026-07-20

## Context

ADR-003 moved mirror writes out of async foreground transactions, but its first
implementation still had three production concerns:

- per-statement worker-kick triggers added foreground work even though the
  database applier is designed to stay running;
- applying every UPDATE through `INSERT ... ON CONFLICT` made mirror catch-up
  much slower than source UPDATE throughput; and
- rejecting apply after retained WAL crossed a configured byte limit stopped
  the only component able to reduce that retention.

The mirror must remain replay-safe after crashes and correct when a concurrent
flush has already pruned a key. Catch-up must also remain memory-bounded and
yield CPU to foreground backends and scheduled flush work. Performance changes
must not remove the PK, `seq`, or partial tombstone indexes that make apply and
flush scale.

## Decision

Async tables do not install worker-kick DML triggers. Activation starts one
database-scoped applier; the shared-preload launcher, explicit ensure paths, and
the consistency fence restore it after failure or restart. Legacy kick triggers
are removed during async activation and teardown.

The decoder continues to create bounded batches of at most 8,192 compatible,
unique keys using typed `unnest` arrays. INSERT and DELETE use conflict-safe
upserts. UPDATE uses one data-modifying CTE:

1. update existing mirror keys with `UPDATE ... FROM incoming` and return them;
2. insert only incoming keys absent from the returned set; and
3. retain `ON CONFLICT DO UPDATE` on that fallback for concurrent/replay safety.

Separate upsert and UPDATE SQL plans are cached per relation. Pgoutput relation
metadata changes invalidate both plans and cached PK types.

When a configured apply budget ends with work still pending, the database
worker performs at most four immediate additional ticks. The fifth pending
result waits through the latch before another burst. Errors reset this budget
and retain the existing bounded exponential backoff.

`koldstore.async_mirror_max_retained_bytes` is health telemetry only. Crossing
it marks `async_mirror_status().retention.ok` and overall `healthy` false, but
never rejects apply or a fence. The older `admission` JSON object remains an
additive compatibility alias. PostgreSQL disk monitoring,
`max_slot_wal_keep_size`, and slot-loss recovery are independent operational
safeguards.

Keep all mirror indexes:

- primary key for direct UPDATE joins and conflict fallback;
- `seq` for ordered flush selection; and
- partial tombstone `seq` for delete-only flush work.

Async small-statement UPDATE uses a 1.10× heap p95 acceptance gate. Strict mode
uses a separate 2.00× gate because it provides transactional mirror visibility.
Worker-on sustainable throughput, peak backlog, and drain time are separate
release metrics from foreground latency.

## Alternatives Considered

### Keep unified UPDATE upsert

- Pros: one SQL shape for every operation.
- Cons: pays conflict arbitration for every normal existing mirror row and was
  the dominant UPDATE catch-up bottleneck.
- Rejected in favor of direct UPDATE with a missing-row fallback.

### Use direct UPDATE without fallback

- Pros: smallest and fastest statement.
- Cons: a flush-pruned or otherwise missing mirror row would remain absent,
  allowing hot/cold reads or later flush selection to miss the latest state.
- Rejected because performance cannot weaken visibility correctness.

### Retry pending work without yielding

- Pros: maximum catch-up aggressiveness.
- Cons: a sustained backlog can monopolize a worker CPU and delay foreground
  backends or flush scheduling.
- Rejected in favor of bounded immediate bursts.

### Stop apply at the retained-WAL threshold

- Pros: presents a hard extension-level error at a configured byte count.
- Cons: creates positive feedback—retention rises while the consumer is
  prevented from draining—and does not cap PostgreSQL disk usage safely.
- Rejected in favor of health telemetry plus independent PostgreSQL safeguards.

### Remove mirror indexes to reduce write amplification

- Pros: cheaper individual mirror writes.
- Cons: degrades key updates, ordered flush selection, or tombstone flushes and
  moves cost into less bounded paths.
- Rejected without workload-specific evidence covering the entire lifecycle.

## Consequences

Positive:

- async foreground UPDATE can remain near the heap path because application
  transactions do not write or kick the mirror;
- existing-row UPDATE catch-up avoids unnecessary conflict handling;
- flush-pruned rows, deletes, and crash replay remain conflict-safe;
- a high retained-WAL alert cannot disable its own recovery path; and
- scheduling remains bounded in memory and fair under persistent backlog.

Trade-offs:

- the UPDATE planner is operation-specific and must maintain two cached SQL
  plans per relation;
- retained-WAL health requires active operator alerting and PostgreSQL capacity
  policy rather than relying on an extension error as a disk cap;
- async foreground parity does not prove sustainable apply capacity, so release
  results must report worker-on backlog and drain behavior separately; and
- strict mode remains slower by design when immediate mirror visibility is
  required.

## References

- [Async mirror capture](../architecture/mirror-capture-async.md)
- [Mirror capture modes](../architecture/mirror-capture-modes.md)
- [DML workflow](../architecture/dml-table.md)
- [Performance criteria](../performance.md)
- [Benchmark methodology](../benchmarks/README.md)
