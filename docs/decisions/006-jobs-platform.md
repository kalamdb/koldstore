# ADR-006: Minimal jobs control plane on `koldstore.jobs`

## Status

Proposed

## Date

2026-07-23

## Context

Flush and migrate already write rows into `koldstore.jobs`, but execution is
inline and mostly single-transaction. Lease columns, claim indexes, and
`max_running_jobs` existed as unused scaffolding. Operators cannot see live
progress, cancel a running flush, or reliably stop and clean up when dropping
a table.

We need a **small** control plane — not a general-purpose job runner to
maintain.

## Decision

1. Keep **one** catalog table: `koldstore.jobs`. Do not add a second queue or
   external broker.
2. **v1 is minimal:** improve the current inline flush/migrate executors.
   Do **not** introduce `koldstore-jobs`, a `JobHandler` framework, or a
   multi-claimer worker loop until there is a clear second need (deferred
   Phase E — https://github.com/kalamdb/koldstore/issues/53).
3. **Progress (accepted):** write typed `phase` and `progress_*` on the job row
   between waves. Keep the single `flush_table` API. **Do not** add mid-flush
   COMMIT / PROCEDURE / autonomous-writer complexity for live peer visibility
   unless a clear product need appears (deferred B.1 —
   https://github.com/kalamdb/koldstore/issues/52). Peers see progress when
   the flush statement commits.
4. **Execution (accepted):** keep synchronous **inline** execution (caller +
   scheduler). Always-enqueue / background-only execution is deferred.
5. Treat **pending cold segments + manifest CAS** as the flush durability
   boundary (ADR-004): activation is the publish point; everything before is
   reclaimable.
6. Wire **cooperative cancel** and **DROP/unmanage** teardown.
7. **Cancel after activate (accepted):** if publish already committed, finish
   required prune then mark **`completed`**, with
   `payload.cancel_requested_after_publish` for audit. Cancel before activate
   remains `cancelled` with pending GC.
8. **DROP TABLE cleanup (accepted):** finish and wire `drop_table_cleanup`.
   On DROP: cancel active jobs, deactivate metadata, enqueue Delete-policy
   cleanup, idempotently remove cold artifacts.
9. **Remove unused claim scaffolding (accepted):** delete `max_running_jobs`
   GUC and unused jobs lease/claim columns (`lease_*`, `last_heartbeat_at`,
   `priority`, `run_after`) plus claim-oriented indexes. No deprecation or
   compatibility shim — extension install SQL may change freely. Keep
   existing error cooldown skip for auto-flush. Do not add a lease claimer
   or auto-retry/`run_after` framework later unless newly justified.
10. Fix doc drift (`recover_segments`, fake claimer ownership) — **done** in Phase D.

## Alternatives Considered

### Full jobs platform (handlers, leases, claim worker)
- Pros: Extensible multi-type runtime.
- Cons: Large maintenance surface; current product only needs progress,
  cancel, and DROP cleanup.
- Rejected for v1; may revisit later.

### Keep single-transaction inline flush forever
- Pros: Simplest.
- Cons: No live progress, no cancel visibility across sessions.
- Rejected for the control-plane goals above; accept mid-job COMMITs of the
  job row only.

### Always-enqueue + background worker only
- Pros: Uniform async model.
- Cons: Harder local/debug UX; more moving parts than needed.
- Rejected for v1.

### External queue (Redis/SQS)
- Rejected — second source of truth.

## Consequences

- Implementation stays in `pg_koldstore` + existing domain crates.
- Progress visibility requires COMMITs between waves; domain safety still
  relies on pending/CAS, not one giant catalog transaction.
- Unused lease/claim scaffolding is deleted from schema and GUCs (no leftover
  knobs).
- Parallel multi-table uploads still blocked by apply-lock scope until a
  later phase.

## Related

- Design: `docs/plans/2026-07-23-jobs-platform-design.md`
- Prior intent (deferred): `docs/plans/crate-architecture-reorg.plan.md`
  (`koldstore-jobs`)
- Segment publish: `docs/decisions/004-segment-publication-protocol.md`
