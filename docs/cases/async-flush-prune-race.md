# Case: Concurrent async DML during flush prune

| | |
| --- | --- |
| Status | Phase-6 prune fence implemented; phase-5.5 pre-lock catch-up, row/WAL caps, and strict seq audit remain deferred |
| Severity | Correctness — possible loss of a newer hot row under async capture |
| Modes | Primary race affects `mirror_capture_mode = 'async'`; sequence-ordering prerequisite also requires a strict-mode audit |
| Related | [flushing-table](../architecture/flushing-table.md), [mirror-capture-modes](../architecture/mirror-capture-modes.md), [ADR-003](../decisions/003-optional-async-mirror-capture.md), [implementation plan](../plans/2026-07-17-async-flush-prune-fence.md) |

---

## Summary

Flush cleanup deletes mirror rows with `seq <= max_seq`, then deletes matching
hot rows by primary key. That is safe when the mirror already reflects every
committed mutation of those keys.

In **strict** mode, heap and mirror mutate in the same transaction, so a
concurrent `UPDATE` changes mirror `seq` before cleanup can see the old
watermark. The updated key falls out of `seq <= max_seq` and stays hot **only if
the new sequence is guaranteed to be greater than the selected watermark**.
That ordering prerequisite is audited separately below.

In **async** mode, heap commits first and the mirror lags until WAL apply. Flush
fences once at start, then may spend a long time writing Parquet. A commit that
lands **during** that window can leave:

- hot = newer values
- mirror = still `seq = S_old <= max_seq`
- cold Parquet = older flushed image

Cleanup then removes `S_old` and deletes the hot row by PK — **dropping the
newer version** while cold still holds the older one.

Pausing the async worker alone does **not** close this hole: user DML still
commits to the heap and WAL while prune runs. The mirror simply stays stale.

---

## Concrete failure timeline

Assume 100k hot rows, `pk = 5` currently at mirror `seq = S_old`.

```text
T0  flush_table: apply_available()          -- fence #1
T1  resolve stats: max_seq includes S_old
T2  stream Parquet for seq <= max_seq       -- may take seconds/minutes
T3  user COMMIT UPDATE pk=5                 -- hot new; WAL pending; mirror still S_old
T4  write manifest (cold becomes visible)
T5  prune: DELETE mirror WHERE seq <= max_seq
       → removes pk=5 at S_old
    DELETE hot USING removed_mirror
       → deletes newer hot pk=5
T6  result: cold has old image, hot gone, mirror gone
    merge scan returns stale cold values (or missing overlay)
```

Strict mode cannot reach T3 with mirror still at `S_old`: the capture trigger
changes `seq` in the same transaction as the heap update. It is safe from the
same `seq <= max_seq` cleanup only when that new sequence is also guaranteed to
sort above the watermark; see [Sequence ordering prerequisite](#sequence-ordering-prerequisite).

---

## Why the current fence is insufficient

| Phase | Today | Gap |
|-------|--------|-----|
| Before selection | `apply_available()` | Covers commits **before** flush starts |
| During Parquet write | No fence; DML allowed | New commits may lag in the slot |
| Before prune | No fence | Cleanup trusts mirror `seq` that may be stale |
| During prune | Phase-0 apply lock is still held | Worker is serialized, but application DML can leave the mirror stale |

ADR-003 requires flush to be strongly consistent with already-committed source
DML. The start fence satisfies that for **selection**. It does not satisfy it
for **prune** after a long upload.

---

## Design goal

Before `prune_flushed_hot_rows`:

1. Every source commit that can affect keys in the prune watermark must already
   be reflected in the mirror (newer `seq` / tombstone as appropriate).
2. No new writer can commit a mutation to the flushed table between that
   catch-up and the prune DELETE.
3. The async worker must not apply into the same mirror rows concurrently with
   prune. The existing transaction-scoped apply advisory lock already provides
   this serialization once phase 0 starts.
4. Strict mode and databases without an async slot stay cheap no-ops for the
   bounded WAL passes and final source-table writer lock.
5. Parquet upload remains concurrent with DML — only the short finalize/prune
   critical section is serialized.

---

## Recommended implementation: short writer fence + bounded WAL apply + prune

The implementation must use a **bounded WAL boundary**, not “drain until idle.”
The database has one logical slot shared by every async table, so unrelated
tables can continue producing WAL even after writers on the table being flushed
are blocked. An explicit upper LSN makes the amount of decoding finite and gives
the fence a precise correctness meaning.

The expensive Parquet/object-store phase stays concurrent with application DML.
Only final WAL catch-up and hot/mirror cleanup run while source-table writers are
blocked.

### Existing transaction behavior that the implementation must preserve

The current phase-0 call to `apply_available()` takes `lock_apply`, implemented
with `pg_advisory_xact_lock`. It therefore already holds the database apply lock
from the beginning of `flush_table` until the caller transaction ends. The
background applier **cannot** run during Parquet upload today.

Consequences:

- finalize must not pretend it is acquiring the apply lock for the first time;
- there is no worker/apply race during prune once phase 0 has run;
- WAL can accumulate for all async tables during a long upload;
- a second call to the current `apply_available()` is unsafe because it can
  acknowledge a checkpoint written by the same still-uncommitted flush
  transaction;
- the implementation needs a lower-level bounded apply pass with explicit
  acknowledgement and replay-skip behavior.

Do not replace the transaction lock with a session lock as part of this fix.
That is a separate lifecycle change with different error/unlock behavior.

### Typed boundaries

Use explicit domain objects rather than passing interchangeable raw `u64`/string
LSNs through the flush path:

```text
WalFenceLsn             -- WAL is known durable through this upper boundary
AppliedWalBoundary      -- last source transaction end-LSN applied in this txn
DurableAppliedLsn       -- checkpoint read from a previously committed txn
PruneSeqFloor           -- target-table mirror seq must be greater than max_seq
```

Suggested result and request shapes (names are illustrative):

```text
BoundedApplyRequest {
  upper_bound: WalFenceLsn,
  skip_through: Option<AppliedWalBoundary>,
  acknowledge_durable_checkpoint: bool,
  target_prune_floor: Option<(TableOid, PruneSeqFloor)>,
}

BoundedApplyOutcome {
  row_changes: i64,
  last_applied: Option<AppliedWalBoundary>,
}
```

`skip_through` applies at a **pgoutput transaction** boundary, not per row. A
single source transaction can change the same PK several times; skipping only
some messages from that transaction can choose the wrong final operation.

For an empty pass, the effective applied boundary remains the request's
`skip_through`, or the durable checkpoint when phase 0 has no skip value. Never
promote the applied boundary to `upper_bound` merely because decoding returned
zero rows: the fence LSN is a read limit, not proof that the slot emitted or
applied a transaction ending there.

### Sequence ordering prerequisite

The prune proof requires every mutation of the target table applied after
selection to receive `new_seq > max_seq`. Uniqueness alone is insufficient.

The current Snowflake layout includes timestamp, backend worker ID, and a
per-process sequence. Calls are monotonic for one generator, but two PostgreSQL
backends generating in the same millisecond are not automatically ordered by
commit time: a later call from a lower worker ID can be numerically below an
earlier value from a higher worker ID. Clock rollback/backend reuse also needs
an explicit contract. Tests using one `pg_test` worker do not prove the required
database-wide ordering.

For this async fix, pass `PruneSeqFloor(max_seq)` to the phase-5.5 and phase-6
apply requests for the target table. Its allocator must:

- generate every post-selection target-table sequence strictly above the floor;
- generate a distinct increasing value for each subsequent target change in the
  flush backend;
- preserve the existing `SeqId`/Snowflake representation and fail on overflow;
- leave changes for unrelated async tables on their normal sequence path.

Do not implement this as unchecked `max_seq + row_number`. Add a typed,
overflow-checked allocator such as `next_id_after(worker_id, PruneSeqFloor)` or
an equivalent generator-floor operation.

Strict capture occurs in application backends and relies on the same
`new_seq > max_seq` assumption. Audit it with concurrent-backend and same-
millisecond tests. If database-wide ordering is not already guaranteed, fix
strict allocation or add the appropriate writer fence; do not continue claiming
strict safety solely because its trigger writes synchronously.

### Phase 0: bounded selection fence

Before resolving the flush watermark:

1. Read the durable `applied_lsn` checkpoint from
   `koldstore.async_mirror_state`.
2. Acknowledge that checkpoint on the slot. This checkpoint came from an earlier
   committed transaction, so acknowledging it preserves ADR-003 crash safety.
3. Capture a WAL upper boundary `F0` using the current WAL insert position.
4. Force or wait for WAL durability through `F0`. Logical decoding only exposes
   WAL that is safely flushed; this is required when an application uses
   `synchronous_commit = off`.
5. Peek/apply with `upto_lsn = F0` and remember the exact last decoded source
   transaction end-LSN as `L0`; if the pass is empty, `L0` remains the durable
   checkpoint from step 1.
6. Record exactly `L0` as the transaction’s pending applied checkpoint.

Do **not** store:

```sql
GREATEST(decoded_end_lsn, pg_current_wal_insert_lsn())
```

A different async table can commit after decoding reaches its boundary but
before that expression is evaluated. Recording the global insert LSN would
claim that an undecoded transaction was applied, and a later slot advance could
discard it. The checkpoint must be the exact last transaction end-LSN emitted by
the decoder.

### Phase 1–5: selection and Parquet write

Resolve the stable eligible prefix and stream it to Parquet as today. DML remains
allowed. Any source mutation committed after `F0` may leave the mirror behind
during this phase; the finalize fence handles it.

The selected prefix must also obey the total flush-size limit described in
[Large cleanup performance](#large-cleanup-performance). `max_rows_per_file`
controls Parquet file size and is **not** a bound on total rows deleted by one
flush.

### Phase 5.5: pre-lock bounded catch-up

After the expensive Parquet/object upload, run another bounded apply pass
**before** taking the source relation lock:

1. Capture a WAL upper boundary `Fp` and force/wait for durability through it.
2. Decode with `upto_lsn = Fp`, skip complete replayed transactions through
   `L0`, and apply the remainder without acknowledging the current
   transaction's pending checkpoint. Allocate sequences for mutations of the
   flush target strictly above `PruneSeqFloor(max_seq)`.
3. Retain the exact last applied transaction end-LSN as `Lp`; if the pass is
   empty, `Lp = L0`.

This pass is not needed for correctness; phase 6 is the correctness fence. It
is required for predictable lock duration with the current architecture. The
logical slot is shared by all async tables, and the phase-0 transaction advisory
lock prevents the background applier from draining it during upload. Without a
pre-lock pass, minutes of WAL from unrelated async tables could be decoded while
writers on the target table are blocked.

Prefer running this pass before manifest/catalog publication so a catch-up
failure does not leave a newly published segment awaiting cleanup. If existing
publication ordering requires it to run afterward, use the same retry and
reconciliation rules as other post-publish failures.

Under sustained WAL load, use a **finite** pre-lock catch-up budget rather than
looping until idle. A practical policy is:

- run one pass unconditionally after upload;
- optionally run a small configured maximum number of additional bounded passes
  while the estimated remaining LSN distance exceeds a lock-window target;
- if apply throughput cannot approach WAL production, fail before taking the
  relation lock and retry/back off instead of creating a long writer convoy.

The LSN-distance threshold is an admission/latency control, not a correctness
claim: transaction density per WAL byte varies. Record decoded transactions,
row messages, elapsed time, and LSN distance so the policy can be tuned from
measurements.

### Phase 6: final writer fence and bounded delta apply

Immediately after manifest publish/catalog upsert and before
`prune_flushed_hot_rows`:

```text
if table.capture_mode != async:
  prune as today
else:
  acquire source relation SHARE ROW EXCLUSIVE by table OID
  capture WAL insert boundary F1
  force/wait for WAL durability through F1
  apply pgoutput transactions where Lp < transaction_end_lsn <= F1
    - do not acknowledge the pending checkpoint from this flush transaction
    - decode with upto_lsn = F1
    - skip complete transactions through Lp when the slot replays them
    - allocate target-table sequences strictly above max_seq
  prune_flushed_hot_rows(max_seq)
  update counters and finish the job
commit promptly; transaction end releases both locks
```

The pre-lock and final passes start decoding from the slot's last durably
confirmed position, so they may receive transactions already applied earlier in
the flush. They must parse enough protocol state to identify transaction
boundaries, but must not reapply transactions through the most recent local
boundary (`L0`, then `Lp`). Reapplying them would allocate fresh snowflake
sequences, move otherwise eligible mirror rows above `max_seq`, and prevent their
cleanup.

One final bounded pass is sufficient for correctness:

- acquisition of the table lock waits for source transactions that already hold
  `ROW EXCLUSIVE` to commit or roll back;
- after acquisition, new source DML cannot start;
- durability through `F1` makes every earlier commit decodable;
- `upto_lsn = F1` prevents other async tables from extending the pass forever;
- the phase-5.5 pass moves most accumulated decoding outside the relation-lock
  window, leaving only the lock-wait delta for phase 6 in the normal case.

Do not loop until `row_changes == 0`. A zero row count is not a durable WAL
boundary, especially with `synchronous_commit = off`, and repeated calls risk
acknowledging uncommitted mirror effects.

```mermaid
sequenceDiagram
  participant App as Application DML
  participant Flush as flush_table
  participant Heap as Hot heap
  participant Slot as Logical slot
  participant Mirror as __cl mirror

  Flush->>Slot: durable bounded apply through F0
  Note over Flush: select + write Parquet (writers allowed)
  App->>Heap: UPDATE pk=5 COMMIT
  Heap->>Slot: pgoutput PK change
  Flush->>Slot: pre-lock bounded apply through Fp
  Flush->>Heap: SHARE ROW EXCLUSIVE
  Note over App,Heap: in-flight writers finish; new writers wait
  Flush->>Slot: capture + flush F1
  Flush->>Slot: apply final delta Lp < end_lsn <= F1
  Flush->>Mirror: seq bump S_old → S_new
  Flush->>Mirror: DELETE WHERE seq <= max_seq
  Note over Mirror: pk=5 at S_new is excluded
  Flush->>Heap: delete hot rows returned by mirror delete
  Note over Heap: pk=5 remains hot
```

### Why `SHARE ROW EXCLUSIVE`

PostgreSQL source DML (`INSERT`, `UPDATE`, `DELETE`, `MERGE`) takes
`ROW EXCLUSIVE`. `SHARE ROW EXCLUSIVE`:

- waits for in-flight DML transactions to finish;
- blocks new writers for the bounded final delta apply + cleanup window;
- allows ordinary `SELECT` (`ACCESS SHARE`);
- is self-exclusive and avoids a later `SHARE` → `ROW EXCLUSIVE` lock upgrade
  when cleanup deletes from the source table.

`SHARE` also conflicts with writers, but PostgreSQL recommends
`SHARE ROW EXCLUSIVE` when the locking transaction will modify the table. Two
transactions that both take `SHARE` and later need `ROW EXCLUSIVE` can deadlock.
`ACCESS EXCLUSIVE` is unnecessarily strong because it blocks ordinary reads.

Acquire the relation lock by OID (`table_oid`) rather than rebuilding an
untrusted qualified SQL name. If managed inheritance/partition descendants are
unsupported, validate that contract and lock `ONLY` the same physical relation
that cleanup targets. If descendants are supported later, lock every relation
on which application DML can produce published changes.

### Lock scope and invocation contract

PostgreSQL relation and transaction advisory locks are released at transaction
end, not when `flush_table()` returns. Therefore the “short critical section”
promise holds only when flush executes in an autocommit or worker-owned
transaction.

Implementation requirement:

- prefer worker-owned flush transactions; or
- reject/document `flush_table()` inside a caller-controlled multi-statement
  transaction.

Otherwise this is possible:

```sql
BEGIN;
SELECT koldstore.flush_table('app.events'); -- obtains writer fence
-- application remains idle or performs unrelated work
COMMIT;                                    -- writers unblock only here
```

Use a configurable local lock timeout. A timed-out fence must fail before prune,
leave hot rows authoritative, and make the job retryable. Do not wait forever
behind an idle transaction, prepared transaction, row locker, or wraparound
autovacuum.

### Strict mode / no async table

Gate on the **managed table’s capture mode**, not merely the presence of a
database async slot:

- strict table → no WAL fence and no writer-blocking prune lock;
- async table with compatible slot → phase-0 selection apply, phase-5.5
  pre-lock catch-up, and phase-6 writer fence;
- async table with missing/incompatible slot → fail closed before selection;
- strict table in a database that has some other async table → stay on the
  strict fast path.

This requires resolving minimal managed-table capture metadata before calling
the database-scoped applier.

### Placement in code

Primary orchestration changes:

- `crates/pg_koldstore/src/sql/flush/execute.rs`
  - resolve capture mode before phase 0;
  - retain `L0`/`Lp` in the prepared/execution context;
  - run pre-lock catch-up after object upload, preferably before publication;
  - call the final fence after `after_manifest_publish` and before
    `before_hot_cleanup` / `prune_flushed_hot_rows`;
- `crates/pg_koldstore/src/async_mirror/apply.rs`
  - split durable acknowledgement from bounded peek/apply;
  - accept `upto_lsn` instead of always passing `NULL`;
  - skip whole replayed transactions through the latest local applied boundary;
  - return the exact last decoded transaction end-LSN;
  - remove the `GREATEST(..., pg_current_wal_insert_lsn())` checkpoint;
- `crates/koldstore-common/src/domain/snowflake.rs` and the SQL wrapper
  - add an overflow-checked floor-aware allocation API;
  - audit cross-worker ordering assumptions and tests;
- `crates/pg_koldstore/src/async_mirror/lifecycle.rs`
  - keep the existing transaction-scoped apply serialization;
- `crates/pg_koldstore/src/sql/flush/spi.rs`
  - acquire the source relation lock by OID and run the existing atomic cleanup.

Keep the cleanup predicate based on the stable sequence prefix. The correctness
fix is the visibility/fencing protocol, not a broader delete predicate.

## Large cleanup performance

The fence makes deletion correct, but lock duration is proportional to work done
after the fence. A flush that selects millions of rows can spend substantial time
deleting heap tuples, updating every source index, writing WAL, materializing
`RETURNING` keys, and creating dead tuples for later vacuum. The design must
bound this work rather than assuming cleanup is always short.

### Current cleanup strengths

`plan_seq_range_cleanup` already has the correct basic shape:

- mirror selection uses the existing B-tree index on `mirror.seq`;
- tombstone-only force flushes can use the partial `(seq) WHERE op = 3` index;
- hot deletes join on the source primary key;
- only PK, `seq`, and `op` are returned from deleted mirror rows;
- no per-row JSON/`jsonb_to_recordset` is built;
- mirror and hot deletion happen in one data-modifying CTE statement, so a
  statement failure cannot commit one side without the other.

Do not add a wide covering `(seq, every_pk, op)` index without measurement.
`DELETE` must visit and modify heap tuples anyway, while a wider index increases
foreground mirror-write cost and future cleanup/vacuum work.

### Required v1 bound: cap total rows per flush

Add a total `max_rows_per_flush` policy/setting distinct from
`max_rows_per_file`:

```text
eligible_rows = policy-selected oldest seq prefix
flush_rows    = min(eligible_rows, max_rows_per_flush)
max_seq       = max seq of that bounded prefix
```

Requirements:

- the cap applies to policy and force flushes;
- one `flush_table` execution publishes and prunes at most one bounded prefix;
- if more eligible rows remain, the scheduler queues/runs another flush in a
  new transaction, releasing the writer lock between flushes;
- `max_rows_per_file` continues to control Parquet object sizing only;
- the shipped default must be conservative and selected from the benchmark
  matrix below, not guessed from development hardware;
- operators can lower the cap for latency-sensitive tables and raise it when
  throughput matters more than brief write pauses.

The cap and watermark must describe the **same complete seq prefix**. Because
cleanup uses `seq <= max_seq`, it must never select only part of a set of rows
that share the boundary sequence and then delete all of them. V1 should encode
and test sequence uniqueness. If that cannot be guaranteed, redesign selection
and cleanup around a stable composite watermark such as `(seq, primary_key)`;
do not silently exceed the cap with an arbitrarily large tie group.

This is the simplest way to bound writer blocking while retaining the existing
single-statement, atomic mirror+hot cleanup. It also bounds the data-modifying
CTE’s `RETURNING` tuplestore for wide/composite primary keys.

The row cap bounds cleanup work, but it does **not** by itself bound WAL apply
work. Use the phase-5.5 pre-lock catch-up to process accumulated global-slot WAL
while target-table writers are still allowed. Before taking the relation lock,
enforce a configurable admission target such as `max_prune_fence_wal_bytes` (or
an equivalent decoded-work/time estimate). If the remaining final delta is too
large, run another finite pre-lock pass or fail/retry; do not knowingly enter an
unbounded writer-pause window.

After the relation lock is granted, capture `F1` immediately. If the gap grew
past the configured hard safety limit while waiting for an in-flight writer,
abort the transaction before prune so PostgreSQL releases the lock. This is a
latency guard, not a substitute for applying every transaction through `F1`.

### Long upload and retained-WAL limit

`max_rows_per_flush` bounds row work, but object-store latency can still make the
transaction long. The phase-5.5 pass reduces the **writer-lock** window; it does
not advance the slot's durable `confirmed_flush_lsn`, because its mirror effects
are still part of the uncommitted flush transaction. Consequently, a long or
stalled upload can:

- retain substantial `pg_wal` from the slot's `restart_lsn`;
- approach `max_slot_wal_keep_size` and invalidate the slot;
- hold an old transaction snapshot that delays vacuum cleanup;
- hold the database-wide apply advisory lock and delay freshness for every async
  table.

V1 must therefore combine the row cap with an object-store/overall flush
timeout, observe slot retained bytes plus `wal_status`/`safe_wal_size`, and fail
closed if the slot becomes lost or invalid. An aborted upload must clean up or
later garbage-collect unreferenced objects.

If production cannot place a reliable upper bound on upload time, the durable
design is a larger worker-owned, multi-transaction workflow: commit the
selection apply/checkpoint, persist an immutable flush intent and watermark,
upload outside that transaction while the normal applier continues, then run a
short revalidated finalize transaction with the writer fence. That requires
durable intent state, segment idempotency, snapshot/schema validation, and
orphan cleanup; it is not safe to obtain merely by releasing the current
advisory lock mid-transaction.

### Why not split one cleanup into many statements in the same transaction

Running repeated `DELETE ... LIMIT batch` statements while holding the same
transaction lock does **not** let writers proceed: relation and row locks remain
held until transaction end. It adds planning/execution overhead and creates
partial-progress accounting problems if a later statement fails and the flush
error is caught.

Small internal statement batches may later be useful to bound memory or improve
interruptibility, but they are not a writer-latency optimization unless each
batch commits independently.

### Future option: independently committed cleanup batches

If benchmarks show that even a bounded flush prefix cannot meet the writer-pause
target, move post-publish cleanup into worker-owned transactions:

```text
publish cold prefix and durable prune_max_seq once
loop while mirror rows remain at seq <= prune_max_seq:
  begin worker transaction
  acquire writer fence
  bounded WAL apply through a durable F_batch
  atomically delete at most cleanup_batch_rows mirror + hot rows
  update actual mirror/hot counter deltas and cleanup progress
  commit and release writer fence
```

Writers may commit between batches. The next batch repeats the fence; keys
changed between batches receive `seq > prune_max_seq` and are skipped. This is
correct but requires durable `cleanup_pending` progress, per-batch idempotent
counters, retry semantics, and a clear point at which `cold_rows_added` is
applied exactly once. It is intentionally not the first implementation.

### Delete-plan verification and benchmarks

Before choosing the default `max_rows_per_flush`, measure on PostgreSQL 15–18:

| Dimension | Values |
| --- | --- |
| Rows pruned | 1k, 10k, 50k, 100k, 1M |
| PK | narrow scalar, UUID, wide composite |
| Source indexes | PK only, PK + 3 secondary, PK + expensive extension index |
| Mirror mix | live rows, tombstone-only, mixed |
| Hot table size | mostly pruned, small prefix, large retained tail |
| WAL mode | `synchronous_commit = on` and `off` |
| Contention | idle, concurrent writers, long row locker, autovacuum |

Capture:

- relation-lock wait and hold duration;
- pre-lock and under-lock bounded WAL catch-up time, LSN distance,
  transactions, and rows decoded;
- mirror delete time, hot delete time, rows/second;
- `EXPLAIN (ANALYZE, BUFFERS, WAL)` for non-production fixtures;
- WAL bytes, temporary bytes, and peak backend memory;
- dead tuples and vacuum duration after cleanup;
- p50/p95 application writer wait during finalize.

The expected plan is an index-backed mirror seq-prefix scan plus a PK-backed or
planner-selected join into the hot heap. Accept a sequential scan when the
bounded prefix is most of a small table; do not force an index plan globally.

Large `DELETE`s create dead tuples rather than immediately shrinking relation
files. Keep autovacuum enabled in production, monitor dead tuples on both source
and mirror, and tune per-table autovacuum thresholds only from measurements.
Do not run `VACUUM FULL` automatically: it takes `ACCESS EXCLUSIVE` and is not a
routine cleanup mechanism.

---

## Alternatives considered

### A. Second `apply_available()` before prune, no table lock

- Pros: tiny change.
- Cons: TOCTOU — a commit can land between “drain returned 0” and DELETE.
  On a heavy-write system, “drain until idle” without blocking writers may
  **never finish**. The current function may also acknowledge a checkpoint
  written by the same uncommitted flush transaction, making rollback unsafe.
- Rejected as insufficient.

### B. Pause worker only (flag / skip poll), no table lock

- Pros: stops concurrent apply.
- Cons: does not stop heap commits; mirror still lags; same wrong delete.
- Rejected as the sole fix. Still useful **as a side effect** of holding
  `lock_apply` during bounded apply + prune.

### C. Change cleanup to delete exact `(pk, seq)` from the Parquet selection set

- Pros: does not delete a newer mirror seq.
- Cons: if async lag left mirror at `S_old`, cleanup would delete the only
  mirror row while hot already has a newer image → broken “one mirror row per
  PK” invariant and worse repair story.
- Rejected; catch-up must happen **before** removing `S_old`.

### D. Add `seq` to the hot heap and conditional-delete on seq match

- Pros: per-row optimistic delete.
- Cons: reintroduces system columns on user tables (clean-schema regression).
- Rejected.

### E. `ACCESS EXCLUSIVE` for the whole flush (including Parquet upload)

- Pros: trivial correctness.
- Cons: blocks writers for the entire cold write; defeats async’s latency goals.
- Rejected; lock only the short prune fence.

### F. Skip hot delete when mirror apply lag is detected

- Pros: fail-safe.
- Cons: leaves hot/mirror rows that should have been pruned; counter drift;
  requires a reliable lag signal and retry path.
- Possible as a belt-and-suspenders guard later, not the primary design.

### G. Flush-aware sync mirror bump on async UPDATE/DELETE

**Idea:** While async mode stays the default path, if a flush is in-flight for
the table, UPDATE/DELETE synchronously bump the existing mirror row’s `seq`
(strict-like, but only when the PK is already in the mirror). Prune’s
`seq <= max_seq` then naturally skips those keys.

#### What this gets right

For the common hole — commit during Parquet upload, prune later — it works:

```text
flush selects max_seq including S_old
user UPDATE pk=5 while flush running
  → trigger: UPDATE __cl SET seq=S_new WHERE pk=5   (S_new > max_seq)
prune DELETE __cl WHERE seq <= max_seq
  → pk=5 not removed; hot kept
```

That matches the strict invariant for keys that were already mirrored, without
blocking writers for the whole upload.

WAL apply later will bump `seq` again (second snowflake). That is fine: apply is
already latest-state and replay-safe; a higher `seq` still sits above `max_seq`.

#### Can it be built?

Yes — moderate engineering, not a fundamental impossibility.

Sketch:

1. Keep (or reinstall) thin statement-level AFTER UPDATE/DELETE triggers on async
   tables (today async drops full capture triggers and keeps only the PK guard).
2. Trigger body roughly:
   ```sql
   IF EXISTS (
     SELECT 1 FROM koldstore.jobs
     WHERE table_oid = TG_RELID
       AND job_type = 'flush'
       AND status = 'running'
   ) THEN
     UPDATE mirror SET seq = snowflake_id(), op = … FROM transition_rows …
     -- only touches rows that already exist in mirror
   END IF;
   ```
3. Leave INSERT on the pure async WAL path (new keys are outside the prune
   watermark anyway).
4. Leave the background applier as-is (double apply OK).

Detecting flush via `koldstore.jobs` is feasible: flush already marks the job
`running` before selection (`mark_flush_job_running`).

#### Why it is more complex than it looks

| Issue | Detail |
|-------|--------|
| **Not sufficient alone at prune time** | Sync bump protects commits that finish *before* prune. Concurrent UPDATE vs prune still races on the mirror/hot row. Example: prune deletes mirror at `S_old` while user UPDATE is in flight; trigger then updates 0 mirror rows; prune deletes hot → same data loss. Needs an extra rule (table lock around prune, or fail-closed/upsert if mirror missing while flush running). |
| **TOCTOU on “flush running?”** | Trigger checks jobs, sees idle; flush then marks running; statement commits with no sync bump → lag until WAL apply → original race. Low probability but real unless the check and mirror write are tied to a stronger fence (e.g. lock that flush also holds), which collapses toward the prune-fence design. |
| **False negatives / positives** | Crashed flush left `status='running'` → every UPDATE/DELETE pays sync cost until job repair. Job not yet `running` at DML time → miss. `pending` vs `running` policy must be explicit. |
| **Partial strictness** | Reintroduces foreground mirror writes for all UPDATE/DELETE on that table whenever a flush runs — often the hot path under continuous flush pressure. Undercuts async’s latency thesis for updates (INSERT still async). |
| **Two capture paths** | Async tables grow conditional trigger logic + WAL apply. Testing matrix doubles (flush idle vs running; bump then apply; bump vs prune; missed flag). |
| **“Only if already in mirror”** | Correct for normal managed live rows. If mirror row is missing (bug, mid-prune, or lag after reinsert), silent no-op leaves hot without mirror. Strict mode fail-closes; this design must choose fail-closed vs upsert. |
| **DELETE semantics** | Must set `op=3` and new `seq`, not only bump seq — same as strict — or merge/cold overlay is wrong even if prune skips the PK. |
| **INSERT / reinsert** | Out of scope in the proposal. Reinsert over a tombstone that is still `seq <= max_seq` can interact with prune removing the tombstone before apply creates `op=1`. Niche but real. |
| **Counters** | Sync DELETE during flush may need the same hot-count delta rules as strict, or counters drift until apply — another dual-path bug surface. |

#### Complexity verdict

| Axis | Rating |
|------|--------|
| Implementability | **Doable** (reuse strict UPDATE/DELETE SQL fragments + jobs predicate) |
| Correctness if used alone | **Incomplete** — closes upload-window lag; does not close prune∩DML without more machinery |
| Operational complexity | **Higher** than prune fence (dual path, flag TOCTOU, crash `running` jobs) |
| Performance fit for async | **Mixed** — no end-of-flush writer stall; reintroduces sync mirror cost during every in-flight flush |
| Test burden | **Higher** |

#### Compared to writer lock + bounded apply + prune

| | Flush-aware sync bump (G) | Prune fence (recommended) |
|--|---------------------------|---------------------------|
| Upload-window UPDATE/DELETE | Protected if flag visible | Protected by bounded final apply |
| Prune∩DML | Still open unless extra lock/upsert | Closed by table lock |
| Writer stall | None during upload; none at end* | Short stall at finalize only |
| Async INSERT path | Unchanged | Unchanged |
| Code paths | Trigger + WAL + flush flag | Flush finalize only |
| Failure mode if wrong | Silent wrong delete / missing mirror | Fail flush / retry |

\*Unless you still add a short prune lock to finish correctness.

#### Recommendation for this alternative

Treat **G as a possible optimization or complement**, not a full replacement:

- **Alone:** not recommended — remaining prune race and flush-flag TOCTOU.
- **With a short prune-only table lock (no bounded WAL apply):** interesting — sync bumps cover upload lag; lock covers the prune instant; skip catch-up if every mutating statement during `running` bumped the mirror. Still need a story for DML that missed the flag and for DELETE/INSERT edge cases.
- **With prune fence + bounded apply (primary):** sync bump is unnecessary for correctness; at most a latency nicety so merge overlays see newer seq earlier during a long upload.

**Default decision remains the prune fence.** Keep G documented so it is not
rediscovered without the failure modes above.

### H. Cooperative per-table advisory DML gate

**Idea:** Every mutating statement on an async table takes a transaction-scoped
shared advisory lock keyed by a Koldstore namespace plus `table_oid`. Flush
takes the exclusive form of that advisory lock before final apply and prune.

This can avoid conflicts with unrelated PostgreSQL maintenance/DDL lock modes,
but it is not a free or safer lock:

- every foreground `INSERT`/`UPDATE`/`DELETE` pays an advisory-lock operation;
- every write path must cooperate, including bulk load, partition routing,
  replication roles, disabled triggers, and future capture paths;
- advisory locks do not protect the table from a writer that bypasses the
  Koldstore trigger/gate;
- the exclusive gate still pauses writers for final WAL apply and cleanup;
- it does not reduce the shared-slot decoding or per-index DELETE work.

Use this only if measured relation-lock interactions are a problem and the
project is prepared to make the shared gate an enforced invariant of every
async DML path. `SHARE ROW EXCLUSIVE` is the recommended v1 mechanism because
PostgreSQL itself applies the conflicting `ROW EXCLUSIVE` lock to all ordinary
source DML.

### I. Lock only candidate rows

Taking row locks on the selected hot/mirror PKs appears narrower, but it does
not close the boundary:

- an old tombstone can have no hot row to lock, so a concurrent reinsert can
  cross prune;
- a newly inserted key has no pre-existing candidate row;
- locking a mirror row does not automatically serialize source heap DML;
- acquiring and retaining locks for a large prefix can cost more memory and
  create more deadlock surfaces than one relation lock.

Row locks can complement cleanup execution, but they cannot replace the
table-wide writer fence for correctness.

---

## Edge cases and required behavior

### Source mutation cases

| Case | Required result |
| --- | --- |
| `UPDATE` of a selected live PK during upload | Final apply writes a new `seq > max_seq`; mirror and hot row remain |
| `DELETE` of a selected live PK during upload | Final apply writes a new tombstone `seq > max_seq`; old cold image stays masked |
| Reinsert of a selected tombstone PK | Final apply changes the tombstone to a newer live operation; new hot row remains |
| Insert of a completely new PK | Not in the old mirror prefix and cannot be pruned; writer lock still closes commit ordering |
| Multiple changes to one PK in one source transaction | Apply the transaction in message order and retain its final operation; replay skipping is transaction-wide |
| A pre-existing `max_seq` from a numerically higher worker ID | Floor-aware target allocation still produces `new_seq > max_seq`; never rely on wall-clock delay between selection and apply |
| Source transaction commits while lock request waits | Relation lock is granted only after its `ROW EXCLUSIVE` releases; its commit is at or before `F1` and must be applied |
| Source transaction begins before lock but writes afterward | Its DML blocks when it requests `ROW EXCLUSIVE`; it cannot cross the catch-up/prune boundary |
| Tombstone-only force flush | Still needs the async fence because a concurrent reinsert can otherwise lose its protecting tombstone |

### WAL and slot cases

| Case | Required result |
| --- | --- |
| `synchronous_commit = off` | Force/wait for WAL flush through `F0`/`Fp`/`F1`; an empty decode result alone is not a fence |
| Continuous writes on another async table | `upto_lsn` bounds every pass; pre-lock catch-up handles accumulated work without blocking target writers |
| Apply throughput remains below WAL production | Exhaust the finite pre-lock budget and fail/back off before taking the relation lock; do not loop forever or enter a known-long fence |
| Upload stalls while apply checkpoint is uncommitted | Enforce overall timeout, monitor slot retained WAL, and abort/garbage-collect safely before slot invalidation or disk exhaustion |
| Slot `wal_status` becomes `lost` / invalidated | Fail closed; do not select or prune from a mirror whose missing WAL cannot be replayed |
| No row changes between `Fp` and `F1` | Final pass may apply zero rows and prune safely because `F1` is durable and bounded |
| Very large source transaction | Decode the complete committed transaction even if it exceeds internal row-message batch size; allocations remain batch-bounded |
| Replayed earlier-pass WAL | Parse and skip complete transactions through `L0`/`Lp`; do not generate new mirror sequences |
| Flush rollback after phase-0/final apply | Slot was not advanced to this transaction’s pending checkpoint; WAL remains replayable |
| Missing/lost/incompatible slot | Fail closed before selecting or deleting rows; never silently use stale mirror state |
| Slot checkpoint ahead of exact decoded end | Treat as corruption/bug and fail closed; never guess a later safe LSN |

### Locking and PostgreSQL activity

| Case | Required result |
| --- | --- |
| Ordinary long `SELECT` | Allowed by `SHARE ROW EXCLUSIVE`; its old snapshot may retain dead tuples until it ends |
| `SELECT ... FOR UPDATE/SHARE` on a prune candidate | Table lock may be compatible, but hot delete can wait on the row lock; statement/lock timeout bounds the pause |
| Long or idle source writer | Fence waits or times out before prune; no partial source delete |
| Prepared transaction holding a source lock | Fence waits or times out until `COMMIT PREPARED`/`ROLLBACK PREPARED` resolves it |
| Autovacuum | Normal conflicting autovacuum is normally interrupted by the lock request; wraparound-prevention vacuum is not, so timeout/fail/retry is required |
| DDL / `CREATE INDEX` / trigger changes | Serialize through PostgreSQL relation locks; use consistent lock order and retry deadlock victims |
| Schema-changing DDL during upload | The source scan's transaction `ACCESS SHARE` must remain held and block `ACCESS EXCLUSIVE` DDL; verify this explicitly, then upgrade to the final writer fence |
| Two flushes for the same table | Existing table job advisory lock serializes them |
| Flushes for different async tables | Current database apply transaction lock serializes all passes; document and measure this database-wide throughput limit |
| Caller keeps transaction open after `flush_table()` | Unsupported or explicitly rejected because source writers remain blocked until caller commit |

### Cleanup, indexes, and vacuum

- Every hot tuple delete also removes entries from every source index. Tables
  with many or expensive extension indexes will have lower prune throughput;
  no generic SQL rewrite can avoid that cost while retaining those indexes.
- `DELETE` creates dead heap/index tuples. Space becomes reusable after ordinary
  vacuum; it is not immediately returned to the operating system.
- Old reader snapshots can delay physical reclamation after writers unblock.
- `TRUNCATE` is not an alternative: cleanup deletes only a stable old prefix and
  must retain newer hot rows.
- Parallel cleanup workers are not a v1 optimization. They compete for the same
  relation fence, increase deadlock risk, and do not remove per-index delete
  work.
- Normal user triggers are suppressed by the existing cleanup guard. Validate
  behavior for `ENABLE ALWAYS` triggers and rewrite rules; either reject them at
  manage time or document that they can run and increase latency/side effects.
- Flush-enabled tables already reject unsupported foreign keys and non-PK unique
  constraints unless an explicit hot-only policy permits them. Revalidate that
  DDL cannot add a constraint whose trigger or index work would change cleanup
  correctness or latency after management.
- Async primary-key updates remain governed by the existing PK guard. If they
  are supported later, logical apply must model old-PK delete plus new-PK insert
  and the fence must protect both identities.
- `TRUNCATE` and partition attach/detach are not ordinary row changes. Either
  reject them for managed async tables or implement explicit mirror/manifest
  semantics; do not assume bounded row apply handles them.

### Failure after manifest publish

Cold data can be durable before cleanup. This is safe only if the following
invariants hold:

- failure before mirror/hot cleanup leaves hot authoritative and retryable;
- the mirror+hot CTE remains one atomic statement;
- failure between cleanup and counter update cannot leave permanent counter
  drift — finalize must use an atomic subtransaction/statement or run explicit
  counter reconciliation before returning the job to service;
- retry never increments `cold_rows_added` twice for the same persisted segment;
- a failed job retains enough segment/prune watermark metadata to determine
  whether cleanup never started, completed, or needs reconciliation;
- cancellation and lock timeout follow the same fail-closed behavior.

Do not introduce multi-statement cleanup batching until durable partial-progress
and idempotent per-batch counters exist.

---

## Correctness argument (after the fix)

Let:

- `F0` be the durable selection WAL boundary;
- `L0` be the last source transaction applied before selection;
- `Fp` be the durable pre-lock catch-up boundary;
- `Lp` be the last source transaction applied by the pre-lock pass;
- `F1` be the durable WAL boundary captured after the writer lock;
- `W` be the mirror rows with `seq <= max_seq` when cleanup executes.

1. Phase 0 applies every decodable source transaction through `F0`, establishing
   the mirror state used to select `max_seq`.
2. Parquet contains exactly the bounded stable sequence prefix selected from
   that state.
3. The pre-lock pass applies through `Fp` while source writers are still
   allowed. This reduces the eventual lock window but is not relied on for
   correctness.
4. `SHARE ROW EXCLUSIVE` is granted only after earlier source writers finish,
   and prevents later source DML until transaction end.
5. WAL is durable through `F1`, captured after the writer lock is granted.
   Therefore every source commit that could precede prune is decodable at or
   before `F1`.
6. The final bounded pass applies complete source transactions in
   `(Lp, F1]`. Replayed transactions through `Lp` are skipped and do not receive
   new sequences.
7. The target-table allocator enforces every sequence generated in phases 5.5
   and 6 is strictly greater than `max_seq`, independent of backend worker ID or
   millisecond timing.
8. Any key updated, deleted, or reinserted after selection has a newer mirror
   operation with `seq > max_seq` before cleanup.
9. Seq-range mirror deletion therefore removes only keys whose selected state is
   still latest at prune time.
10. Hot deletion uses only PKs returned by that mirror deletion and cannot remove
    a newer source row: such a row would require a source commit applied in step
    6, which would have moved the mirror row out of `W`.
11. Old cold images for keys that remain hot are expected and are masked by merge
   resolution (`HOT_SEQ_SENTINEL`).
12. The pending applied checkpoint commits with the mirror/prune transaction.
    Only a later transaction may advance the slot to it, preserving replay after
    rollback or crash.

---

## Failure and operational behavior

| Event | Required behavior |
| --- | --- |
| Phase-0 bounded apply fails | Abort/fail job before selection; no cold write or prune |
| Manifest/object write fails | No prune; hot remains authoritative |
| Upload exceeds overall duration or retained-WAL budget | Cancel before prune, release transaction/apply lock, and garbage-collect unreferenced objects |
| Logical slot is invalidated/lost | Fail closed and require the existing slot/mirror recovery procedure; never advance or prune |
| Pre-lock catch-up exceeds its budget | Do not take the source relation lock; fail/back off with hot authoritative |
| Relation lock times out | No prune; mark retryable and avoid indefinite writer convoy |
| Final WAL delta exceeds the hard fence limit | Abort before prune, release locks at transaction end, and retry after catch-up/backoff |
| WAL cannot become durable through `F1` | No prune; fail closed |
| Final bounded apply fails | No prune; pending checkpoint is not acknowledged |
| Cleanup SQL fails | Atomic mirror+hot statement rolls back; reconcile/fail job |
| Counter/job completion fails after cleanup | Reconcile from actual tables/segments before retry; never double-add cold counts |
| Backend crash before transaction commit | Relation/apply locks release; uncommitted mirror/prune effects roll back; slot can replay |
| Very large eligible set | Select one capped complete seq prefix (subject to the boundary-tie rule); handle the remainder in later transactions |
| Equal `seq` values at the row-cap boundary | Fail the uniqueness invariant or use a composite watermark; never prune an unflushed row sharing `max_seq` |
| Long cleanup despite cap | Record phase timings; lower cap or move to independently committed cleanup batches |
| Strict table | No async bounded apply/fence in this design; separately prove or fix its cross-backend `new_seq > max_seq` invariant |

Manifest publish before the prune fence remains intentional because that is the
current cold-persistence ordering. The fence changes source/mirror consistency,
not object visibility. Post-publish failures must therefore preserve hot-wins
merge semantics and idempotent recovery.

---

## Testing plan

Prefer deterministic failpoints and isolation schedules over sleeps.

### Correctness isolation tests

1. **`update_during_async_flush_prune`**
   - Pause after manifest publish and before the writer fence.
   - Commit `UPDATE` for a key inside the watermark.
   - Resume and assert new hot values survive, mirror `seq > max_seq`, and cold
     old image is masked.
2. **`delete_during_async_flush_prune`**
   - Commit `DELETE` during upload.
   - Assert a newer tombstone remains and old cold row is invisible.
3. **`reinsert_tombstone_during_async_flush_prune`**
   - Select an old tombstone, reinsert the same PK during upload, then finalize.
   - Assert the live hot row and newer mirror operation survive.
4. **`writer_commits_while_prune_lock_waits`**
   - Hold a source writer open, request the fence, commit the writer, then assert
     its WAL is included before prune.
5. **`new_writer_blocks_during_prune_fence`**
   - Pause after the relation lock is granted; assert new DML waits while
     ordinary `SELECT` completes.
6. **`other_async_table_writes_do_not_starve_fence`**
   - Accumulate WAL on a second published table during object upload.
   - Assert the phase-5.5 pass processes the backlog before the relation lock,
     the final `F1` delta terminates, and the target table is correct.
7. **`prelock_apply_overload_fails_before_writer_fence`**
   - Produce global-slot WAL faster than the configured catch-up budget.
   - Assert flush backs off without acquiring the source relation lock or
     pruning any row.

### WAL/checkpoint tests

8. **`async_fence_with_synchronous_commit_off`**
   - Commit selected-key DML with `synchronous_commit = off`; assert durability
     wait makes it visible to the bounded pass.
9. **`second_apply_does_not_ack_current_transaction`**
   - Run phase 0 and final apply, then force outer rollback/crash.
   - Assert `confirmed_flush_lsn` did not move past the durable prior checkpoint
     and WAL replays successfully.
10. **`checkpoint_uses_exact_decoded_end_lsn`**
   - Commit on another async table between cursor exhaustion and checkpoint
     failpoint; assert that transaction is not skipped.
11. **`replay_skip_preserves_sequence`**
    - Re-decode earlier-pass transactions during pre-lock/final apply and assert
      their mirror sequences do not change.
12. **`same_pk_multiple_changes_one_transaction`**
    - Insert/update/delete or update/update one PK in one source transaction;
      assert transaction-wide replay skipping preserves the final operation.
13. **`target_seq_floor_cross_worker_ordering`**
    - Seed `max_seq` from a numerically higher worker ID, then apply a target
      mutation from a lower worker ID in the same logical millisecond.
    - Assert the floor-aware allocator produces a distinct `seq > max_seq`.
    - Run the corresponding strict-mode audit; if it fails, track and fix the
      strict bug before claiming the mode is safe.

### Cleanup size and operational tests

14. **`flush_total_row_cap`**
    - Create more eligible rows than `max_rows_per_flush`.
    - Assert one job writes/prunes only the oldest bounded prefix and a later job
      safely processes the remainder.
15. **`wide_composite_pk_cleanup_memory_bound`**
    - Exercise the cap with wide composite PKs and assert no unbounded JSON or
      returned-row allocation.
16. **`row_cap_does_not_split_seq_ties`**
    - Construct or simulate equal sequence values at the cap boundary.
    - Assert every row removed by `seq <= max_seq` was included in the persisted
      prefix, or assert schema validation proves sequence uniqueness.
17. **`lock_timeout_is_retryable`**
    - Hold a long writer/row locker; assert timeout causes no prune and retry
      later succeeds.
18. **`final_delta_limit_is_retryable`**
    - Let the relation-lock wait create a delta above the hard fence limit.
    - Assert the transaction aborts before prune and a later retry succeeds.
19. **`failure_after_cleanup_reconciles_counters`**
    - Fail after cleanup and before job completion; assert hot, mirror, cold, and
      catalog counters reconcile without double counting.
20. **`strict_flush_avoids_async_fence`**
    - Keep an async slot for another table, flush a strict table, and assert no
      source writer fence/database apply work is taken for the strict table.
21. **`stalled_upload_retained_wal_fails_closed`**
    - Pause object upload while producing WAL on another async table.
    - Assert timeout/retention admission aborts without prune, releases the apply
      lock, and leaves any uploaded-but-unreferenced object collectible.
22. **`schema_ddl_waits_for_flush_snapshot`**
    - Pause after source selection and request schema-changing DDL.
    - Assert DDL cannot commit before flush releases its source relation locks.
23. **`unsupported_async_truncate_is_rejected`**
    - Assert managed async `TRUNCATE` fails unless explicit mirror/manifest
      semantics have been implemented and tested.
24. **Performance matrix** from
    [Delete-plan verification and benchmarks](#delete-plan-verification-and-benchmarks)
    records lock hold time, writer wait, WAL, temp bytes, and vacuum cost on
    PostgreSQL 15–18.

Existing regression coverage under `tests/e2e/isolation/schedules.rs` must still
pass in strict and async modes. Reuse `before_hot_cleanup` and
`during_hot_cleanup`; add failpoints after pre-lock catch-up, after relation-lock
acquisition, after `F1` durability, after final apply, and after cleanup/before
counter update.

---

## Implementation checklist

### Apply protocol

- [ ] Resolve the target table’s capture mode before the phase-0 apply call
- [x] Add typed `WalFenceLsn`, `AppliedWalBoundary`, and bounded apply request/outcome
- [x] Add typed `PruneSeqFloor` and an overflow-checked floor-aware allocator
- [x] Split durable slot acknowledgement from peek/apply
- [x] Pass explicit `upto_lsn` to logical decoding (phase-6 `F1`; phase-0 still unbounded available)
- [x] Force/wait for WAL durability through fence LSN (phase-6 `F1`)
- [x] Return and retain the exact phase-0 transaction end-LSN `L0`
- [ ] Run a bounded pre-lock catch-up and retain its exact boundary `Lp`
- [x] Skip complete replayed transactions through the latest local boundary
- [x] Never acknowledge a checkpoint written by the current flush transaction
- [ ] Store exact decoded end-LSN; remove `GREATEST(..., pg_current_wal_insert_lsn())`
- [ ] Audit strict-mode sequence ordering across PostgreSQL backends

### Flush and cleanup

- [ ] Add `max_rows_per_flush` and cap the selected seq prefix for policy/force flush
- [ ] Prove sequence uniqueness or implement a stable composite cleanup watermark
- [ ] Add finite pre-lock catch-up budgets and a final-delta safety limit
- [ ] Add overall flush/upload and retained-WAL admission limits
- [x] Acquire `SHARE ROW EXCLUSIVE` by source table OID only for async tables
- [x] Call final bounded apply after manifest publish and before cleanup
- [x] Keep mirror+hot delete as one atomic data-modifying CTE
- [x] Add configurable local relation-lock/cleanup timeout behavior
- [ ] Enforce or document autocommit/worker-owned flush transaction lifetime
- [ ] Make cleanup/counter/job failure recovery idempotent and reconcilable
- [ ] Verify source `ACCESS SHARE` prevents schema changes throughout selection/upload
- [ ] Reject or explicitly implement async `TRUNCATE` and partition topology changes
- [ ] Record phase timings and actual prune counts for operational diagnosis

### Documentation and verification

- [ ] Update [flushing-table](../architecture/flushing-table.md) with phase-0,
  phase-5.5, and phase-6 fences plus the total-row cap
- [ ] Update [mirror-capture-modes](../architecture/mirror-capture-modes.md) from one automatic fence to selection + prune fences
- [ ] Update [ADR-003](../decisions/003-optional-async-mirror-capture.md) with exact checkpoint/acknowledgement rules
- [ ] Document `max_rows_per_flush`, lock timeout, and transaction-lifetime requirements
- [ ] Land all correctness, rollback, cap, contention, and performance tests above
- [ ] Run local pgrx PostgreSQL 15–18 verification; keep Docker as packaging smoke only

---

## PostgreSQL references

- [Table lock modes and conflicts](https://www.postgresql.org/docs/18/explicit-locking.html)
- [`LOCK TABLE` and why writers should take `SHARE ROW EXCLUSIVE`](https://www.postgresql.org/docs/18/sql-lock.html)
- [Logical slot peek and advance functions](https://www.postgresql.org/docs/18/functions-admin.html)
- [`confirmed_flush_lsn` retention semantics](https://www.postgresql.org/docs/18/view-pg-replication-slots.html)
- [Logical decoding only reads safely flushed transactions](https://www.postgresql.org/docs/18/logicaldecoding-output-plugin.html)
- [`DELETE ... RETURNING`](https://www.postgresql.org/docs/18/dml-returning.html)
- [Routine vacuuming after large deletes](https://www.postgresql.org/docs/18/routine-vacuuming.html)

---

## Decision

Implement a **short `SHARE ROW EXCLUSIVE` prune fence plus WAL apply bounded by
a durable `F1` LSN** for async tables. Reuse the existing transaction-scoped
database apply lock, but do not call the current `apply_available()` in a
drain-until-zero loop and do not acknowledge checkpoints written by the current
flush transaction. Run a bounded pre-lock pass through `Fp` after object upload
so the relation lock normally covers only `(Lp, F1]` plus cleanup. Apply target
mutations after selection with a typed floor that guarantees `seq > max_seq`.

Bound writer latency in v1 by limiting the total stable seq prefix processed by
one flush with `max_rows_per_flush`, limiting pre-lock retries, and refusing a
known-oversized final WAL delta. Keep the current single-statement atomic
mirror+hot delete. Evaluate independently committed cleanup batches only if the
measured bounded cleanup still cannot meet the writer-pause target.

Flush-aware synchronous bumps (alternative G) and advisory DML gates remain
possible optimizations, not primary correctness mechanisms. They add foreground
cost and do not eliminate the need for a prune-time serialization rule.

This preserves concurrent DML during object upload, prevents deletion of newer
async source state, keeps slot acknowledgement crash-safe, and gives large
cleanup an explicit, measurable bound.
