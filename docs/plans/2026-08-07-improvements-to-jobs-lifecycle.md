# Improvements to Jobs Lifecycle (Flush Queue Hardening)

**Date:** 2026-08-07 (prompt / working plan)  
**Status (as of 2026-08-06):** **Active backlog — mostly not implemented.** Narrowed MVP for ordered-flush memory **done**; rest deferred by product choice (prefer scanning work first).

This document is the full 12-phase hardening prompt (autovacuum-style coordinator, pass resume, spool, etc.). It overlaps and extends [2026-08-05-worker-flush-job-queue.md](2026-08-05-worker-flush-job-queue.md).

### Phase checklist

| Phase | Intent | State |
| --- | --- | --- |
| 1 | Coordinator-only launch + shmem worker reservation | **Not done** (clients still `spawn_flush_executor_if_needed`) |
| 2 | Fair indexed claim + candidate page; coordinator orphan reclaim | **Partial** (ORDER BY exists; no claim index / page of 16) |
| 3 | Durable `flush_passes` + true crash resume | **Not done** |
| 4 | Single RR snapshot → local spool → upload outside PG | **Not done** (streaming pages only) |
| 5 | Remove unbounded ordered-row memory; PG sort | **Done** (MVP 2026-08-06); no `flush_worker_memory_mb` yet |
| 6 | Manifest I/O outside slot lock | **Not done** |
| 7 | Safe activation SQL (FOR UPDATE + writer/pass cardinality) | **Partial** (generation CAS only) |
| 8 | Typed retryable outcomes + backoff | **Not done** |
| 9 | Bounded final fence GUC | **Partial** (prelock GUCs; no `flush_final_fence_max_ms`) |
| 10 | Cost balancing / WaitLatch yield | **Partial** (`max_parallel_flush_jobs` only) |
| 11 | Async DROP remote GC | **Not done** (sync delete in DROP) |
| 12 | Compile-out / SUSET inline + failpoints | **Not done** (Userset) |

**Verdict:** Keep as the hardening backlog. **Not outdated.** Prefer high-ROI slices from the Aug-5 plan audit (manifest out of slot lock, activation safety, retryable slot busy, coordinator-only spawn) before claiming production readiness.

---

You are working in:

Repository: kalamdb/koldstore
Branch: feature/worker-flush-job-queue
Base: main

Goal:
Harden the worker-owned flush queue into a production-grade PostgreSQL
maintenance subsystem. Follow PostgreSQL/autovacuum principles: one coordinator
owns worker scheduling, maintenance yields to foreground work, resource impact is
bounded globally, workers never perform remote I/O while holding critical
PostgreSQL locks, and crashes resume the same durable job/pass safely.

Do not introduce Redis, SQS, Kafka, pg_durable, duroxide, or another general
job runtime.

Do not weaken the existing pending-segment + generation-CAS publication model.

Before changing code:
1. Read:
   - docs/plans/2026-08-05-worker-flush-job-queue.md
   - docs/decisions/004-segment-publication-protocol.md
   - crates/pg_koldstore/src/worker/flush_executor.rs
   - crates/pg_koldstore/src/worker/flush_task.rs
   - crates/pg_koldstore/src/worker/loop.rs
   - crates/pg_koldstore/src/sql/flush/execute.rs
   - crates/pg_koldstore/src/sql/flush/jobs.rs
   - crates/pg_koldstore/src/sql/flush/spi.rs
   - crates/pg_koldstore/src/sql/job_lock.rs
   - crates/koldstore-flush/src/table_jobs.rs
   - crates/koldstore-flush/src/segment_catalog.rs
   - crates/koldstore-flush/src/encode.rs
   - crates/koldstore-storage/src/fault.rs
   - crates/koldstore-storage/src/model.rs
2. Produce a short implementation plan before editing.
3. Keep PostgreSQL 15, 16, 17, and 18 support.
4. Do not perform unrelated refactors.

NON-NEGOTIABLE INVARIANTS

1. No object-store or filesystem network I/O while holding:
   - the database slot advisory lock,
   - the source-table SHARE ROW EXCLUSIVE lock,
   - or the final activation/prune transaction.

2. A pass may prune hot/mirror rows only after every expected segment:
   - exists in the catalog,
   - is status=pending,
   - belongs to the expected job_id and pass_id,
   - has the expected row count,
   - and has a validated immutable object.

3. Activation, hot/mirror prune, counters, and durable pass checkpoint must
   commit atomically.

4. A crashed executor must resume the same durable job and pass. It must not
   silently create a new target watermark or duplicate a previously uploaded
   range.

5. Slot contention, source-lock contention, worker-slot exhaustion, network
   timeouts, throttling, HTTP 5xx, and transient object-store failures are
   retryable scheduling outcomes, not terminal job errors.

6. Same-table work is serialized across commits with the existing session
   advisory lock.

7. Different tables may encode/upload concurrently.

8. The queue must never exceed configured per-database or cluster-wide worker
   limits, including workers in the “starting” state.

9. All waits must be interruptible. Do not use long std::thread::sleep loops
   inside PostgreSQL transactions.

10. Every attempt-fenced UPDATE must require exactly one affected job row.
    Zero affected rows means stale ownership and the executor must stop before
    any further side effect.

PHASE 1 — COORDINATOR-ONLY WORKER LAUNCHING

Modify queue mode so client backends never register flush executors.

In `flush_table_pg_impl`:
- enqueue or reuse the durable job UUID,
- ensure/wake the database coordinator,
- return immediately,
- remove direct `spawn_flush_executor_if_needed()`.

Only the database coordinator may register executors.

Add a worker reservation registry modeled after PostgreSQL autovacuum:
- track starting and running flush executors,
- track database OID,
- track job/table reservation where practical,
- atomically reserve before dynamic worker registration,
- release reservation on registration failure,
- transition starting→running when the executor connects,
- release on worker exit/crash,
- enforce per-database max_parallel_flush_jobs,
- enforce a new conservative cluster-wide max flush-worker limit.

Use shared memory because advisory locks are database-scoped and cannot safely
enforce a cluster-wide cap across databases.

The coordinator must be signalled when:
- a job is enqueued,
- an executor exits,
- an executor requeues work,
- a worker registration fails transiently.

Keep one-shot executors for now.

PHASE 2 — FAIR, INDEXED CLAIMING

Replace:
- full pending count(*) scans,
- JSON aggregation of all running table OIDs,
- first-candidate-only claiming.

Add an index matching claim order:

CREATE INDEX ... ON koldstore.jobs
    (available_at, updated_at, id)
    INCLUDE (table_oid)
WHERE job_type = 'flush' AND status = 'pending';

Select a bounded candidate page, e.g. 16:
- available_at <= clock_timestamp()
- ORDER BY available_at, updated_at, id
- attempt session table try-lock for each candidate
- skip busy tables
- claim the first runnable candidate.

Move orphan-running scans to the coordinator only.
Process them in bounded batches.
Executors must not scan every running job before each claim.

Allow an auto-flush tick to enqueue a bounded number of due tables rather than
only the first table. Suggested bound:
max(available executor slots * 2, 8), capped at 32.

PHASE 3 — DURABLE PASS STATE AND TRUE RESUME

Introduce an explicit domain pass record. Prefer a small typed table rather than
hiding correctness state inside arbitrary JSON:

koldstore.flush_passes:
- pass_id uuid primary key
- job_id uuid not null
- table_oid oid not null
- range_start_seq bigint not null
- range_end_seq bigint not null
- expected_rows bigint not null
- uploaded_rows bigint not null default 0
- expected_segment_count integer
- expected_generation bigint
- state text: selected/uploading/ready/finalized/abandoned
- created_at/updated_at
- unique(job_id, range_start_seq, range_end_seq)
- index(job_id, state)

Before any remote upload:
- persist selected pass range and expected row count.

After the final segment is durably cataloged:
- mark pass ready,
- store expected segment count and uploaded row count.

On executor claim:
- load the job row and return the persisted flush_seq_upper_bound,
  checkpoint_seq, rows_flushed, batches_completed, and started_at;
- never use a newly computed in-memory watermark when a durable watermark exists;
- reconcile non-finalized passes before selecting new work.

Recovery behavior:
- ready pass: validate and finalize;
- uploading pass with a complete valid segment set: mark ready and finalize;
- incomplete pass: quarantine/delete incomplete objects and regenerate the same
  sequence range;
- finalized pass with stale job checkpoint: repair checkpoint idempotently.

Add writer ownership to every activation predicate:
writer_job_id and pass_id must match.
The attempt token may change after reclaim, so pass ownership must not require
the old attempt token for resume.

PHASE 4 — CONSISTENT PASS SNAPSHOT

Current stats and mirror pages use separate transactions. Replace this with one
consistent pass snapshot.

Implement queue-mode pass production as:

1. Open REPEATABLE READ read transaction.
2. Resolve pass stats and sequence range.
3. Read all mirror/hot payload pages through one cursor/snapshot.
4. Encode into bounded local spool files.
5. Close cursor and commit the transaction.
6. Upload spool files outside PostgreSQL.
7. Catalog pending segments in short transactions.
8. Securely remove spool files.

Requirements:
- spool directory permissions must be owner-only;
- spool files are non-authoritative and may be discarded after crash;
- never retain the complete pass in Rust memory;
- check interrupts and runtime budget between pages and row groups;
- bound local spool bytes per worker.

If a complete spool implementation is too large for one change, first keep the
current fail-closed validation but classify selection drift as retryable and add
a documented follow-up. Do not claim full production stability until the
single-snapshot spool path is complete.

PHASE 5 — REMOVE UNBOUNDED ORDERED-ROW MEMORY

In crates/koldstore-flush/src/encode.rs:
- remove ordered_rows Vec accumulation,
- remove Rust sorting of all FlushMirrorRow values,
- remove debug-format allocation from the comparator.

Push physical ordering into PostgreSQL:
ORDER BY order_key, primary-key columns, seq.

Consume it using the pass snapshot cursor in pages.
Allow PostgreSQL work_mem/temp-file spilling rather than using an unbounded Rust
Vec.

Add:
koldstore.flush_worker_memory_mb
with a conservative default, e.g. 128 MiB.

Enforce the budget across:
- decoded rows,
- Arrow builders,
- Parquet buffers,
- local spool buffers,
- manifest structures.

Prefer streaming/file-backed Parquet encoding and streaming uploads where the
storage API supports it.

For one-shot queue executors:
- remove malloc_trim from every pass,
- process exit will reclaim the heap,
- retain threshold-based trimming only for inline/long-lived backends.

PHASE 6 — SPLIT MANIFEST I/O FROM FINALIZATION

Refactor build_manifest_and_finalize/finalize_flush.

No write_manifest_with_client call may occur under with_slot_lock_retry.

Implement generation-specific immutable manifest publication:

1. Read expected generation and build candidate manifest in a short read txn.
2. Write immutable generation-specific manifest shards/root outside PostgreSQL
   and outside the slot lock.
3. Validate upload result/checksum.
4. Enter finalization transaction.
5. Try slot lock once.
6. Try source-table lock with a short timeout.
7. Revalidate:
   - manifest generation,
   - complete pass segment set,
   - pass row count,
   - object validation marker,
   - schema version.
8. Fence/apply WAL.
9. Atomically:
   - generation CAS,
   - pending→active,
   - prune,
   - counters,
   - pass finalized,
   - job checkpoint.
10. Commit.
11. Outside locks, update the derived latest manifest pointer.
12. If the derived pointer update fails, mark sync_state=pending_write and let
    a retry worker repair it. Do not invalidate already-activated catalog data.

PHASE 7 — SAFE ACTIVATION SQL

Rewrite plan_activate_flush_segments.

Before CAS:
- select all expected segment IDs FOR UPDATE;
- require exact cardinality;
- require exact writer_job_id and pass_id;
- require status=pending;
- require sum(row_count)=pass.expected_rows;
- reject duplicate ordinals;
- require segment count=pass.expected_segment_count.

The generation CAS must execute only when all checks succeed.

After UPDATE:
- require activated count equals expected count.

Return zero rows on any mismatch.
Never bump generation and then activate only a subset.

Add SQL/unit tests for:
- one missing ID,
- one already-active ID,
- wrong job owner,
- wrong pass,
- wrong row total,
- duplicate ordinal,
- generation conflict.

PHASE 8 — RETRYABLE OUTCOMES

Create a typed FlushError classification:

Retryable:
- slot busy
- source lock timeout
- worker capacity unavailable
- network timeout/reset
- DNS temporary failure
- object-store throttling
- HTTP 5xx
- generation conflict
- pass selection changed concurrently

Permanent:
- unsupported type
- invalid schema
- permission/credential failure after classification
- checksum mismatch
- corrupt locally generated Parquet
- catalog invariant violation

Retryable failure:
- rollback active transaction,
- keep uploaded pending pass state,
- set job status=pending,
- clear attempt_token,
- set available_at using exponential backoff with jitter,
- increment retry metadata,
- wake coordinator,
- exit executor.

Do not mark slot contention as status=error.

Use a bounded maximum retry count only for repeated identical permanent-looking
failures. Preserve an operator-readable error history.

PHASE 9 — BOUNDED FINAL FENCE

Remove the unlimited final apply while SHARE ROW EXCLUSIVE is held.

Add:
- koldstore.flush_final_fence_max_ms
- a conservative default, e.g. 1000 ms
- hard max configurable by operator.

Pre-lock catch-up should drain most WAL.

After source lock:
- capture fixed durable fence;
- apply only through that fence;
- check hard wall-clock deadline between decoded message batches;
- if deadline is exceeded, raise a retryable error and abort the complete
  finalization transaction;
- no partial mirror apply, activation, or prune may commit.

Ensure all lock waits use lock_timeout and interrupt checks.

PHASE 10 — RESOURCE COST BALANCING

Add an autovacuum-inspired maintenance budget.

At minimum:
- cluster-wide active flush worker cap;
- per-worker memory cap;
- configurable flush cost delay;
- aggregate cost limit divided among active workers.

Charge approximate cost for:
- rows decoded,
- uncompressed bytes processed,
- compressed bytes generated,
- remote bytes uploaded,
- catalog batches.

Yield with PostgreSQL WaitLatch/interruptible wait.
Never use long std::thread::sleep in a PostgreSQL worker transaction.

Keep default max_parallel_flush_jobs=2 until all stress/failure tests pass.

PHASE 11 — DROP CLEANUP

Do not LIST and DELETE all remote objects inside the user's DROP TABLE
transaction.

On DROP:
- signal/cancel active work;
- acquire table ownership;
- transactionally deactivate/tombstone KoldStore metadata;
- record a durable drop-cleanup job;
- allow DROP to complete.

A background cleanup executor should perform remote LIST/DELETE idempotently.
Missing objects are success.
A failed object delete must not roll back the user's already-valid DROP.

PHASE 12 — GUC HARDENING

koldstore.flush_execution=inline is intended for pg_test, but it is currently a
user-settable production switch.

Make inline mode:
- compiled only under pg_test/test feature, or
- SUSET and rejected outside explicit test builds.

Do the same for destructive failpoint modes.
Ordinary application users must not be able to force synchronous inline flush
or arm failure injection.

TEST REQUIREMENTS

Do not mark the work complete until these tests pass.

A. Worker scheduling
- 100 concurrent flush_table callers for one table return one UUID.
- 100 callers across 20 tables never exceed per-database cap.
- Multiple databases never exceed cluster-wide cap.
- Include starting workers in cap enforcement.
- Busy first candidate does not starve later tables.
- Worker completion immediately refills available capacity.
- Client backend performs no dynamic worker registration.

B. Resume and crash
For each failpoint:
- after claim
- after pass selection
- during local encode
- after local spool
- during remote upload
- after object upload before catalog
- after pending catalog
- after pass-ready marker
- before slot lock
- after slot lock
- before activation
- after activation statement before commit
- during prune
- after prune before checkpoint
- after checkpoint before completion

Perform:
- SIGKILL executor,
- wait for PostgreSQL recovery,
- leave flush_execution=queue,
- do not manually switch to inline,
- do not manually insert a replacement job,
- verify coordinator automatically reclaims/resumes the same job UUID,
- verify final logical table against a reference PostgreSQL table,
- verify a second recovery run is a no-op.

C. Activation safety
Inject missing/wrong pending segments and prove:
- generation does not advance,
- prune does not run,
- hot data remains intact,
- job is retryable or errors safely.

D. Slow storage
Using MinIO plus Toxiproxy or a controllable storage wrapper:
- 30-second manifest delay must not hold slot lock;
- changes_since remains within the existing SLO during upload;
- timeout after server commit is resolved idempotently;
- partial copy destination is rejected;
- stale LIST/HEAD/GET combinations fail closed.

E. Memory
- ordered flush with 200k narrow rows;
- ordered flush with wide text/JSON payloads;
- two workers concurrently;
- assert per-worker and aggregate RSS budgets;
- prove no all-rows ordered Vec remains;
- ensure no retained RSS after one-shot executor exit.

F. Foreground server impact
Run pgbench-like foreground DML:
1. baseline without flush,
2. one active flush worker,
3. two active flush workers,
4. slow object storage.

Record:
- TPS,
- p50/p95/p99 latency,
- changes_since lag,
- slot retained WAL,
- source-lock duration,
- worker RSS,
- CPU.

Set explicit acceptance gates:
- idle queue subsystem adds no per-query catalog polling;
- upload phase does not materially affect mirror latency;
- normal source-lock hold remains sub-second;
- no unbounded WAL retention;
- foreground TPS degradation is bounded and documented.

G. Deadlock/isolation
Create isolation tests for:
- flush vs flush same table;
- flush vs manage;
- flush vs unmanage;
- flush vs DROP;
- flush finalize vs mirror apply;
- two table finalizers contending for one slot;
- cancellation during every wait.

Use statement_timeout and inspect pg_locks.
No test may require retrying a terminal slot-lock error.

H. Fault sweep
Run storage fault injection in:
- fail exactly operation N,
- fail operation N and every operation after N.

Advance N until the complete workflow succeeds.
After each failure:
- disable injection,
- restart/recover,
- run verify_table_integrity,
- compare with reference table,
- run recovery a second time and require no-op.

I. Integrity checker
Extend verify_table_integrity with optional deep mode:
- HEAD every active/pending object;
- verify size/checksum/etag;
- read and validate Parquet footer;
- compare footer row count and row groups with catalog;
- verify complete pass ownership;
- verify active segment generation;
- detect partial activation;
- cap work and samples to avoid unbounded operator queries.

COMMANDS / CI

Run and report:

cargo fmt --all -- --check
cargo clippy --workspace --all-targets --no-default-features -- -D warnings
cargo test -p koldstore-flush --lib
cargo test -p koldstore-storage --lib
cargo test -p koldstore-catalog --lib
cargo test -p koldstore-worker --lib

For PostgreSQL 15, 16, 17, 18:
- pg_test serial suite
- full E2E queue suite
- crash suite
- MinIO fault suite
- memory suite
- SQL regression
- storage/foreground performance comparison

Do not leave the PR test checklist unchecked.
Include benchmark results before and after.

DELIVERABLES

1. Code changes.
2. Updated ADR describing:
   - coordinator-only launch,
   - shared worker reservations,
   - pass recovery,
   - lock order,
   - retry policy,
   - resource-cost balancing.
3. Updated jobs/flush architecture documentation.
4. New schema and indexes.
5. Full tests listed above.
6. A final report containing:
   - files changed,
   - invariants established,
   - tests executed and results,
   - foreground performance comparison,
   - known remaining limitations.

Do not claim production readiness if automatic queue crash-resume, bounded
memory, complete segment activation validation, and foreground-load gates have
not passed.