# Production UPDATE Performance Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development to implement this plan task-by-task.

**Goal:** Bring async foreground UPDATE close to PostgreSQL while increasing sustainable mirror catch-up, preserving crash safety, bounded memory, and scalable mirror indexes.

**Architecture:** Keep async logical-WAL capture and strict transactional capture as separate consistency products. Specialize async UPDATE apply for the common existing-row path with an insert-missing fallback, let bounded pending work make short immediate progress bursts, and treat retained WAL as health/backpressure telemetry without ever stopping the drain path. Make benchmark gates and documentation mode-specific and explicit about foreground versus sustainable throughput.

**Tech Stack:** Rust, pgrx, PostgreSQL 15–18 logical decoding, SQL data-modifying CTEs, cargo-nextest, local pgrx PostgreSQL, pgbench/storage-comparison benchmarks.

---

### Task 1: Specialize async UPDATE apply

**Files:**
- Modify: `crates/koldstore-mirror/tests/storage_contract.rs`
- Modify: `crates/koldstore-mirror/src/shared/write.rs`
- Modify: `crates/pg_koldstore/src/async_mirror/apply.rs`

1. Add a failing SQL-contract test requiring `plan_async_mirror_batch_update` to perform a typed set-based `UPDATE`, then upsert only missing PKs.
2. Run `cargo nextest run -p koldstore-mirror --test storage_contract async_mirror_batch_update` and confirm the old unified upsert fails the assertion.
3. Implement a data-modifying CTE that returns updated keys and applies `INSERT ... ON CONFLICT` only to missing rows.
4. Cache separate upsert and update plans in `ManagedRelation`; select the update plan only for `MirrorOperation::Update`.
5. Re-run the focused test and the full `koldstore-mirror` test suite.

### Task 2: Make bounded pending work progress fairly

**Files:**
- Modify: `crates/koldstore-worker/src/scheduler.rs`
- Modify: `crates/koldstore-worker/src/lib.rs`
- Modify: `crates/pg_koldstore/src/database_worker/loop.rs`

1. Add failing pure-Rust tests for a pending-poll budget: four `ContinuePending` results retry immediately, the fifth yields, and ordinary/error paths reset the burst.
2. Run `cargo nextest run -p koldstore-worker pending_poll` and confirm the missing policy fails to compile.
3. Implement the small state object and integrate it into the database worker without changing transaction, signal, or backoff semantics.
4. Re-run `cargo nextest run -p koldstore-worker` and compile the extension crate.

### Task 3: Keep retained-WAL pressure from stopping catch-up

**Files:**
- Modify: `crates/pg_koldstore/src/async_mirror/apply.rs`
- Modify: `crates/pg_koldstore/src/async_mirror/status.rs`
- Modify: `crates/pg_koldstore/src/pg_tests/async_mirror_worker.inc.rs`
- Modify: `tests/e2e/dml/async_mirror_worker.rs`

1. Add a failing async E2E regression that creates retained WAL above a tiny configured threshold and proves `wait_for_async_mirror()` still drains it exactly once.
2. Run that single E2E test against local pgrx PostgreSQL 16 and confirm the old admission error.
3. Remove retained-WAL rejection from the apply path; retain status health fields and typed retained-byte observation.
4. Update the in-server status regression and run focused pgrx/E2E tests.

### Task 4: Split benchmark gates by consistency mode

**Files:**
- Modify: `benchmarks/src/verdict.rs`
- Modify: `benchmarks/src/main.rs`

1. Add failing unit tests for async UPDATE overhead `<= 1.10x` and strict UPDATE overhead `<= 2.00x`.
2. Run `cargo nextest run -p pg-koldstore-benchmarks verdict` and confirm the old single `2.6x` gate cannot satisfy them.
3. Introduce mode-specific constants/functions and identify the current pgbench runner explicitly as strict mode.
4. Re-run the benchmark crate tests.

### Task 5: Make published benchmark claims production-oriented

**Files:**
- Modify: `scripts/run-storage-comparison.sh`
- Modify: `scripts/render-storage-comparison-results.py`
- Modify: `docs/benchmarks/README.md`
- Modify: `docs/benchmarks/RESULTS.md`
- Modify: `docs/performance.md`
- Modify: `docs/sql-api.md`
- Modify: `docs/operations/scheduling.md`
- Modify: `docs/operations/upgrade.md`
- Modify: `docs/architecture/mirror-capture-async.md`

1. Refuse `--update-results` from a dirty tree so published output is reproducible.
2. Label the existing UPDATE row with its exact single-sample deltas and distinguish foreground from sustainable throughput.
3. Require six counterbalanced isolated samples for release decisions, median plus dispersion, worker-on lag/backlog metrics, and separate single-row versus 1,000-row UPDATE reporting.
4. Document retained-WAL configuration as a health threshold that never blocks the applier; document PostgreSQL slot-retention limits and rebuild implications separately.
5. Document async and strict performance SLOs independently.

### Task 6: Verify correctness and performance

**Files:**
- No additional production files.

1. Run formatting, `git diff --check`, targeted unit tests, extension compile, focused pgrx tests, and async E2E regression.
2. Run the repeated local storage comparison and worker-on pgbench UPDATE probes used for the baseline.
3. Compare before/after async catch-up, foreground TPS, backlog, and strict correctness; reject the optimization if it regresses correctness or scalable change-feed selection.
4. Review the complete diff and report any environment-limited verification explicitly.

### Task 7: Synchronize architecture documentation

**Files:**
- Modify: `docs/architecture/mirror-capture-async.md`
- Modify: `docs/architecture/mirror-capture-modes.md`
- Modify: `docs/architecture/dml-table.md`
- Modify: other architecture index/worker pages only where the audit finds stale behavior

1. Map the implemented async UPDATE planner, plan caches, pending-tick retry budget, and retained-WAL health behavior to the architecture pages that own those contracts.
2. Document the direct `UPDATE ... FROM` common path and insert-missing fallback, including why mirror indexes remain required.
3. Document the worker's four immediate bounded retries followed by a latch yield, without presenting it as an unbounded drain loop.
4. State that the retained-WAL threshold affects health status only; PostgreSQL disk and slot-retention controls remain independent hard safeguards.
5. Cross-check architecture links and search for stale claims that async UPDATE is a unified upsert or that the health threshold blocks apply.
6. Run Markdown/link-oriented repository checks available locally plus `git diff --check`.
