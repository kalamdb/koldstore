# Async Mirror Autoprovision and Worker Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development and the repository cargo-pgrx workflow task-by-task.

**Goal:** Make async mirror capture zero-setup after `wal_level=logical`, apply committed WAL automatically within a bounded delay, and run the same DML correctness contract in strict and async modes.

**Architecture:** Extension installation creates the empty pgoutput publication. The first async `manage_table` call creates the deterministic slot before any transactional writes, activates PK-only publication capture, installs a lightweight worker-kick statement trigger, and starts one dynamic PostgreSQL background worker per database. The worker serializes slot access with explicit fences, applies committed changes in short polling transactions, and exits when async infrastructure is disabled.

**Tech Stack:** Rust, pgrx background workers and SPI, PostgreSQL pgoutput logical decoding, tokio-postgres E2E tests.

---

### Task 1: Autoprovision publication and slot

**Files:**
- Modify: `crates/pg_koldstore/sql/koldstore--0.1.0.sql`
- Modify: `crates/pg_koldstore/src/async_mirror/lifecycle.rs`
- Modify: `scripts/run-pg-e2e.sh`
- Test: `tests/e2e/dml/async_change_log_mirror.rs`

1. Add an E2E assertion that extension installation already created the empty publication and that no manual setup SQL is needed.
2. Remove publication/slot creation from the E2E wrapper and run the test to observe the missing-publication/slot failure.
3. Add idempotent empty-publication creation to extension bootstrap SQL.
4. Change `prepare_capture` to validate `wal_level`, validate/reuse a compatible slot, or create the deterministic pgoutput slot before manage-table writes.
5. Verify the async E2E passes from a freshly recreated database.

### Task 2: Automatic bounded-lag worker

**Files:**
- Create: `crates/pg_koldstore/src/async_mirror/worker.rs`
- Modify: `crates/pg_koldstore/src/async_mirror/mod.rs`
- Modify: `crates/pg_koldstore/src/async_mirror/apply.rs`
- Modify: `crates/pg_koldstore/src/async_mirror/lifecycle.rs`
- Modify: `crates/pg_koldstore/sql/koldstore--0.1.0.sql`
- Test: `tests/e2e/dml/async_change_log_mirror.rs`

1. Replace the explicit-fence-only assertion with a deadline-based assertion that committed async rows appear without calling `wait_for_async_mirror`.
2. Run it and verify it times out with the current implementation.
3. Add a database-scoped apply advisory lock so worker and explicit fence cannot consume the same logical slot concurrently.
4. Register a dynamic SPI background worker for the current database, with a short bounded poll interval and abnormal-exit restart.
5. Install a lightweight async-only statement trigger that ensures the worker is present after server restart without restoring strict mirror writes.
6. Verify background visibility, rollback exclusion, bounded batches, explicit fence compatibility, and flush-owned-delete suppression.

### Task 3: Safe async teardown

**Files:**
- Modify: `crates/pg_koldstore/src/async_mirror/lifecycle.rs`
- Modify: `crates/koldstore-migrate/src/sql/capture.rs`
- Test: `crates/koldstore-migrate/tests/change_log_mirror_dml.rs`
- Test: `tests/e2e/dml/async_change_log_mirror.rs`

1. Add failing tests that teardown includes the async worker-kick trigger and that infrastructure cleanup is rejected while async tables remain active.
2. Add `koldstore.disable_async_mirror()` as a security-definer SQL function.
3. Serialize cleanup with the apply lock, drop slot before publication, remove durable state, and let the worker exit after observing the missing slot.
4. Verify cleanup planning and active-table safety.

### Task 4: One DML contract, two modes

**Files:**
- Modify: `tests/e2e/dml/change_log_mirror.rs`
- Modify: `tests/e2e/dml/async_change_log_mirror.rs`

1. Introduce a small strict/async test-mode enum and a mode-aware mirror synchronization helper.
2. Run the existing insert/update/delete/reinsert/rollback, PK-guard, counter, and bulk latest-state scenarios once per mode.
3. Keep async-only assertions for publication columns, trigger shape, background lag, slot state, and flush replication-origin suppression.
4. Run both E2E binaries sequentially against local pgrx PostgreSQL.

### Task 5: Documentation and final verification

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture/mirror-capture-modes.md`
- Modify: `docs/architecture/dml-table.md`
- Modify: `docs/architecture/manage-table.md`
- Modify: `docs/decisions/003-optional-async-mirror-capture.md`
- Modify: `docs/benchmarks/README.md`
- Modify: `tests/storage/README.md`

1. Replace manual publication/slot instructions with the one unavoidable `wal_level=logical` prerequisite.
2. Document worker cadence, restart kick, explicit fence semantics, slot serialization, failure behavior, and teardown.
3. Preserve benchmark separation between foreground DML and asynchronous catch-up.
4. Run focused Rust tests, cargo checks, strict and async E2E, documentation link validation, and `git diff --check`.
