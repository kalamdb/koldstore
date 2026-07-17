# Async Flush Prune Fence Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the async flush prune race: after manifest publish, block source writers, catch mirror up through a bounded WAL upper LSN, then prune — so concurrent DML cannot lose a newer hot row.

**Architecture:** Keep Parquet upload concurrent with DML. Add only the phase-6 critical section from [async-flush-prune-race](../cases/async-flush-prune-race.md): `SHARE ROW EXCLUSIVE` on the flushed table → durable WAL fence `F1` → bounded apply with skip/no-ack → existing `prune_flushed_hot_rows`. Gate on the **table’s** `mirror_capture_mode = async`; strict stays unchanged.

**Tech Stack:** Rust/pgrx SPI, existing `pg_logical_slot_peek_binary_changes`, pg relation locks, snowflake `SeqId` allocation.

**Out of scope (follow-ups):** phase-5.5 pre-lock catch-up, `max_rows_per_flush`, admission/WAL-byte limits, strict-mode cross-backend seq audit, session-lock lifecycle change, full phase-0 rewrite.

**Spec:** [docs/cases/async-flush-prune-race.md](../cases/async-flush-prune-race.md) — implement phase 6 only.

---

### Task 1: Bounded apply API (no ack, skip, upto_lsn)

**Files:**
- Modify: `crates/pg_koldstore/src/async_mirror/apply.rs`
- Modify: `crates/koldstore-common/src/domain/` (small LSN/seq newtypes if none exist; otherwise keep thin structs in `apply.rs` for v1)
- Test: unit/pg_test covering skip + no-ack (prefer library-level decode/skip tests; SPI pg_test only if needed)

**Why:** A second `apply_available()` during the same flush txn is unsafe: it can acknowledge a checkpoint written by this still-uncommitted transaction. Phase 6 needs a lower-level pass.

1. Add request/outcome shapes (names can match the case doc):

```text
BoundedApplyRequest {
  upper_bound: WalFenceLsn,              // upto_lsn = F1
  skip_through: Option<AppliedWalBoundary>,
  acknowledge_durable_checkpoint: bool,  // false for phase 6
  target_prune_floor: Option<(Oid, PruneSeqFloor)>,
}

BoundedApplyOutcome {
  row_changes: i64,
  last_applied: Option<AppliedWalBoundary>,
}
```

2. Refactor `apply_available()` to call the new path with today’s behavior:
   - `acknowledge_durable_checkpoint = true`
   - `upper_bound = None` / unbounded peek (keep current `NULL` upto_lsn)
   - no skip / no prune floor
3. New path must:
   - pass `upto_lsn` when `upper_bound` is set
   - skip **whole** pgoutput transactions with `end_lsn <= skip_through` (do not reapply; do not allocate new seq)
   - when `acknowledge_durable_checkpoint = false`, still apply mirror writes but **do not** call `record_applied_lsn` / slot advance for this pass’s pending work
   - return exact last decoded commit `end_lsn`; empty pass → keep `skip_through` (never promote to `upper_bound`)
4. Keep existing `lock_apply` xact advisory lock; do not switch to session locks.

**Verify:** existing async apply / flush tests still pass; add one focused test that skipped transactions are not reapplied.

---

### Task 2: Floor-aware sequence allocation for prune fence

**Files:**
- Modify: `crates/koldstore-common/src/domain/snowflake.rs`
- Modify: `crates/pg_koldstore/src/async_mirror/apply.rs` (batch SQL / seq injection for target table only)
- Test: `crates/koldstore-common` unit tests for overflow + monotonicity above floor

**Why:** Applied mutations of the flushed table during phase 6 must get `new_seq > max_seq`, or prune still deletes them.

1. Add overflow-checked `next_id_after(worker_id, floor: i64) -> Result<i64, SnowflakeError>` (or equivalent) that preserves snowflake layout and fails on overflow.
2. During bounded apply with `target_prune_floor = Some((oid, floor))`, allocate target-table row seqs via that API; unrelated async tables keep normal `SNOWFLAKE_ID()` / `next_id`.
3. Do **not** use unchecked `max_seq + row_number`.

**Verify:** unit tests for `floor + 1`, same-ms multiple ids, overflow error.

---

### Task 3: Writer fence + wire into flush finalize

**Files:**
- Modify: `crates/pg_koldstore/src/sql/flush/execute.rs` (after `after_manifest_publish`, before `before_hot_cleanup` / `prune_flushed_hot_rows`)
- Modify: `crates/pg_koldstore/src/sql/flush/spi.rs` (OID-based `LOCK TABLE ... IN SHARE ROW EXCLUSIVE MODE`)
- Possibly: small helper to read table capture mode from managed settings

**Steps (async table only):**

```text
1. LOCK source relation SHARE ROW EXCLUSIVE by table_oid (ONLY physical relation cleanup targets)
2. F1 = pg_current_wal_insert_lsn(); wait/force durability through F1
3. bounded_apply({
     upper_bound: F1,
     skip_through: L0,           // last applied end-LSN from phase-0 in this txn
     acknowledge_durable_checkpoint: false,
     target_prune_floor: (table_oid, max_seq),
   })
4. prune_flushed_hot_rows(max_seq)   // existing atomic CTE
```

**Strict / no-async:** skip steps 1–3; prune as today.

**Phase 0:** keep calling `apply_available()` as today, but **retain** its last applied end-LSN (`L0`) in flush context so phase 6 can skip through it. Minimal change: have `apply_available` / bounded path return `last_applied` and stash it on the flush path.

**Lock timeout:** set a local `lock_timeout` for the fence; on timeout, fail before prune (hot remains authoritative, job retryable). Do not wait forever.

**Verify:** `cargo pgrx test` / existing flush tests green; strict path unchanged.

---

### Task 4: One correctness E2E (the race)

**Files:**
- Create or extend under `tests/e2e/flush/` (reuse failpoints `after_manifest_publish` / `before_hot_cleanup` if present)
- Reference scenario: `update_during_async_flush_prune` in the case doc

**Scenario:**
1. Async-managed table with hot rows; start flush.
2. Pause after manifest publish / before prune fence.
3. Concurrent `UPDATE` of a PK that was selected into the flush watermark; commit.
4. Resume flush through fence + prune.
5. Assert: updated hot row still present with newer values; cold holds old image; mirror has `seq > max_seq` for that PK (not deleted).

Optional second case if cheap: `delete_during_async_flush_prune` (tombstone survives prune).

**Verify:** local pgrx E2E only (`./run-pg-e2e.sh` or targeted flush binary). No Docker requirement.

---

### Task 5: Docs (minimal)

**Files:**
- Modify: `docs/architecture/flushing-table.md` — replace “known gap” with phase-6 fence description
- Modify: `docs/cases/async-flush-prune-race.md` — status → implemented (phase 6); note deferred items
- Modify: `docs/notes.md` — remove the prune-lock todo

Do not expand ADR/caps docs until those follow-ups land.

---

## Deferred (explicit non-goals for this PR)

| Item | Why deferred |
|------|----------------|
| Phase 5.5 pre-lock catch-up | Latency under multi-table WAL load; not required for correctness |
| `max_rows_per_flush` | Bounds lock duration; separate performance work |
| Admission / `max_prune_fence_wal_bytes` | Same |
| Strict seq ordering audit | Separate mode; case already isolates it |
| Full typed LSN module polish | Thin request structs in apply path are enough for v1 |

---

## Success criteria

- Concurrent UPDATE/DELETE during async flush Parquet→prune window cannot drop a newer hot row.
- Strict flush path has no new writer lock / WAL apply.
- No second `apply_available()` ack of the flush txn’s pending checkpoint.
- One E2E proves the race is closed.
