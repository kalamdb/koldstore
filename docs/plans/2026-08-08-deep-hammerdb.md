# Deep HammerDB Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ship opt-in deep HammerDB (profiles + manage sets + mid-run proofs + CH modes) without changing weekly smoke CI.

**Architecture:** Extend `scripts/hammerdb/` with a separate `run-deep.sh` driver; keep `run.sh` as the smoke path. Dispatch-only GitHub workflow wires the deep driver.

**Tech Stack:** bash, Python 3, HammerDB Tcl, psql/pgrx, GitHub Actions `workflow_dispatch`

---

### Task 1: Profiles helper

**Files:**
- Create: `scripts/hammerdb/profiles.sh`

**Step 1:** Export `resolve_hammerdb_profile` that sets `WAREHOUSES`, `VIRTUAL_USERS`, `DURATION`, `READ_ITERS`, `RAMPUP` from profile name or custom env overrides.

**Step 2:** Source-check with `bash -n`.

### Task 2: Manage policy SQL

**Files:**
- Create: `scripts/hammerdb/manage_policy.sql`
- Keep: `scripts/hammerdb/manage_history.sql` (smoke compatibility)

**Step 1:** Parameterize `:STORAGE_ROOT` and `:MANAGE_SET`.
**Step 2:** Always prepare + manage `history`. Best-effort manage `order_line` for `append`/`broad`, and `orders` for `broad`. Log skips.

### Task 3: CH schema + queries + runner

**Files:**
- Create: `scripts/hammerdb/ch_schema.sql`
- Create: `scripts/hammerdb/ch_queries.py`
- Create: `scripts/hammerdb/ch_runner.py`

**Step 1:** Create/load `region`/`nation`/`supplier` compatible with Citus CH queries.
**Step 2:** Port 22 queries (attribute Citus/CH-benCHmark).
**Step 3:** Runner supports `--mode after|concurrent|only`, duration, threads, JSON latency out.

### Task 4: Proofs helper

**Files:**
- Create: `scripts/hammerdb/proofs.py`
- Reuse: `scripts/hammerdb/read_bench.py` patterns

**Step 1:** Flush listed tables; require active cold segments.
**Step 2:** EXPLAIN managed HISTORY PK → merge + opened≥1; customer stays native.

### Task 5: Deep driver

**Files:**
- Create: `scripts/hammerdb/run-deep.sh`

**Step 1:** CLI: `--profile`, `--manage-set`, `--ch`, PG version.
**Step 2:** Background timed run + mid-run flush/proof; CH per mode; summary JSON.

### Task 6: Dispatch workflow + docs

**Files:**
- Create: `.github/workflows/deep-hammerdb.yml`
- Modify: `scripts/hammerdb/README.md`
- Modify: `docs/benchmarks/hammerdb.md`
- Modify: `docs/benchmarks/README.md` (link)

**Step 1:** workflow_dispatch inputs matching design; never schedule.
**Step 2:** Document profiles, depth knobs, claim wording, “not in default CI”.

### Task 7: Verify

**Step 1:** `bash -n` on new shell scripts.
**Step 2:** `python3 -m py_compile` on new Python.
**Step 3:** Confirm weekly workflow still calls `run.sh` / readiness wrapper only.
