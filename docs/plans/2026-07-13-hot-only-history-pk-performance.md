# Hot-Only HISTORY PK Performance Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development to implement this plan task-by-task.

**Goal:** Identify and reduce the managed, pre-flush `HISTORY` primary-key lookup overhead relative to an unmanaged heap lookup without weakening hot+cold read correctness.

**Architecture:** Measure the existing `KoldMergeScan` planning and execution path, then optimize only the catalog/cache operation proven to dominate hot-only point lookups. Keep PostgreSQL integration in `pg_koldstore`, preserve fail-closed managed reads, and retain the native hot child plan when no cold segments exist.

**Tech Stack:** Rust, pgrx, PostgreSQL 16, local pgrx-managed PostgreSQL, HammerDB read microbenchmark.

---

### Task 1: Reproduce and localize the overhead

**Files:**
- Inspect: `docs/benchmarks/hammerdb.md`
- Inspect: `scripts/hammerdb/read_bench.py`
- Inspect: `crates/pg_koldstore/src/merge_scan/pg.rs`
- Inspect: `crates/pg_koldstore/src/catalog/cache.rs`

1. Preserve the recorded baseline and hot-only timings and plans.
2. Run a focused local managed-table point-lookup benchmark when the current pgrx instance is available.
3. Compare repeated lookup timings with catalog/cache behavior and identify the repeated work.

### Task 2: Add a failing cache regression test

**Files:**
- Modify: `crates/pg_koldstore/src/catalog/cache.rs`
- Test: the narrowest pure-Rust or `#[pg_test]` cache test supported by the boundary

1. Add a test proving that an absent pre-flush manifest is a cacheable lookup result.
2. Run the narrow test and verify it fails because the cache currently stores only positive manifest results.

### Task 3: Cache the hot-only manifest absence

**Files:**
- Modify: `crates/pg_koldstore/src/catalog/cache.rs`

1. Represent cache entries as present-or-absent results for a table/predicate-column key.
2. Store `None` after a successful catalog lookup that finds no published manifest.
3. Preserve existing invalidation on table lifecycle/flush changes.
4. Run the narrow test and relevant pgrx scan tests.

### Task 4: Measure and document the result

**Files:**
- Modify if new evidence is produced: `docs/benchmarks/hammerdb.md`

1. Re-run the focused before/after point-lookup benchmark on local pgrx PostgreSQL.
2. Confirm `EXPLAIN` still shows `KoldMergeScan`, `Hot Plan: Index Scan`, and zero opened cold segments.
3. Run `cargo check` and focused `cargo pgrx test` verification.
4. Record the root cause, before/after numbers, and any remaining irreducible custom-scan overhead.
