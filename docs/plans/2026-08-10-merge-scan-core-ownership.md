# Merge-Scan Core Ownership and Performance Implementation Plan

> **For Claude:** REQUIRED SUB-SKILLS: use `test-driven-development`,
> `rust-skills`, `cargo-pgrx`, `performance-optimization`, and
> `verification-before-completion` while executing this plan task-by-task.

**Goal:** Move PostgreSQL-free merge decisions out of `pg_koldstore`, remove
duplicated/legacy adapter logic, and reduce merge-path allocations without
regressing native hot-only reads or hot+cold correctness.

**Architecture:** `koldstore-merge` owns winner masking, ordered-frontier
selection, row-group set operations, and cold projection planning.
`pg_koldstore::merge_scan::pg` remains a thin owner of PostgreSQL hooks, SPI,
catalog access, Parquet opening, tuple conversion, and EXPLAIN. Changes land in
small test-gated slices and retain the existing progressive execution model.

**Tech Stack:** Rust, pgrx/PostgreSQL CustomScan, Arrow/Parquet, Criterion,
local pgrx PostgreSQL.

**Existing WIP:** Continue on `codex/stabilize-wal-flush-workers`; preserve the
pre-existing edits in `cold.rs`, `execute.rs`, and `profile.rs`. Do not switch
branches or strand those changes.

**Locked paths:** Do not change the empty-manifest planner return,
`cold_side_proven_empty`, `EmitPath::HotChild`, the exact-PK hot probe, or the
rule that every potentially satisfiable cold query uses KoldMergeScan.

---

### Task 1: Make ordered row-group selection conservative and reusable

**Files:**

- Modify: `crates/koldstore-merge/src/scan/ordered_merge.rs`
- Modify: `crates/koldstore-merge/src/scan/mod.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg/cold.rs`

**Step 1: Write failing pure-Rust tests**

Add tests proving that mismatched min/max catalog arrays retain unmatched row
groups as unknown, and that intersecting planned and competitive row groups
preserves planned order without repeated linear `contains` scans.

**Step 2: Verify RED**

Run:

```bash
cargo test -p koldstore-merge scan::ordered_merge
```

Expected: the mismatched-array case drops an unknown row group and the new
intersection API is not implemented.

**Step 3: Implement the minimal core helpers**

Iterate to the maximum bound-array length and treat a missing directional bound
as unknown/competitive. Add an allocation-conscious intersection helper using
sorted membership checks, then replace the adapter's quadratic
`selected.contains` loop.

**Step 4: Verify GREEN**

Run the focused tests and `cargo test -p koldstore-merge`.

---

### Task 2: Centralize tombstone masking in the merge resolver

**Files:**

- Modify: `crates/koldstore-merge/src/core/resolver.rs`
- Modify: `crates/koldstore-merge/tests/resolver.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg/execute.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg/mirror.rs`

**Step 1: Extend resolver characterization tests**

Cover immediate and deferred tombstone masks, duplicate masked keys, live hot
winners followed by masking, and seen-key cap behavior.

**Step 2: Keep the tests green while refactoring**

Make `NewestFirstWinnerResolver` the single masking authority. Count overlay
matches without allocating a replacement cold vector, apply deferred masks to
the resolver before resolving the batch, and delete the adapter-owned
`filter_cold_rows_with_overlay` implementation.

**Step 3: Verify**

Run resolver tests, the full merge crate tests, and `cargo check -p pg_koldstore`.

---

### Task 3: Reduce winner-resolution cloning and duplicate batch logic

**Files:**

- Modify: `crates/koldstore-merge/src/core/resolver.rs`
- Modify: `crates/koldstore-merge/tests/resolver.rs`
- Modify: `benchmarks/benches/extension_serialization.rs` only if a separate
  streaming benchmark is needed

**Step 1: Record the before measurement**

Use the existing 10k hot + 10k cold Criterion case. Current median baseline:
`4.224 ms` for `deduplicate_hot_and_cold_by_primary_key`.

**Step 2: Add equivalence coverage**

Prove borrowed and owned resolution select identical winners, tie-breaking,
tombstones, and canonical PK output.

**Step 3: Refactor after the characterization gate**

Resolve borrowed inputs through borrowed candidates so only final winner row
images are cloned. Pre-size winner maps. Consolidate duplicated hot/cold batch
loops through a monomorphized internal helper without dynamic dispatch.

**Step 4: Measure after**

Re-run the same Criterion filter and report the confidence interval. Keep the
change only if it is neutral or faster and tests remain green.

---

### Task 4: Move cold compete/body projection planning into `koldstore-merge`

**Files:**

- Create: `crates/koldstore-merge/src/scan/projection.rs`
- Modify: `crates/koldstore-merge/src/scan/mod.rs`
- Modify: `crates/koldstore-merge/src/lib.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg/cold.rs`

**Step 1: Write failing projection tests**

Cover leading-PK deduplication, composite PKs, narrow projections that disable
late materialization, stable body order, and body hydration's `body + PK`
projection.

**Step 2: Verify RED**

Run the new module tests; expect the projection planner assertions to fail
against a minimal scaffold.

**Step 3: Implement and wire the core plan**

Represent `Full`, `Compete`, and `Body` column ownership with one typed plan.
Keep actual Parquet opens and profiles in `pg_koldstore`, but remove the
duplicated set construction from `ColdRowStream`.

**Step 4: Verify GREEN**

Run merge tests and the pg crate check.

---

### Task 5: Thin PostgreSQL adapter duplication and argument plumbing

**Files:**

- Modify: `crates/pg_koldstore/src/merge_scan/pg/hot_cursor.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg/literals.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg/cold.rs`

**Step 1:** Reuse the single existing relabel-expression helper instead of the
byte-for-byte duplicate in `hot_cursor.rs`.

**Step 2:** Replace newly added `too_many_arguments` suppressions with focused
request/context structs when doing so shortens call sites and does not add
runtime indirection.

**Step 3:** Run `cargo check -p pg_koldstore` after each adapter cleanup.

---

### Task 6: PostgreSQL and performance verification

**Files:**

- Modify only if behavior changes: `docs/architecture/scanning-table.md`
- Test existing merge coverage under `crates/pg_koldstore/src/pg_tests/` and
  `tests/e2e/merge/`

**Step 1: Static gates**

```bash
cargo fmt --all
cargo fmt --all -- --check
cargo clippy -p koldstore-merge --all-targets -- -D warnings
cargo check -p pg_koldstore
```

**Step 2: Pure and in-server behavior**

```bash
cargo test -p koldstore-merge
cargo pgrx test -p pg_koldstore pg18 <focused merge tests>
```

Verify hot PK hits, empty/proven-empty cold paths, ordered hot-dominant LIMIT,
cold-winning LIMIT, overlap/tombstone masking, parameters, joins, and rescan.

**Step 3: Performance gate**

Re-run the Criterion deduplication benchmark and existing pg benches for native
hot, managed hot, and cold lifecycle where feasible. Do not claim a speedup
without before/after measurements.

**Step 4: Final review**

Inspect the diff for unrelated WIP, unsafe-boundary expansion, public API churn,
dead exports, outdated comments, and architecture-contract changes. Update
`scanning-table.md` only if user-visible behavior or invariants changed.
