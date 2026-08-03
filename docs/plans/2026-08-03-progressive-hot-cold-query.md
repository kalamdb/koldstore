# Progressive Hot–Cold Query Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Land a maintainable path portfolio with real pathkeys and costing, then progressively replace mixed SPI JSON merge with native hot cursors and bound-gated cold access — without regressing locked hot-only / exact-PK paths.

**Architecture:** PostgreSQL chooses among KoldStore `CustomPath` strategies (`KoldPathStrategy`). Pure comparison/frontier logic lives in `koldstore-merge`; PG glue lives under `pg_koldstore::merge_scan::pg::path_strategy` and related modules. Hot access prefers the native child; SPI JSON is retired as each shape is covered. APIs take `scope_key` for forward compatibility; product scoping is out of scope.

**Tech Stack:** Rust, pgrx CustomPath/CustomScan, PostgreSQL planner hooks, existing `koldstore.cold_segment_index` / catalogs, later `cold_segment_order_index`.

**Design:** [2026-08-03-progressive-hot-cold-query-design.md](2026-08-03-progressive-hot-cold-query-design.md)

**Skills:** @cargo-pgrx for `cargo pgrx test` / pg_test boundaries; do not weaken e2e assertions to hide scan bugs.

**Constraint:** Uncommitted WIP on `feature/wal-only-capture-71` (EXPLAIN/scan docs) may overlap. Either finish/commit that WIP on this branch before Phase 1, or stash/carry it onto the implementation branch — do not strand it.

---

## Phase 0: Prep and baseline

### Task 0.1: Capture baseline plans and tests

**Files:**
- Read: `docs/architecture/scanning-table.md`
- Read: `crates/pg_koldstore/src/merge_scan/pg.rs` (`set_rel_pathlist`)
- Read: `tests/e2e/merge/user_scope_cold_pruning.rs`
- Read: `crates/pg_koldstore/src/pg_tests/scan.inc.rs`

**Step 1:** Note current contracts to preserve:
- Empty manifest / cold-proven-empty → native paths, no `KoldMergeScan`
- Exact-PK hot hit → no Parquet / mirror init
- Cold-capable → `KoldMergeScan` (today unordered)

**Step 2:** Run focused baseline (adjust if local pgrx port differs):

```bash
cargo test -p koldstore-merge --lib
cargo pgrx test -p pg_koldstore pg15 -- --test scan
```

Expected: current suite green (or known WIP failures documented).

**Step 3:** Commit only if user requests; otherwise leave baseline notes in the PR description.

---

## Phase 1: Strategy module + path portfolio + pathkeys

### Task 1.1: Add `KoldPathStrategy` in `koldstore-merge` (PG-free)

**Files:**
- Create: `crates/koldstore-merge/src/scan/strategy.rs`
- Modify: `crates/koldstore-merge/src/scan/mod.rs`
- Test: `crates/koldstore-merge/src/scan/strategy.rs` (inline `#[cfg(test)]`) or `tests/` under that crate if preferred

**Step 1: Write failing unit tests** for strategy classification helpers, e.g.:
- supported order column + PK → `OrderedProgressive` candidate
- mutable/unknown order → `GeneralMerge`
- exact full PK equality → `ExactPrimaryKey`
- default `scope_key` is `""` on specs

**Step 2: Run tests — expect fail**

```bash
cargo test -p koldstore-merge strategy --lib
```

**Step 3: Implement**

```rust
//! Path strategy identity for managed hot/cold reads.
//!
//! Owns which progressive or fallback execution shape a CustomPath represents.
//! PostgreSQL pathkeys and CustomScan FFI stay in `pg_koldstore`.

/// Planner/executor strategy for a KoldMergeScan path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KoldPathStrategy {
    ExactPrimaryKey,
    ProvenHotOnly,
    UnorderedHotFirst,
    OrderedProgressive(OrderedPathSpec),
    GeneralMerge,
}

/// Immutable order identity advertised by an ordered progressive path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderedPathSpec {
    pub sort_order_id: i32,
    pub leading_column_id: i16,
    pub primary_key_columns: Vec<i16>,
    /// Forward-compat: one query binds one scope; default `""` today.
    pub scope_key: String,
    // directions / nulls / codec version as needed for equality & tests
}
```

Document invariants on the enum and struct with `///`.

**Step 4: Re-run tests — expect pass**

**Step 5: Commit** (when user asks): `feat: add KoldPathStrategy for progressive merge paths`

---

### Task 1.2: Evolve path replacement API for a portfolio

**Files:**
- Modify: `crates/koldstore-merge/src/scan/path.rs`
- Modify: existing path unit tests in that module / crate

**Step 1: Extend tests** so replacement can describe multiple candidate wrappers (ordered + general), each with optional pathkey descriptor id / strategy, instead of a single custom path.

**Step 2: Implement** `PathReplacementDecision` (or successor) that returns a list of portfolio entries: `(strategy, prefers_hot_path_kind, startup_bias, advertises_order)`. Keep `clear_partial_heap_paths` behavior.

**Step 3: `cargo test -p koldstore-merge --lib`**

**Step 4: Commit:** `feat: model multi-path KoldMergeScan portfolio decisions`

---

### Task 1.3: Create `path_strategy` module shell in `pg_koldstore`

**Files:**
- Create: `crates/pg_koldstore/src/merge_scan/pg/path_strategy/mod.rs`
- Create: `crates/pg_koldstore/src/merge_scan/pg/path_strategy/portfolio.rs`
- Create: `crates/pg_koldstore/src/merge_scan/pg/path_strategy/cost.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg.rs` (mod declaration; thin hook)

**Step 1:** Move *calling* of “build custom path from hot child” into `portfolio.rs` with `//!` module docs stating: one place for strategy selection; locked early-returns stay in the hook.

**Step 2:** Initially still emit a single `GeneralMerge` path (behavior-preserving refactor). Wire `scope_key: ""` into private private metadata that will become strategy private data.

**Step 3:**

```bash
cargo pgrx test -p pg_koldstore pg15 -- --test scan
```

Expected: same plans as before (unordered `KoldMergeScan`).

**Step 4: Commit:** `refactor: centralize KoldMergeScan path construction in path_strategy`

---

### Task 1.4: Offer ordered path + copy pathkeys from matching hot child

**Files:**
- Modify: `path_strategy/portfolio.rs`, `cost.rs`
- Modify: `set_rel_pathlist` in `pg.rs` (enumerate useful hot paths, not only cheapest)
- Modify: `plan_custom_path` / custom private data to store `KoldPathStrategy`
- Test: `crates/pg_koldstore/src/pg_tests/scan.inc.rs`
- Test: new e2e under `tests/e2e/merge/` for `ORDER BY id DESC LIMIT n` plan shape

**Step 1: Write failing pg_test / e2e** asserting:
- Supported `ORDER BY` on PK or configured order column → `KoldMergeScan` with no parent `Sort` when planner picks ordered path (or EXPLAIN shows pathkeys / “Output Order”)
- Locked cases unchanged (empty cold → native; PK point lookup)

**Step 2: Implement portfolio:**
- Collect useful native paths (cheapest unordered; index paths whose pathkeys match supported order)
- For each, `add_path` a `CustomPath` with:
  - `path.pathkeys` = hot child’s pathkeys when strategy is `OrderedProgressive`
  - `startup_cost` = hot startup + catalog frontier lookup bias
  - `total_cost` = hot total + expected cold/merge
- Still clear bare heap paths and `partial_pathlist`
- Serialize strategy into custom scan private data for executor/EXPLAIN

**Step 3: Run tests**

```bash
cargo pgrx test -p pg_koldstore pg15
# plus e2e ordered limit plan assertion
```

**Step 4:** Update `docs/architecture/scanning-table.md` planner section: multiple paths via `add_path`, pathkeys preserved.

**Step 5: Commit:** `feat: advertise ordered KoldMergeScan paths with pathkeys`

---

### Checkpoint A — after Phase 1

- [ ] Strategy types live in one PG-free module + one PG portfolio module
- [ ] Planner can choose an ordered custom path (no false orders)
- [ ] Hot-only / exact-PK / empty-manifest contracts green
- [ ] Docs + `///` on new modules
- [ ] No SPI JSON changes required yet (executor may still be general merge)

---

## Phase 2: Native ordered hot cursor (retire SPI JSON for ordered path)

### Task 2.1: Hot cursor trait + native child adapter

**Files:**
- Create: `crates/pg_koldstore/src/merge_scan/pg/hot_cursor.rs`
- Modify: `execute.rs`, `hot.rs`, `emit.rs`, `profile.rs`
- Delete or gate: SPI JSON keyset usage for `OrderedProgressive` in `spi_query.rs` / `keyset.rs` as coverage allows

**Steps:** Implement `HotCursor` with `peek` / `next` over native child slots (typed Datums). Document RLS/winner-resolution invariant. Wire `OrderedProgressive` (even if cold still uses old stream initially) to consume this cursor. EXPLAIN: `Hot Actual Access: Native PostgreSQL Child` (or “Typed Index Cursor”), not `SPI JSON Keyset Scan`.

**Tests:** e2e ordered query no longer requires `to_jsonb` in EXPLAIN; correctness vs heap+cold unchanged.

**Cleanup:** remove dead JSON paging code paths only when no strategy references them; update `scan.inc.rs` assertions that require SPI JSON for mixed merge.

**Commit:** `feat: native hot cursor for ordered progressive merge`

---

### Task 2.2: Adaptive page / lazy production under parent Limit

**Files:** `hot_cursor.rs`, `execute.rs`, e2e top-N

**Steps:** Ensure executor does not drain the full hot relation before returning; parent Limit stops requests. Add regression: large hot+cold table, `ORDER BY … LIMIT 5`, hot rows fetched bounded (profile counter).

**Commit:** `fix: stop eager full-hot drain on ordered limited scans`

---

## Phase 3: Ordered cold frontier + hot-dominance proof

### Task 3.1: Catalog DDL for `cold_segment_order_index`

**Files:**
- Modify: `crates/pg_koldstore/sql/koldstore--0.1.0.sql`
- Flush/publish path that writes segment index today
- Tests for index population on flush

**Steps:** Add table + min/max indexes including `scope_key`. Publish composite bounds on flush (start with PK order and configured segment-order column). Default `scope_key = ''`.

**Commit:** `feat: add cold_segment_order_index for ordered frontiers`

---

### Task 3.2: PG-free frontier comparison

**Files:**
- Create: `crates/koldstore-merge/src/scan/ordered_frontier.rs`
- Unit tests for HotStrictlyWins / ColdMayWinOrTie / ties

**Commit:** `feat: ordered actual-vs-bound frontier comparisons`

---

### Task 3.3: Catalog frontier + progressive merge glue

**Files:**
- Create: `crates/pg_koldstore/src/merge_scan/pg/cold_frontier.rs`
- Create: `crates/koldstore-merge/src/scan/ordered_merge.rs`
- Modify: `execute.rs`, `cold.rs`, `profile.rs`
- E2E: hot-dominates `ORDER BY … LIMIT 5` → 0 Parquet opens

**Steps:** Page order index by `table_oid, scope_key, sort_order_id`. Do not open Parquet until expansion. EXPLAIN skip reason when hot outranks max cold bound. Deferred mirror: zero rows when no cold expansion.

**Cleanup:** do not use newest-first `ColdRowStream` for `OrderedProgressive`.

**Commit:** `feat: bound-gated ordered cold frontier with hot-dominance skip`

---

### Checkpoint B — after Phase 3

- [ ] Hot-dominant top-N: no Sort, 0 Parquet, 0 mirror, small hot fetch
- [ ] Mixed top-N still correct when cold wins
- [ ] Exact-PK non-regression
- [ ] `scanning-table.md` describes progressive path
- [ ] SPI JSON not used for ordered progressive

---

## Phase 4: Row-group expansion + deferred overlay

### Task 4.1: Expand segment → row groups → metadata → payload

**Files:** `cold_frontier.rs`, parquet reader projection, profile counters

**Acceptance:** only competitive row groups open; payload columns late where possible.

### Task 4.2: Batched mirror + hot PK probes for cold candidates

**Files:** `mirror.rs`, ordered merge resolve path

**Acceptance:** no eager full tombstone HashSet on ordered progressive; mirror seq retained for cold resolution.

**Commit(s):** as above.

---

## Phase 5: Unordered hot-first LIMIT

### Task 5.1: `UnorderedHotFirst` strategy

**Files:** `path_strategy/`, `execute.rs`, e2e `LIMIT` without `ORDER BY`

**Behavior:** emit hot winners first; initialize cold only when hot exhausted under parent Limit. Prefer native child. No false pathkeys.

**Commit:** `feat: unordered hot-first path for LIMIT without ORDER BY`

---

## Phase 6+: Later (track only)

- Parquet page index / Bloom on PK miss
- Partial aggregate upper paths
- Join / runtime filter optimizations
- Per-user scope product (manifests per `scope_key`; queries remain single-scope)

---

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| False pathkeys → wrong order | Only advertise orders with proven immutable identity + PG-compatible compare |
| RLS hides hot → cold resurrection | Winner resolution before ExecScan quals; trusted hot cursor when needed |
| Regress hot-only PK latency | Keep locked early returns; dedicated regression e2e/pg_test |
| WIP EXPLAIN conflicts | Finish or stash current branch WIP before portfolio edits |
| Scope feature creep | Plumb `scope_key` only; no partition product in Phases 1–5 |

## Open questions (resolve during Phase 1 if blocking)

- Exact private-data encoding for strategy on `CustomScan` (bytea vs integer tag + side table).
- First ordered column set: PK only vs PK + configured segment-order in the same milestone (prefer both if flush already maintains segment-order stats).
