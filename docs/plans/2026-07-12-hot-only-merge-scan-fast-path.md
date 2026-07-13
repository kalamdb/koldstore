# Hot-Only KoldMergeScan Fast Path Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make a warmed managed-table point lookup with no published cold segments run within 10% of the equivalent native PostgreSQL lookup, without weakening hot/cold correctness.

**Architecture:** Keep `KoldMergeScan` as a thin correctness wrapper in the first delivery. Add a typed, backend-local managed-scan cache that stores both positive and negative entries, invalidate it across backends with PostgreSQL relcache invalidation, and decide `HotChild` before relation/schema/projection/manifest preparation. The hot-only executor then delegates to the native child plan; the existing catalog, Parquet, mirror, and merge setup remains lazy and runs only when published cold data may exist.

**Tech Stack:** Rust, pgrx 0.19.1, PostgreSQL 15-18 Custom Scan and relcache invalidation APIs, local pgrx-managed PostgreSQL, `tokio-postgres`, `EXPLAIN (ANALYZE, FORMAT JSON)`.

---

## Diagnosis and design constraints

The current hot-only path eventually delegates tuple production to PostgreSQL's native child, but it proves that cold is absent too late.

For each pre-flush managed point lookup, the current path performs:

1. `set_rel_pathlist` loads the managed-table snapshot and calls `cached_manifest_segment_stats(table_oid, &[])`.
2. Because `None` is not cached, the no-manifest result executes SPI again on every plan.
3. `PlanCustomPath` clones primary-key metadata, serializes a `MergeScanPlan`, and stores it in `custom_private`.
4. `BeginCustomScan` deserializes that plan even though the result is unused.
5. It resolves the qualified relation name through uncached SPI.
6. It loads cached migration metadata and the managed snapshot.
7. It walks target lists and quals and allocates projection/filter vectors.
8. `load_cold_rows_for_merge` loads and decodes the active schema through uncached SPI.
9. It calls the manifest/segment query again with a predicate-column cache key; before flush this is another uncached `None`.
10. Only then does it allocate `ScanMemory`, install `HotChild`, call the native child for the tuple, and copy the child slot into the custom result slot.

The no-cold cache miss, relation lookup, and active-schema lookup are the highest-priority costs. Tuple-slot copying and thread-local state dispatch are lower-priority costs to measure after the SPI work is removed.

The comparison harness also needs correction: an unmanaged baseline relation currently executes an uncached "is this managed?" SPI lookup during every plan because unmanaged `None` is not cached. Report both ad-hoc and prepared execution, and treat the managed/native ratio as the release gate.

### Required invariants

- A query must never silently return hot-only data when a committed published cold segment may be visible.
- A prepared statement planned before a flush must see cold rows on its first execution after that flush commits.
- A flush or migration in backend B must invalidate scan metadata and applicable saved plans in backend A.
- Before the first flush, the warmed hot-only path performs no object-store work and no SPI/catalog query.
- The first delivery preserves `Custom Scan (KoldMergeScan)` in `EXPLAIN` for managed reads.
- Tests under `tests/` use local pgrx PostgreSQL only. Docker remains a packaging smoke test.

## Performance acceptance criteria

- Warm prepared point lookup: managed p50 latency <= 1.10x native p50 latency.
- Warm ad-hoc point lookup: managed p50 latency <= 1.15x native p50 latency.
- With the supplied 674 microseconds/op native reference, the primary target is <= 741 microseconds/op managed (about >= 1,349 ops/s); <= 700 microseconds/op is the stretch target.
- The result is based on at least 1,000 measured lookups after at least 100 warmups, repeated three times and reported as the median run.
- A deterministic integration assertion proves zero hot-path SPI probes after warmup; timing alone is not the correctness gate.

## Task 1: Add phase-level hot-path observability

**Files:**

- Modify: `crates/pg_koldstore/src/merge_scan/pg/profile.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg.rs`
- Modify: `tests/e2e/common/assertions.rs`
- Create: `tests/e2e/merge/hot_only_fast_path.rs`
- Modify: `tests/e2e/Cargo.toml`

**Step 1: Write a failing local pgrx integration test**

Create a managed table, insert one hot row without flushing, warm the same backend once, then run `EXPLAIN (ANALYZE, FORMAT TEXT)` for a primary-key lookup. Require properties equivalent to:

```text
Hot Plan: Index Scan
Execution Mode: hot-child
Cold Decision: no-published-segments
Catalog Probes: 0
Cold segments: considered=0, ... opened=0
Parquet segment: none
```

The test should fail initially because execution mode, decision source, and catalog-probe counts are not exposed.

**Step 2: Run the test to verify it fails**

Run:

```bash
KOLDSTORE_E2E_NEXTEST_FILTER='binary(hot_only_fast_path)' scripts/run-pg-e2e.sh 16
```

Expected: FAIL because the new `EXPLAIN` properties are absent.

**Step 3: Add a lightweight setup profile**

Add a focused type rather than more unrelated fields on `ColdReadProfile`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ScanExecutionMode {
    HotChild,
    Merge,
}

#[derive(Debug, Clone)]
pub(super) struct ScanSetupProfile {
    pub(super) mode: ScanExecutionMode,
    pub(super) cold_decision: &'static str,
    pub(super) catalog_probes: u32,
    pub(super) setup_ms: f64,
}
```

Store it in `ExplainScanMeta` and render it only through `ExplainCustomScan`. Keep ordinary query logging disabled.

**Step 4: Count only merge-scan-owned catalog probes**

Increment the counter at the extension's scan cache/SPI boundary, not globally in all SPI helpers. This makes the assertion deterministic and avoids affecting unrelated SQL.

**Step 5: Run the test to verify it passes**

Run the command from Step 2. Expected: PASS.

**Step 6: Commit**

```bash
git add crates/pg_koldstore/src/merge_scan/pg/profile.rs \
  crates/pg_koldstore/src/merge_scan/pg.rs \
  tests/e2e/common/assertions.rs tests/e2e/merge/hot_only_fast_path.rs \
  tests/e2e/Cargo.toml
git commit -m "test: expose merge scan hot-path setup cost"
```

## Task 2: Model managed scan eligibility and cache negative entries

**Files:**

- Modify: `crates/koldstore-catalog/src/cache.rs`
- Modify: `crates/koldstore-catalog/src/decode.rs`
- Modify: `crates/koldstore-catalog/src/queries.rs`
- Modify: `crates/koldstore-catalog/src/lib.rs`
- Modify: `crates/koldstore-catalog/tests/schema_versions.rs`
- Modify: `crates/pg_koldstore/src/catalog/cache.rs`

**Step 1: Write failing pure Rust cache tests**

Cover all three states and invalidation:

```rust
pub enum ManagedScanEligibility {
    Unmanaged,
    Managed {
        snapshot: Arc<ManagedTableSnapshot>,
        cold_visibility: ColdVisibility,
    },
}

pub enum ColdVisibility {
    NoPublishedSegments,
    Published {
        manifest_generation: String,
    },
}
```

Test that `Unmanaged` and `NoPublishedSegments` are stored and returned, not represented as an uncached `None`. Test `invalidate(table_oid)` and `clear()`.

**Step 2: Run the tests to verify they fail**

Run:

```bash
cargo test -p koldstore-catalog cache
```

Expected: FAIL because the eligibility types/cache do not exist.

**Step 3: Add one compact eligibility query**

Extend the managed snapshot query or add `plan_managed_scan_eligibility()` so its first execution returns:

- unmanaged vs managed;
- the existing managed snapshot fields;
- whether a non-placeholder manifest generation exists and at least one segment is `published`;
- the newest manifest generation when cold is visible.

Do not aggregate object paths or segment statistics in this query. Use `EXISTS` for published segment presence. Full segment metadata remains owned by `plan_in_sync_manifest_scan_context()` and is loaded only on the cold path.

**Step 4: Implement typed decoding and the backend-local cache**

Replace the positive-only `MANAGED_TABLE_CACHE` lookup used by planning with an eligibility cache keyed by the typed table identifier. Reuse the `Arc<ManagedTableSnapshot>`; do not clone its vectors on cache hits.

If adding a table-OID newtype, place the PostgreSQL-free `TableOid(u32)` in `koldstore-common`; keep `pg_sys::Oid` conversion in `pg_koldstore`.

**Step 5: Add a recording-loader test for negative caching**

Use a loader closure or trait that increments a counter. Two lookups for an unmanaged OID and two lookups for a managed/no-cold OID must each call the loader exactly once.

**Step 6: Run focused tests**

```bash
cargo test -p koldstore-catalog
cargo test -p pg_koldstore --no-default-features
```

Expected: PASS.

**Step 7: Commit**

```bash
git add crates/koldstore-catalog crates/pg_koldstore/src/catalog/cache.rs
git commit -m "perf: cache managed scan eligibility including hot-only state"
```

## Task 3: Make cache invalidation transactional and cross-backend

**Files:**

- Modify: `crates/pg_koldstore/src/catalog/cache.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg.rs`
- Modify: `crates/pg_koldstore/src/sql/flush/execute.rs`
- Modify: `crates/pg_koldstore/src/sql/migrate_pg.rs`
- Modify: `tests/e2e/merge/hot_only_fast_path.rs`

**Step 1: Write the failing two-backend prepared-plan test**

Use two independent `tokio-postgres` connections:

1. Backend A prepares and executes a managed PK query while all rows are hot.
2. Backend A executes again to warm `NoPublishedSegments`.
3. Backend B flushes the queried row to a published cold segment and commits.
4. Backend A executes the same prepared statement without reconnecting.
5. Assert the row is still returned and `EXPLAIN EXECUTE` now uses the cold-capable state.

Also test abort: a flush transaction that rolls back must not make backend A observe a published-cold state.

**Step 2: Run the test to verify it fails**

Run:

```bash
KOLDSTORE_E2E_NEXTEST_FILTER='binary(hot_only_fast_path)' scripts/run-pg-e2e.sh 16
```

Expected: FAIL because backend A retains backend-local cache state.

**Step 3: Split local eviction from invalidation publication**

Provide two explicit operations:

```rust
pub fn invalidate_local_table(table_oid: pg_sys::Oid);
pub unsafe fn publish_table_invalidation(table_oid: pg_sys::Oid);
```

The callback must only evict local state; it must never publish another message.

**Step 4: Register a relcache callback once during extension initialization**

Use `CacheRegisterRelcacheCallback`. The callback receives a relation OID; invalidate one table for a valid OID and all extension caches for `InvalidOid`. Keep it panic-free and limit it to cache eviction.

**Step 5: Publish relcache invalidation at visibility-changing boundaries**

After manifest/catalog publication in flush and after manage, demigrate, or schema-version changes, call `CacheInvalidateRelcacheByRelid(table_oid)`. Keep the existing local invalidation behavior. PostgreSQL queues relcache invalidation transactionally and its plan cache callback invalidates saved plans that reference the relation.

**Step 6: Run the two-backend test and relevant lifecycle tests**

```bash
KOLDSTORE_E2E_NEXTEST_FILTER='binary(hot_only_fast_path)' scripts/run-pg-e2e.sh 16
KOLDSTORE_E2E_NEXTEST_FILTER='binary(merge_scan_results)' scripts/run-pg-e2e.sh 16
```

Expected: PASS, including prepared execution after cross-backend flush.

**Step 7: Commit**

```bash
git add crates/pg_koldstore/src/catalog/cache.rs \
  crates/pg_koldstore/src/merge_scan/pg.rs \
  crates/pg_koldstore/src/sql/flush/execute.rs \
  crates/pg_koldstore/src/sql/migrate_pg.rs \
  tests/e2e/merge/hot_only_fast_path.rs
git commit -m "fix: invalidate merge scan state across postgres backends"
```

## Task 4: Branch to the native child before any cold setup

**Files:**

- Modify: `crates/pg_koldstore/src/merge_scan/pg.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg/cold.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg/profile.rs`
- Modify: `crates/pg_koldstore/tests/merge_scan_explain.rs`
- Modify: `tests/e2e/merge/hot_only_fast_path.rs`

**Step 1: Tighten the failing hot-only assertion**

After one warmup, require:

- `Execution Mode: hot-child`;
- `Cold Decision: eligibility-cache-hit/no-published-segments`;
- `Catalog Probes: 0`;
- no active-schema, manifest-stats, mirror, or object-store work;
- native `Index Scan` child for a PK equality;
- correct results for literals and prepared parameters.

**Step 2: Remove segment-stat loading from `set_rel_pathlist`**

The managed path replaces all heap-only final paths, so its segment-derived cost currently cannot make a competing final path win. Use the compact eligibility state once, keep the native child cost, and remove the planner call to `cached_manifest_segment_stats(table_oid, &[])`.

**Step 3: Make `BeginCustomScan` decide immediately**

Structure it as:

```rust
let eligibility = cached_managed_scan_eligibility(table_oid)?;
if eligibility.is_managed_hot_only() {
    require_hot_child(node)?;
    install_hot_child_state(node, setup_profile);
    return;
}
begin_cold_capable_scan(node, estate, eligibility)?;
```

The first branch must run before:

- qualified relation-name SPI;
- migration catalog loading;
- target-list/projection allocation;
- residual-qual conversion;
- active schema lookup;
- manifest segment-stat loading;
- `ScanMemory` creation;
- mirror overlay loading.

**Step 4: Remove unused plan serialization work**

`deserialize_custom_private(plan)` is currently assigned to `_planned` and ignored. Stop serializing/deserializing the JSON `MergeScanPlan` in PostgreSQL glue unless a cold-path consumer is added in the same change. Keep the PostgreSQL-free `MergeScanPlan` domain API if other tests/callers use it.

**Step 5: Preserve the documented custom-scan slot contract**

Continue filling and returning `ps_ResultTupleSlot` through the existing slot copy for this task. Do not return the child's slot directly until tuple-descriptor compatibility is proven for joins, projections, rescans, and all supported PostgreSQL versions.

**Step 6: Run focused verification**

```bash
cargo test -p koldstore-merge
cargo test -p pg_koldstore --no-default-features
KOLDSTORE_E2E_NEXTEST_FILTER='binary(hot_only_fast_path)' scripts/run-pg-e2e.sh 16
KOLDSTORE_E2E_NEXTEST_FILTER='binary(merge_scan_results)' scripts/run-pg-e2e.sh 16
KOLDSTORE_E2E_NEXTEST_FILTER='binary(merge_scan_outage)' scripts/run-pg-e2e.sh 16
```

Expected: all PASS; hot-only reports zero warmed probes, while post-flush and outage behavior remain unchanged.

**Step 7: Commit**

```bash
git add crates/pg_koldstore/src/merge_scan \
  crates/pg_koldstore/tests/merge_scan_explain.rs \
  tests/e2e/merge/hot_only_fast_path.rs
git commit -m "perf: enter native hot child before cold scan setup"
```

## Checkpoint 1: Verify the high-value fix before lower-level executor work

Run the improved benchmark from Task 5. If both latency-ratio targets pass, skip Task 6. Avoid adding unsafe custom-state machinery without measured need.

## Task 5: Repair the hot-only benchmark methodology and add a regression gate

**Files:**

- Modify: `tests/storage/pg_vs_koldstore.rs`
- Modify: `tests/storage/README.md`
- Modify: `docs/benchmarks.md`
- Modify: `docs/performance.md`

**Step 1: Add warmup and sufficient samples**

Replace the fixed 20-query loop with configurable defaults of at least 100 warmups and 1,000 measurements. Record per-operation durations and report p50, p95, mean, and ops/s. Repeat the pair three times and use the median run.

**Step 2: Measure two protocols**

Report separately:

1. ad-hoc/unnamed execution, which includes repeated planning;
2. a server-side prepared statement, which isolates warmed execution and exercises cached-plan invalidation.

Use the same selected columns and PK value for baseline and managed tables. Alternate or deterministically shuffle baseline/managed order per repetition to reduce ordering bias.

**Step 3: Expose baseline hook state**

Document that the baseline is an unmanaged heap in a backend where the extension hook is loaded. Assert the second baseline plan uses the negative eligibility cache, so the comparison no longer includes repeated unmanaged SPI probes.

**Step 4: Add ratio evaluation without making noisy CI fail**

The harness should print PASS/FAIL against the 1.10 prepared and 1.15 ad-hoc targets. Keep absolute timing as a local release benchmark rather than a default CI failure. The deterministic zero-probe integration test remains the CI guard.

**Step 5: Run a release-profile comparison three times**

```bash
KOLDSTORE_STORAGE_ROWS=100000 \
KOLDSTORE_STORAGE_HOT_LIMIT=10000 \
scripts/run-storage-comparison.sh
```

Expected: managed prepared p50 <= 1.10x baseline and managed ad-hoc p50 <= 1.15x baseline. Preserve the raw three-run results in the PR description.

**Step 6: Commit**

```bash
git add tests/storage/pg_vs_koldstore.rs tests/storage/README.md \
  docs/benchmarks.md docs/performance.md
git commit -m "bench: gate warmed hot-only merge scan overhead"
```

## Task 6: Contingent executor-state optimization

Only execute this task if Task 5 still exceeds either ratio and profiling attributes meaningful time to custom executor dispatch/slot handling.

**Files:**

- Modify: `crates/pg_koldstore/src/merge_scan/pg.rs`
- Modify: `tests/memory/merge_scan_leak.rs`
- Modify: `tests/e2e/merge/hot_only_fast_path.rs`

**Step 1: Add a focused dispatch benchmark or counter**

Measure TLS `HashMap` lookup, `RefCell` borrow, interrupt check, child `ExecProcNode`, and result-slot copy separately enough to identify the remaining cost. Do not infer it from total latency.

**Step 2: Embed typed state in a larger `CustomScanState`**

Use a `#[repr(C)]` wrapper whose first field is `pg_sys::CustomScanState`, as supported by the Custom Scan API. Explicitly initialize and drop Rust-owned fields; do not rely on `palloc0` producing a valid Rust `Option`, `Vec`, or enum. Keep PostgreSQL Datum ownership in the scan memory context.

**Step 3: Remove the thread-local node-pointer map**

Read the execution mode directly from the embedded state in `ExecCustomScan`, `ReScanCustomScan`, `EndCustomScan`, and `ExplainCustomScan`.

**Step 4: Verify rescans, errors, and memory**

```bash
cargo test -p koldstore-memory-tests --test merge_scan_leak
KOLDSTORE_E2E_NEXTEST_FILTER='binary(hot_only_fast_path)' scripts/run-pg-e2e.sh 16
KOLDSTORE_E2E_NEXTEST_FILTER='binary(koldstore_koldstore_join)' scripts/run-pg-e2e.sh 16
```

Expected: no state leak, correct rescan/join behavior, and a measured improvement.

**Step 5: Re-run Task 5's release benchmark and commit only if it improves the ratio**

```bash
git add crates/pg_koldstore/src/merge_scan/pg.rs \
  tests/memory/merge_scan_leak.rs tests/e2e/merge/hot_only_fast_path.rs
git commit -m "perf: store merge scan execution state on the custom node"
```

## Checkpoint 2: Full local verification

Run:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --exclude e2e --exclude storage-comparison
KOLDSTORE_E2E_PGVERSION=15 scripts/run-pg-e2e.sh 15
KOLDSTORE_E2E_PGVERSION=16 scripts/run-pg-e2e.sh 16
KOLDSTORE_E2E_PGVERSION=17 scripts/run-pg-e2e.sh 17
KOLDSTORE_E2E_PGVERSION=18 scripts/run-pg-e2e.sh 18
```

Docker is not part of this correctness loop.

## Deferred fallback: planner-level native-path elision

Do not include this in the first implementation. If the thin custom wrapper still cannot meet the 10% prepared target after Tasks 1-6, prepare a separate design change that leaves PostgreSQL's native pathlist untouched when `ColdVisibility::NoPublishedSegments` is known at plan time.

That fallback requires all of the following before approval:

- the cross-backend relcache invalidation and prepared-plan test from Task 3;
- a concurrency test covering a flush commit between planning and execution;
- updates to FR-030 and tests that currently require `KoldMergeScan` for every managed SELECT;
- evidence that native-path elision materially outperforms the early `HotChild` wrapper.

## Risks and mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Negative cache hides a flush from another backend | Incorrect missing cold rows | Transactional relcache invalidation plus a two-backend prepared-plan test |
| Invalidation is emitted before a failed flush rolls back | Unnecessary replan or wrong cache state | Use PostgreSQL's transaction-aware invalidation path and test abort behavior |
| Eligibility says cold exists but scoped query has no matching segment | Some user-scoped queries still pay cold setup | Correctness-first table-wide state now; add scope-keyed visibility only after shared path meets target |
| Direct child-slot return breaks tuple descriptors | Wrong projection or executor corruption | Keep `ExecCopySlot` in the first delivery; optimize only with proof |
| Embedded Rust state is mishandled across PostgreSQL `longjmp` | Leak or undefined behavior | Make Task 6 contingent; explicit initialization/drop and memory tests |
| Timing gate is noisy | False regression signal | Use ratios, warmups, 1,000+ samples, three median runs, and a deterministic zero-probe CI assertion |
| Existing dirty work overlaps the implementation | User changes could be overwritten | Implement in an isolated worktree and reconcile intentionally |

## Authoritative PostgreSQL behavior used by this plan

- Custom paths may carry child paths, which PostgreSQL converts to child plans.
- `BeginCustomScan` completes private state initialization; `ExecCustomScan` fills and returns the result tuple slot.
- A Custom Scan state may be a larger structure embedding `CustomScanState` as its first member.
- PostgreSQL relcache invalidation callbacks receive the affected relation OID, and PostgreSQL's plan cache invalidates saved plans that depend on that relation.

References:

- PostgreSQL Custom Scan paths: <https://www.postgresql.org/docs/current/custom-scan-path.html>
- PostgreSQL Custom Scan plans: <https://www.postgresql.org/docs/current/custom-scan-plan.html>
- PostgreSQL Custom Scan execution: <https://www.postgresql.org/docs/current/custom-scan-execution.html>
- PostgreSQL prepared statements and replanning: <https://www.postgresql.org/docs/current/sql-prepare.html>
