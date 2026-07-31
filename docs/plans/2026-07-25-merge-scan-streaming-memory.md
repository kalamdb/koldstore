# KoldMergeScan Streaming Memory Implementation Plan

> **Status (2026-07-25):** Mixed/cold streaming + bounded hot paging landed.
> Remaining deferred work: on-disk spillable seen-set, `count(*)` catalog fast
> path. Canonical behavior is documented in
> [scanning-table.md](../architecture/scanning-table.md).

> **For Codex:** REQUIRED SUB-SKILLS: use `test-driven-development`, `rust-skills`,
> `cargo-pgrx`, and `verification-before-completion` while executing this plan.

**Goal:** Replace full cold-result materialization with newest-first,
segment-bounded merge emission so full cold scans do not retain every decoded
row image and every PostgreSQL Datum until `EndCustomScan`.

**Architecture:** Plan cold segments during `BeginCustomScan`, but open and
decode them lazily from `ExecCustomScan`. Group overlapping sequence ranges so
each group can be resolved exactly, process groups newest-first, and retain only
compact primary-key identities after each winner payload is emitted. Keep the
hot and mirror overlays authoritative and let PostgreSQL `ExecScan` continue to
apply residual and RLS predicates.

**Tech Stack:** Rust, pgrx CustomScan APIs, PostgreSQL memory contexts,
ObjectStore/Parquet readers, local pgrx-managed PostgreSQL.

---

### Task 1: Specify newest-first streaming winner resolution

**Files:**

- Modify: `crates/koldstore-common/src/domain/pk.rs`
- Modify: `crates/koldstore-merge/src/core/resolver.rs`
- Test: `crates/koldstore-merge/tests/resolver.rs`

**Step 1: Write the failing tests**

Add tests proving that:

- a single-column logical PK converts to an exact compact identity without
  retaining its repeated column name;
- hot keys and newer cold winners mask older cold versions across batches;
- tombstones mark a key as seen without producing a visible row;
- only the current batch retains row payloads.

**Step 2: Verify RED**

Run:

```bash
cargo test -p koldstore-merge --test resolver streaming
```

Expected: failure because the compact identity and newest-first resolver do not
exist.

**Step 3: Implement the minimal pure-Rust resolver**

Introduce a type-safe compact PK-value identity and a newest-first resolver
whose persistent state is only a `HashSet` of those identities plus counters.
Resolve duplicates inside each supplied batch before marking keys as seen.

**Step 4: Verify GREEN**

Run the focused resolver tests and the complete `koldstore-merge` test suite.

### Task 2: Plan safe segment groups

**Files:**

- Modify: `crates/koldstore-merge/src/scan/plan.rs`
- Test: `crates/koldstore-merge/tests/merge_scan_exec.rs`

**Step 1: Write the failing tests**

Cover disjoint ranges, transitively overlapping ranges, equal boundaries, and
missing/invalid `seq` metadata.

**Step 2: Verify RED**

Run:

```bash
cargo test -p koldstore-merge --test merge_scan_exec newest_first
```

Expected: failure because segment grouping is not implemented.

**Step 3: Implement grouping**

Read typed `SeqId` bounds from each segment's catalog stats, sort by newest
maximum sequence, and combine every transitively overlapping interval. Reject
missing or malformed sequence metadata instead of risking an incorrect stream.

**Step 4: Verify GREEN**

Run the focused grouping tests and the complete `koldstore-merge` suite.

### Task 3: Stream cold segment groups from the PostgreSQL executor

**Files:**

- Modify: `crates/pg_koldstore/src/merge_scan/pg/cold.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg/execute.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg/emit.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg/tuple.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg/profile.rs`
- Test: `crates/pg_koldstore/src/pg_tests/scan.inc.rs`

**Step 1: Write the failing pgrx regression**

Create several cold segments, run `EXPLAIN ANALYZE SELECT count(*)`, and assert
that the emit path is streaming and the reported peak retained cold payload is
no larger than one non-overlapping segment group.

**Step 2: Verify RED**

Run the single new `#[pg_test]` with the locally configured pgrx PostgreSQL.
Expected: failure because the existing path reports `merge_buffer` and has no
bounded-payload counter.

**Step 3: Implement lazy execution**

Make cold planning return an owned stream descriptor rather than a
`Vec<ColdRow>`. On each CustomScan access:

1. emit unconsumed hot winners;
2. open and decode the next safe cold segment group;
3. resolve its winners against compact identities already seen;
4. materialize one row into a resettable per-row PostgreSQL memory context;
5. drop the emitted JSON payload before requesting the next row.

Accumulate segment profiles and peak-payload counters as groups are consumed.
Preserve the existing buffered path only for hot-only SPI fallback where it is
already cheap.

**Step 4: Verify GREEN**

Run the new pgrx regression, the existing scan regressions, and an ordered
multi-wave flush regression.

### Task 4: Verify scope and memory behavior

**Files:**

- Modify if needed: `tests/e2e/suite/memory_leak.rs`
- Modify: `docs/plans/2026-07-10-merge-scan-redesign.md`
- Modify: `docs/roadmap.md`

**Step 1: Run correctness checks**

Run:

```bash
cargo fmt --all --check
cargo check -p pg_koldstore --features pg18
cargo test -p koldstore-merge
```

Then run focused pgrx scan tests and the local e2e ordered-limit test.

**Step 2: Review the diff**

Confirm that no Docker-based correctness dependency was introduced, no
unrelated dirty-worktree files were changed, and no query/test workaround hides
incorrect scan results.

**Step 3: Report the precise memory bound**

Document that retained row payload and Datum memory is bounded by the largest
overlapping segment group plus one hot SPI page. The exact-PK seen set remains
linear in distinct logical keys; it is compact and payload-free, so this change
substantially reduces RSS without claiming constant total memory.
