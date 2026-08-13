# Compact Cold-Scan EXPLAIN Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make KoldMergeScan EXPLAIN output compact by default while exposing enough phase timing to attribute catalog lookup, Parquet open/footer work, object-store I/O, and scan/decode time.

**Architecture:** Keep the existing hot/cold execution paths unchanged. Extend the Parquet read profile with measured open, object-store I/O, and scan/decode durations; aggregate those measurements in the PostgreSQL-facing cold profile. Normal EXPLAIN shows aggregate counters and timings, while EXPLAIN VERBOSE owns raw SQL and per-segment diagnostics.

**Tech Stack:** Rust, pgrx/PostgreSQL EXPLAIN APIs, Apache Parquet async reader, object_store, cargo-pgrx tests.

---

### Task 1: Lock the compact EXPLAIN contract

**Files:**
- Modify: `crates/pg_koldstore/src/pg_tests/scan.inc.rs`

**Step 1: Write the failing test**

Extend the cold EXPLAIN regression so ordinary timed `EXPLAIN ANALYZE` requires aggregate `Parquet Open Time`, `Object Store Read Time`, and `Parquet Scan Time`, and rejects `Cold Segments Query`, `Segment Index Query`, and the `Parquet Segments` detail group. Add a VERBOSE assertion that the raw catalog SQL and per-segment object detail remain available.

**Step 2: Run the test to verify it fails**

Run: `cargo pgrx test pg18 explain_analyze_shows_scan_merge_flow_and_phase_timing --package pg_koldstore`

Expected: FAIL because the aggregate phase fields do not exist and normal output still contains SQL/per-segment detail.

### Task 2: Measure Parquet open and object-store I/O

**Files:**
- Modify: `crates/koldstore-parquet/src/object_reader.rs`
- Modify: `crates/koldstore-parquet/src/reader/options.rs`
- Modify: `crates/koldstore-parquet/src/reader/object_store.rs`

**Step 1: Add a failing pure-Rust profile test**

Add assertions around a typed I/O snapshot and `ParquetReadProfile` timing summary so call count, byte count, and accumulated object-store wait are reported independently.

**Step 2: Run the focused test to verify it fails**

Run: `cargo test --package koldstore-parquet object_store_read_stats`

Expected: FAIL because the timing snapshot fields do not exist.

**Step 3: Implement the minimal timing collection**

Time each `get_range`, `get_ranges`, and suffix `get_opts` operation and accumulate nanoseconds atomically. Measure `ParquetRecordBatchStreamBuilder::new_with_options` as open/footer time and the remaining row-group selection/build/iteration work as scan time. Store durations in `ParquetReadProfile` without changing read semantics.

**Step 4: Run the focused test**

Run: `cargo test --package koldstore-parquet object_store_read_stats`

Expected: PASS.

### Task 3: Render compact aggregate timing and VERBOSE details

**Files:**
- Modify: `crates/pg_koldstore/src/merge_scan/pg/profile.rs`

**Step 1: Aggregate the new profile durations**

Add helpers that sum per-segment open, object-store I/O, and scan durations. Keep `Cold Read Time` as end-to-end Parquet wall time so existing tooling remains compatible.

**Step 2: Change the default/VERBOSE split**

In ordinary EXPLAIN, emit aggregate cold counters and the new timing fields only. Emit raw `Cold Segments Query`, `Segment Index Query`, `Hot SPI Query`, and the `Parquet Segments` group only when `ExplainState.verbose` is true. Preserve structured pipeline nodes, but keep raw SQL conditional on VERBOSE.

**Step 3: Run the pgrx regression**

Run: `cargo pgrx test pg18 explain_analyze_shows_scan_merge_flow_and_phase_timing --package pg_koldstore`

Expected: PASS.

### Task 4: Validate the catalog-query concern

**Files:**
- Inspect: `crates/koldstore-catalog/src/queries.rs`
- Inspect: `crates/pg_koldstore/sql/koldstore--0.1.0.sql`
- Inspect: the six `/tmp/koldstore-mem/prod-probe-60047/explain_*.txt` captures

**Step 1: Compare measured catalog time with total execution time**

Use `Segment Catalog Time` and `Segment Index Lookup Time` from the captures to determine whether SQL complexity is an observed latency source.

**Step 2: Check access-shape intent**

Confirm the segment-index `UNION ALL` is deliberate so min/max and unknown-bound arms remain indexable, and confirm supporting partial/B-tree indexes exist.

**Step 3: Avoid an unsupported query rewrite**

If catalog work remains sub-millisecond to roughly one millisecond and the access shape is indexed, retain the SQL and remove it from default EXPLAIN noise. Only create a separate query-architecture change if an actual catalog plan demonstrates a material bottleneck.

### Task 5: Verify the complete change

**Files:**
- Verify all touched Rust files

**Step 1: Format**

Run: `cargo fmt --all`

**Step 2: Run focused tests**

Run: `cargo test --package koldstore-parquet object_store_read_stats`

Run: `cargo pgrx test pg18 explain_analyze_shows_scan_merge_flow_and_phase_timing --package pg_koldstore`

**Step 3: Compile-check affected crates**

Run: `cargo check --package koldstore-parquet --package pg_koldstore`

**Step 4: Check formatting and review scope**

Run: `cargo fmt --all -- --check`

Review `git diff` and `git status --short` to confirm the existing `docker/docker-compose.release.yml` edit is untouched.
