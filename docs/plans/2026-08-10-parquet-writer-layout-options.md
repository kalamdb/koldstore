# Parquet Writer Layout Options Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Expose Parquet row-group, data-page, and Bloom false-positive-rate options per managed table and use them for future flushes.

**Architecture:** Store validated values in `ManageTableOptions`, thread them through `FlushPreparedContext` and `StreamEncodeInput`, and construct `WriterOptions` from that input. `manage_table` and the existing `ALTER TABLE` option hook are the two configuration entrypoints. PK-derived pruning/Bloom defaults remain derived rather than being persisted as overrides.

**Tech Stack:** Rust, pgrx, PostgreSQL, parquet-rs, Rust E2E tests.

---

### Task 1: Specify and prove the new `manage_table` SQL contract

**Files:**
- Modify: `tests/e2e/flush/mod.rs`
- Create: `tests/e2e/flush/parquet_layout_options.rs`
- Modify: `crates/pg_koldstore/src/sql/migrate/mod.rs`
- Modify: `crates/pg_koldstore/src/sql/migrate/manage.rs`
- Modify: `crates/koldstore-migrate/src/validation/manage_table.rs`
- Modify: `crates/koldstore-common/src/config/options.rs`

**Step 1:** Write an E2E that invokes `manage_table` with layout options and fails because the SQL arguments do not exist.

**Step 2:** Add typed, positive-only persisted options and wire the SQL arguments through validation.

**Step 3:** Run the focused E2E to prove the SQL API succeeds.

### Task 2: Apply settings to future Parquet writes

**Files:**
- Modify: `crates/pg_koldstore/src/sql/flush/execute.rs`
- Modify: `crates/koldstore-flush/src/encode.rs`
- Modify: `tests/e2e/flush/parquet_layout_options.rs`

**Step 1:** Extend the E2E to inspect the emitted Parquet footer and fail while the encoder uses hard-coded/default writer layout.

**Step 2:** Thread the persisted options to `WriterOptions` and add a focused unit test for the conversion.

**Step 3:** Run the focused E2E and unit test to prove multiple row groups/pages are produced.

### Task 3: Support future-flush ALTER options and verify

**Files:**
- Modify: `crates/pg_koldstore/src/hooks/ddl.rs`
- Modify: `crates/pg_koldstore/src/pg_tests/manage.inc.rs`
- Modify: `docs/sql-api.md`

**Step 1:** Write the failing PostgreSQL test for `ALTER TABLE ... SET` layout options.

**Step 2:** Validate and persist the values through the existing hook.

**Step 3:** Run focused tests, formatting, and the relevant workspace checks.
