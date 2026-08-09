# Schema-Qualified Mirror Names Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Prevent mirror sharing between same-named source tables and preserve that uniqueness across source schema and table renames.

**Architecture:** Generate mirrors as `koldstore.<source_schema>_<source_table>__cl`, retaining a deterministic hash suffix only when PostgreSQL's identifier limit requires it. Extend the ProcessUtility rename follow-up to rehome every generated mirror artifact, and use PostgreSQL-backed regression tests for collisions and rename reuse.

**Tech Stack:** Rust, pgrx, PostgreSQL ProcessUtility hook, `cargo pgrx test`.

---

### Task 1: Reproduce schema collision and rename behavior

**Files:**
- Modify: `crates/pg_koldstore/src/pg_tests/manage.inc.rs`

**Step 1:** Add `#[pg_test]` coverage that manages `schema_a.messages` and `schema_b.messages`, verifies distinct mirrors, and proves one can be unmanaged without dropping the other.

**Step 2:** Add a `#[pg_test]` that renames a managed source table and schema, verifies its artifacts adopt the new derived names, and manages a new table at the old name.

**Step 3:** Run the focused pgrx tests and confirm they fail against the unmodified implementation.

### Task 2: Generate schema-qualified, bounded mirror identifiers

**Files:**
- Modify: `crates/koldstore-wal-mirror/src/mirror/shared/relation.rs`
- Test: the crate's existing relation/planning tests

**Step 1:** Add pure-Rust tests for ordinary schema-qualified output and deterministic bounded output.

**Step 2:** Implement the naming rule with a stable hash fallback for identifiers over PostgreSQL's 63-byte limit.

**Step 3:** Run the narrow unit tests.

### Task 3: Rehome artifacts after source relation renames

**Files:**
- Modify: `crates/koldstore-wal-mirror/src/mirror/shared/schema.rs`
- Modify: `crates/koldstore-wal-mirror/src/mirror/guard.rs`
- Modify: `crates/pg_koldstore/src/sql/migrate/schema_registry.rs`
- Modify: `crates/pg_koldstore/src/hooks/ddl.rs`

**Step 1:** Plan the table/index and guard function/trigger rename DDL from the old and new mirror identities.

**Step 2:** Invoke the plan after table renames and for all managed tables affected by `ALTER SCHEMA ... RENAME TO`.

**Step 3:** Run the focused regressions and `cargo check`.

### Task 4: Verify and format

**Files:**
- Modify: touched Rust and test files only

**Step 1:** Run `cargo fmt --all`.

**Step 2:** Run the focused pgrx tests, workspace check, and the appropriate full package tests.

**Step 3:** Review the diff and verify the active branch has only this fix's changes.
