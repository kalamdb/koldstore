# Stable Column IDs Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Introduce stable `column_id` (from `pg_attribute.attnum`) as the identity for managed columns across schema registry, cold metadata, manifests, and scan planning so renames do not invalidate persisted metadata.

**Architecture:** Names become schema-version labels only. Persisted maps/keys use `column_id`. Planner uses `Var.varattno` as `column_id`. Parquet read resolves `(schema_version, column_id)` to the physical field name for that version. Breaking reset — no migration.

**Tech Stack:** Rust workspace crates (`koldstore-*`, `pg_koldstore`), serde JSON catalog, pgrx SPI, cargo pgrx tests / e2e under `tests/`.

**Issue:** https://github.com/kalamdb/koldstore/issues/66

---

### Task 1: Domain `ColumnId` + `ColumnRef`

**Files:**
- Create: `crates/koldstore-common/src/domain/column.rs`
- Modify: `crates/koldstore-common/src/domain/mod.rs`, `lib.rs`
- Test: unit tests in `column.rs`

**Steps:** Add `ColumnId(i16)` newtype and `ColumnRef { column_id, name }`. Export from crate root.

### Task 2: SchemaColumn + wire JSON

**Files:**
- Modify: `crates/koldstore-schema/src/schema_registry.rs`
- Modify: `crates/koldstore-schema/tests/schema_models.rs`

**Steps:** Add `column_id` to `SchemaColumn` / wire; update builders; test serialize includes `column_id` and validate by ID.

### Task 3: Evolution by ID

**Files:**
- Modify: `crates/koldstore-schema/src/evolution.rs`

**Steps:** Add `column_id` to `CatalogColumnShape`; compare by ID; PK by ID list; rename = refresh preserving ID; drop+add same name = different ID; type change same ID = reject. TDD first.

### Task 4: Introspection + registration

**Files:**
- Modify migrate catalog introspection/register/refresh/validation
- Related migrate tests

**Steps:** Emit `attnum` as `column_id`; `CatalogColumn` carries ID; cold_metadata / PK shape / indexed use IDs + display names.

### Task 5: Breaking DDL

**Files:**
- `crates/pg_koldstore/sql/koldstore--0.1.0.sql`
- `crates/koldstore-setup/src/*`

**Steps:** `cold_segment_stats` PK `(segment_id, column_id)`; update indexes.

### Task 6: Flush + catalog read path

**Files:**
- flush `segment_catalog`, catalog `queries`/`decode`/`cache`

**Steps:** Write/read stats by `column_id`.

### Task 7: Manifest

**Files:**
- `koldstore-manifest` model/assembly + golden + JSON schema

**Steps:** `column_stats` array with `column_id`; bloom `column_ids`; PK filter uses attnums.

### Task 8: Parquet + planner

**Files:**
- parquet reader/writer/prune; merge quals/plan; `pg_koldstore` qual/cold

**Steps:** Write physical names from schema version; read via `(schema_version, column_id)`; quals use `varattno`.

### Task 9: E2E rename coverage

**Files:**
- `tests/e2e/suite/schema_evolution.rs` (+ helpers)

**Steps:** Flush → rename → query old cold by new name; flush after rename; drop+add same name does not reuse stats.

### Task 10: Update issues

Comment on #66 with done work; update #65 to use `column_id` throughout.
