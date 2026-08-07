# Catalog-Routed `changes_since` Cursor

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace bulk hot+cold materialization in `koldstore.changes_since` with a catalog-routed seq cursor that opens at most one cold Parquet segment per page (or mirror-only), streams until `limit`, and does not dedupe per-PK across sources.

**Architecture:** Manifest `min_seq`/`max_seq` chooses the next source. Cold pages stream one oldest overlapping segment with early stop; when the cursor is past all cold (or there are no segments), pages are mirror `ORDER BY seq LIMIT n`. Same PK may appear again on a later page with a higher seq. `scope_key` remains an optional filter hook (mirror pushdown + post-filter on cold).

**Tech Stack:** Rust, pgrx SPI, `koldstore-merge` / `koldstore-parquet` / `koldstore-mirror`.

---

## Contract (no legacy)

- Cursor advances strictly by exclusive mirror/cold `seq` (`since_seq`).
- No in-page “latest state per PK” merge across hot+cold.
- No `i32::MAX` hot prefetch; no loading all candidate segments into a `Vec` then truncating.
- Open ≤1 Parquet file per resume page in the common disjoint-segment case.
- Retention gap unchanged: real cursor behind `min(segment.min_seq)` errors.
- `last_rows`: newest-N rewind with bounded memory (mirror first; cold only if shortfall).

## Routing

```
if no published segments OR since_seq >= max(max_seq):
  → mirror_only(since_seq, limit, scope?)
else:
  → segment = oldest where max_seq > since_seq
  → stream segment rows with seq > since_seq until limit (early stop)
  → if page < limit (EOF): fill from mirror (seq > last_emitted, LIMIT rest)
```

## Tasks

### Task 1: Pure seq helpers in `koldstore-merge`

**Files:** `crates/koldstore-merge/src/core/changelog.rs`, `tests/changelog.rs`, `src/sql/events.rs`, `tests/changes_since.rs`, `tests/e2e/dml/change_feed.rs`

Replace `latest_state_after` in `changes_since` with filter `seq > since` → sort by seq → truncate. Update unit tests. Keep retention-gap checks.

### Task 2: Parquet stream early-stop

**Files:** `crates/koldstore-parquet/src/reader/options.rs`, `object_store.rs`, `local.rs`, tests

Add optional `row_limit` on `ParquetReadOptions`. Stop the record-batch loop once enough rows with `seq` in range are collected.

### Task 3: Rewrite `fetch_*` in `pg_koldstore` events

**Files:** `crates/pg_koldstore/src/sql/events/mod.rs`

Implement catalog router; mirror fetch with real `limit`; single-segment cold stream; acquire parquet reader permit; project full application columns for `row_image`; scope hook.

### Task 4: E2E / docs

**Files:** `tests/e2e/dml/wal_only_seq_cursor.rs`, `docs/sql-api.md` / roadmap note if needed

Adjust tests that assumed in-page PK latest-state collapse. Keep segment-not-opened-early coverage. Verify memory-sensitive drain path can page without loading whole 1M segments when limit is small.
