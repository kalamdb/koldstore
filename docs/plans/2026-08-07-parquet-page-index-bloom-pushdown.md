# Parquet Page-Index and Bloom Pushdown Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Land [#95](https://github.com/kalamdb/koldstore/issues/95) + [#99](https://github.com/kalamdb/koldstore/issues/99): operator-configurable prune/Bloom columns, flush writes Bloom for the effective set, and selective cold reads apply page-index `RowSelection` with EXPLAIN/profile counters.

**Architecture:** Extend footer-first `koldstore-parquet` reads (RG stats → Bloom → page-index `RowSelection`). Persist operator lists on `ManageTableOptions`, merge into existing `ColdMetadataConfig` at register time. Do not catalog-mirror page indexes (#70 stays separate).

**Tech Stack:** Rust, `parquet` (Arrow async reader / `RowSelection` / `PageIndexPolicy`), pgrx SPI for manage/flush/EXPLAIN, crate unit tests + focused e2e.

**Design:** [2026-08-07-parquet-page-index-bloom-pushdown-design.md](2026-08-07-parquet-page-index-bloom-pushdown-design.md)

---

### Task 1: Operator column lists on `ManageTableOptions`

**Files:**
- Modify: `crates/koldstore-common/src/config/options.rs`
- Test: same file `#[cfg(test)]` module (extend existing options tests)

**Step 1: Write the failing test**

Add tests that:
- `pruning_columns` / `bloom_filter_columns` round-trip through `to_value` / `from_value` / `try_from_value` as `Vec<String>` (SQL names; stable ids resolved later in migrate)
- empty / whitespace-only entries are rejected by `try_from_value`
- omitted fields stay `None` (default auto-derive path)

**Step 2: Run test to verify it fails**

```bash
cargo test -p koldstore-common manage_table_options -- --nocapture
```

Expected: FAIL (fields missing).

**Step 3: Minimal implementation**

Add to `ManageTableOptions`:

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub pruning_columns: Option<Vec<String>>,
#[serde(default, skip_serializing_if = "Option::is_none")]
pub bloom_filter_columns: Option<Vec<String>>,
```

In `try_from_value`, reject blank names. Add `with_pruning_columns` / `with_bloom_filter_columns` builders.

**Step 4: Run tests — expect PASS**

**Step 5: Commit**

```bash
git add crates/koldstore-common/src/config/options.rs
git commit -m "feat(config): accept pruning_columns and bloom_filter_columns options"
```

---

### Task 2: Merge operator lists into `ColdMetadataConfig`

**Files:**
- Modify: `crates/koldstore-migrate/src/catalog/register.rs`
- Modify: `crates/koldstore-migrate/tests/schema_registry.rs`
- Modify: `crates/koldstore-migrate/tests/manage_table_validation.rs` (if validation lives there)
- Modify: `crates/koldstore-migrate/src/validation/manage_table.rs` as needed

**Step 1: Failing tests**

- Operator `bloom_filter_columns: ["id"]` → `cold_metadata.bloom_filter_columns` includes PK `id` (forced) even if list omits other indexed cols
- Operator `pruning_columns: ["created_at"]` merges with forced keys (`seq` is writer-side; config stores app columns)
- Unknown column name → prepare/validate error
- Unsupported type for Bloom → error

**Step 2: Run — expect FAIL**

```bash
cargo test -p koldstore-migrate cold_metadata -- --nocapture
```

**Step 3: Implement**

Extend `cold_metadata_config` (or add `cold_metadata_config_with_overrides`) to accept optional operator pruning/Bloom name lists + table columns for lookup. Resolve names → `ColumnRef` via schema columns. Force-include primary key into Bloom set. Keep indexed auto-derive when overrides are `None`.

Wire from `RegistrationMetadata::prepare` using `self.options.pruning_columns` / `bloom_filter_columns`.

**Step 4: PASS + commit**

```bash
git commit -m "feat(migrate): merge operator prune/Bloom columns into cold_metadata"
```

---

### Task 3: Flush honors effective Bloom / stats columns

**Files:**
- Modify: `crates/koldstore-flush/src/encode.rs` (today hardcodes `.with_bloom_filter_columns(primary_key…)`)
- Modify: `crates/pg_koldstore/src/sql/flush/execute.rs` (pass bloom/stats from catalog options)
- Test: `crates/koldstore-flush` unit tests and/or `crates/koldstore-parquet/tests/writer_roundtrip.rs`

**Step 1: Failing test**

Encode/flush input with extra bloom column → writer plan `bloom_filter_columns` contains PK + extra; stats columns include operator pruning list.

**Step 2–4:** Thread `bloom_filter_columns` / `statistics_columns` (or `indexed_columns` already used for stats) from schema `cold_metadata` into `FlushEncodeInput` / `WriterOptions`. Ensure page indexes remain enabled (parquet default `offset_index_disabled=false`; assert column_index present when reading with `PageIndexPolicy::Required`).

**Step 5: Commit**

```bash
git commit -m "feat(flush): write Bloom/stats for configured cold_metadata columns"
```

---

### Task 4: Page-index `RowSelection` helper + profile fields

**Files:**
- Modify: `crates/koldstore-parquet/src/prune.rs` (or new `crates/koldstore-parquet/src/page_prune.rs`)
- Modify: `crates/koldstore-parquet/src/reader/options.rs` (`PageIndexPruneMode`, counters on `ParquetReadProfile`)
- Modify: `crates/koldstore-parquet/src/lib.rs` exports
- Test: `crates/koldstore-parquet/tests/reader_pruning.rs`

**Step 1: Failing tests**

Write a small Parquet file with multiple pages (small `data_page_row_count_limit` / row_group_size), distinct PK ranges per page. Assert helper:
- equality value in page 2 → `RowSelection` skips earlier pages
- missing page index → returns `None` / absent mode (caller reads full RGs)
- profile counters: `pages_total > pages_selected`, `pages_skipped > 0`

**Step 2: Run — expect FAIL**

```bash
cargo test -p koldstore-parquet reader_pruning -- --nocapture
```

**Step 3: Implement**

```text
for each selected row group:
  column_index[rg][col] + offset_index[rg][col]
  for each page: if page null-only OR min/max cannot contain any probe value → skip
  else select page row span via first_row_index + page length
concatenate into RowSelection across selected RGs (builder applies per-file selection after with_row_groups)
```

Reuse existing string/physical-type comparison patterns from `row_group_may_contain_pk_values` / `bloom_may_contain`.

**Step 4: PASS + commit**

```bash
git commit -m "feat(parquet): build RowSelection from page indexes for equality probes"
```

---

### Task 5: Wire page-index load into ObjectStore + local readers

**Files:**
- Modify: `crates/koldstore-parquet/src/reader/object_store.rs`
- Modify: `crates/koldstore-parquet/src/reader/local.rs`
- Modify: `crates/koldstore-parquet/src/object_reader.rs` (already avoids caching indexed footers — verify)
- Test: extend `reader_pruning.rs` ObjectStore path tests (existing bloom tests nearby)

**Step 1: Failing integration test**

ObjectStore read with `pk_values` on multi-page file → profile `page_index=applied`, `pages_skipped > 0`, correct row returned; bytes_read lower than full RG decode baseline (optional soft assert).

**Step 2–4:** After RG/Bloom selection, if `pk_values` present:
1. Rebuild/open builder with `PageIndexPolicy::Optional` (or Required when writer guarantees indexes)
2. Apply `RowSelection` when indexes present
3. Fill profile fields; else `absent` / `not_requested`

Non-PK reads keep Skip policy (no extra I/O).

**Step 5: Commit**

```bash
git commit -m "feat(parquet): apply page-index RowSelection on selective cold reads"
```

---

### Task 6: EXPLAIN + merge-scan pushdown gates

**Files:**
- Modify: `crates/pg_koldstore/src/merge_scan/pg/profile.rs`
- Modify: `crates/pg_koldstore/src/merge_scan/pg/cold.rs` (only if probe wiring needs a flag; PK probes already use `with_pk_values`)
- Test: `crates/pg_koldstore/src/pg_tests/scan.inc.rs` or e2e explain assertion

**Steps:** Surface `Page Index`, `Pages Skipped` (and totals) beside Bloom fields. Do **not** pass mutable/residual quals as `pk_values`. Document gate: only ExactPrimaryKey / PK IN / trusted scope equality.

Commit:

```bash
git commit -m "feat(scan): expose page-index prune counts in EXPLAIN"
```

---

### Task 7: Fail-closed schema change + docs + e2e smoke

**Files:**
- Modify: schema refresh / alter validation path that compares active vs current columns
- Modify: `docs/architecture/manage-table.md` (document option keys)
- Modify: `docs/roadmap.md` (mark prune/Bloom config + page-index as landed or in-progress)
- Test: e2e manage with explicit `bloom_filter_columns` + cold PK probe profile counters

**Steps:** DROP/rename of configured prune/Bloom column fails closed. Update design status note. Run:

```bash
cargo test -p koldstore-common -p koldstore-migrate -p koldstore-parquet -p koldstore-flush
# focused pgrx / e2e as available
```

Commit:

```bash
git commit -m "test+docs: page-index Bloom pushdown acceptance for #95/#99"
```

---

## Verification checklist

- [ ] Page-index `RowSelection` when footers expose indexes
- [ ] Bloom used for supported equality when present (existing + configured cols)
- [ ] Operator `pruning_columns` / `bloom_filter_columns` on manage options
- [ ] Never push predicates needing winner resolution
- [ ] EXPLAIN/profile expose page prune counts
- [ ] Cold point/selective probe shows fewer pages/bytes in unit/integration test
