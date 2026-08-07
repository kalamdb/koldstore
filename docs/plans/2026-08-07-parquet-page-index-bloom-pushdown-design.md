# Parquet Page-Index and Bloom Filter Pushdown Design

**Issues:** [#95](https://github.com/kalamdb/koldstore/issues/95) (page-index / Bloom pushdown), [#99](https://github.com/kalamdb/koldstore/issues/99) (operator `pruning_columns` / `bloom_filter_columns`)

**Status:** Implemented on `feature/parquet-page-index-bloom-pushdown` (2026-08-07). Approach: extend footer-first reader + operator lists in `cold_metadata` (not catalog-mirrored page bounds, not DataFusion-only `RowFilter`).

**Related (out of scope):** [#70](https://github.com/kalamdb/koldstore/issues/70) row-group catalog stats, [#78](https://github.com/kalamdb/koldstore/issues/78) async filter-before-materialize.

**Typical order noted on #95:** `#70 → #99 → #95 → #78`. This design lands **#99 + #95** without requiring #70.

---

## Goal

Use Parquet page indexes and Bloom filters during cold reads so non-matching pages/rows are skipped before decoding full payloads, especially for PK equality, PK `IN`, and trusted scope equality. Operators can configure which columns participate in min/max stats and Bloom generation at flush time.

## Architecture

Cold reads stay footer-first in `koldstore-parquet` (ObjectStore + local paths). Selective cold open pipeline:

1. Catalog / caller may already narrow `row_groups`
2. Footer min/max stats refine row groups
3. Bloom refine when a pushdown-safe equality probe is present and more than one row group remains
4. **New:** if page indexes exist and the predicate is pushdown-safe, build a `RowSelection` and apply it before the batch stream
5. Exact residual filter + winner/version merge stay after decode (unchanged)

Page indexes and Bloom bitsets are **not** copied into PostgreSQL catalogs. Presence is discovered from the Parquet footer; footer cache continues to store Skip-policy footers only.

### Predicate safety

**Pushdown-safe:** PK equality / `IN`, and trusted scope equality when the scope column is configured and immutable for prune purposes.

**Never pushdown:** mutable column quals, expression quals, or anything that must wait for hot/mirror winner resolution. Those stay residual after merge.

### Write side

Flush enables Parquet page indexes and writes Bloom filters for the effective `bloom_filter_columns` set (forced PK ∪ operator list). Stats columns honor `pruning_columns` ∪ forced keys (PK, `seq`, segment order / scope when set).

---

## Config (#99)

`ManageTableOptions` gains optional `pruning_columns` and `bloom_filter_columns` (column names or stable ids).

At manage/register:

1. Validate types (`supports_stats` / `supports_bloom`)
2. Merge with forced PK (+ order/scope as required)
3. Persist under `options.cold_metadata` (existing `ColdMetadataConfig` shape)

Default with no operator list = today’s auto-derived PK + indexed behavior.

Alter fails closed on DROP or incompatible type change of a listed column. Incomplete `cold_metadata` on older rows falls back to derive-defaults (PK ∪ indexed).

---

## Components

| Layer | Change |
| --- | --- |
| `koldstore-common` | Optional `pruning_columns` / `bloom_filter_columns` on `ManageTableOptions` |
| `koldstore-migrate` | Resolve operator lists → `ColumnRef`s; merge forced keys; rebuild `ColdMetadataConfig`; fail-closed schema change |
| `koldstore-flush` / parquet writer | Write page indexes; Bloom/stats for effective column sets (not PK-only hardcoded) |
| `koldstore-parquet` reader | Conditional page-index load; `RowSelection` from page min/max for equality/`IN`; profile counters |
| `pg_koldstore` merge scan | Pass pushdown-safe probes; EXPLAIN page prune aggregates |
| Footer cache | Cache Skip footers only; page-index loads uncached or separately keyed |

### Selective equality read flow

```
open segment
  → footer (stats) → RG prune
  → Bloom (if needed) → RG refine
  → page index Optional/Required for probe column
  → RowSelection from page bounds
  → project + decode selected pages only
  → residual exact PK / merge winner
```

Non-selective / ordered expand keeps today’s Skip page-index policy (no extra page-index I/O).

---

## Observability

Extend `ParquetReadProfile`:

- `page_index`: `not_requested` | `absent` | `applied`
- `pages_total`, `pages_selected`, `pages_skipped`

EXPLAIN surfaces aggregates beside existing Bloom / row-group prune fields (`Bloom Filters Fetched`, row groups skipped, etc.).

---

## Errors and fallbacks

| Condition | Behavior |
| --- | --- |
| Missing page index | `page_index=absent`; decode selected RGs as today |
| Bloom absent / fetch error | Keep row group (conservative) |
| Operator column unknown / wrong type / unsupported | Manage/alter error |
| DROP/type-change of configured prune/Bloom column | Fail closed until options updated |
| Incomplete legacy `cold_metadata` | Derive defaults |

---

## Tests

- **Writer:** page indexes + Bloom present for configured columns; PK always included
- **Reader unit (`reader_pruning`):** equality/`IN` builds `RowSelection`; pages skipped; absent index is no-op; non-pushdown predicates never request page index
- **Config:** manage JSON round-trip; validation; fail-closed alter
- **E2E:** cold PK point / selective `IN` — fewer pages/bytes vs baseline (profile counters); result equality with pushdown disabled
- **Correctness:** multi-version PK still resolves via merge (pushdown must not hide newer winners)

## Benchmarks

Extend storage comparison / cold PK probe notes: report `pages_skipped`, Bloom mode, `bytes_read` for cold point/selective shapes. Acceptance is a measurable bytes/pages drop on selective cold probe in crate tests or a focused bench; full RESULTS republish is optional.

---

## Delivery slices

1. Options + flush wiring (`pruning_columns` / `bloom_filter_columns` → writer)
2. Page-index `RowSelection` + profile / EXPLAIN
3. Merge-scan pushdown gates + e2e / bench

## Explicit non-goals

- Catalog-mirrored page or row-group bounds (#70)
- Async filter before in-memory materialization (#78)
- DataFusion planning
- Copying Bloom bitsets or full page indexes into Postgres
