# Contract: Parquet Field Identity and Footer Stats

**Feature**: `003-column-id-lifecycle`  
**References**: KalamDB `ColumnDefinition.column_id`; ADR-002 footer-derived catalog stats

## Write

- Every application column Arrow/Parquet field MUST carry field identity equal to `column_id`.
- Object path MUST be `{prefix}/segment-{NNNN}.parquet` (zero-padded, width ≥ 4).
- Do not emit `batch-*.parquet`.

## Stats Publish

1. Encode Parquet to bytes (writer already collects footer/chunk statistics).
2. Aggregate min/max across row groups per `column_id`.
3. Type-aware convert to catalog JSON.
4. Persist into `segments.column_stats` / `segment_stats` / manifest.
5. **Delete** encode-time `indexed_bounds` (or equivalent) used only for catalog publish.

## Read

- Project and map columns by `field_id` / `column_id`.
- No name-first identity for cutover builds.
- Row-group prune continues to use in-file footer/blooms after open.
- Segment prune continues to use catalog stats only (no open-all-files).

## Failure Rules

- Missing/inexact footer stats for a required prune column: omit key or fail flush — never publish bounds that falsely exclude rows.
- Hard cutover: no “name fallback if field_id missing” in production read path.
