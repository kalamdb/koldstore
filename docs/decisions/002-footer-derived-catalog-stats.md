# ADR-002: Footer-Derived Catalog Segment Stats

## Status

Accepted (deferred implementation)

## Date

2026-07-10

## Context

On flush, KoldStore publishes per-segment `column_stats` (min/max for `seq`,
primary-key, and indexed columns) into `koldstore.cold_segments` /
`cold_segment_stats` so `KoldMergeScan` can prune whole Parquet files **before**
opening them.

Today those bounds are computed twice on the write path:

1. **Manual encode tracking** — `CleanColdRecordBatchBuilder` updates
   `indexed_bounds` per indexed cell via `flush_value_to_json` +
   `compare_json_values`, then `FlushWriteChunk::from_encoded_batches` merges
   bounds across retained Arrow batches.
2. **Parquet writer** — the same columns already have
   `EnabledStatistics::Chunk` (and PK blooms), so the footer already holds
   per–row-group min/max.

`byte_size` is **not** double-paid: catalog size already comes from
`published.byte_size` after durable object publish.

Scan-time segment prune must keep using catalog/manifest metadata only. Opening
every candidate Parquet file just to read footer stats would defeat prune-before-open.

## Decision

1. **Keep** catalog/manifest min/max as the authority for **segment** prune
   (no object open).
2. **Keep** Parquet footer min/max + bloom as the authority for **row-group**
   prune inside an opened file.
3. **Change the write-time source** of catalog `column_stats`: after encode,
   extract segment-level min/max from in-memory Parquet footer statistics
   (min-of-mins / max-of-maxs across row groups), convert into the existing
   catalog JSON shape, and publish that. Remove `indexed_bounds` /
   `update_indexed_bounds` and stop retaining Arrow batches solely to merge
   bounds.
4. Extraction must use the bytes already held for validate/publish — not a
   post-publish object GET.
5. Conversion must be **type-aware** and preserve today’s
   `compare_json_values` domain (e.g. `timestamptz` catalog bounds remain
   RFC3339 strings even though Parquet stores INT64 micros). Unsupported or
   inexact footer stats fail open for that column (omit key) or fail flush for
   required columns — never publish bounds that can falsely exclude a segment.

## Alternatives Considered

### Keep manual `indexed_bounds` forever

- Pros: already matches planner JSON predicates; no footer codec.
- Rejected: duplicates work the writer already does; forces retaining all
  `ColdRecordBatch`es per segment mainly for bound merge.

### Drop catalog stats; prune from Parquet footers at scan begin

- Pros: single stats source on disk.
- Rejected: requires opening (or range-reading) every candidate segment before
  prune; breaks the O(segments) catalog prune contract.

### Publish raw physical footer values as JSON numbers/bytes

- Pros: trivial extraction.
- Rejected: breaks prune for `timestamptz` and other domain mismatches
  (`compare_json_values` returns `None` → silent prune loss, or wrong
  conversion → false exclude).

## Consequences

- Flush encode becomes a single logical stats owner (Parquet writer), with a
  small metadata pass at finalize instead of per-cell JSON bound updates.
- Segment prune API and catalog schema stay unchanged; scanners do not care
  how bounds were produced.
- Implementation needs an allowlisted physical→catalog JSON codec, multi–row-group
  aggregation, adversarial tests, and careful rolling compatibility with
  existing segments that already store encode-time JSON shapes.
- **Priority:** correct architecture, but **not** the next performance
  implementation. Cold PK lookup latency (footer/reader cache, cold-native
  emit) outranks this flush-path dedup. See [roadmap](../roadmap.md) and
  [performance](../performance.md).

## Implementation sketch (when scheduled)

1. Allowlist convertible types; map via PG/Arrow schema, not physical type alone.
2. Fold footer open into validate: return row count + aggregated column stats.
3. Tests: multi-RG, null-only groups, long strings, timestamptz, missing/inexact stats.
4. Wire `indexed_column_stats_json` (or replacement) from footer extraction;
   delete `indexed_bounds` tracking and batch retention for bounds.
5. Keep `byte_size` from publish metadata.
