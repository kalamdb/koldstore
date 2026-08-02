# ADR-002: Footer-Derived Catalog Segment Stats

## Status

Implemented

## Date

2026-07-10

## Context

On flush, KoldStore publishes Sort Key V1 bounds into
`koldstore.cold_segment_index` so `KoldMergeScan` can prune whole Parquet files
**before** opening them.

The earlier implementation computed those bounds twice on the write path:

1. **Manual encode tracking** — `CleanColdRecordBatchBuilder` updates
   `indexed_bounds` per indexed cell via `flush_value_to_json` +
   `compare_json_values`, then `FlushWriteChunk::from_encoded_batches` merges
   bounds across retained Arrow batches.
2. **Parquet writer** — the same columns had
   `EnabledStatistics::Chunk` (and PK blooms), so the footer already holds
   per–row-group min/max.

`byte_size` is **not** double-paid: catalog size already comes from
`published.byte_size` after durable object publish.

Scan-time segment prune must keep using catalog/manifest metadata only. Opening
every candidate Parquet file just to read footer stats would defeat prune-before-open.

## Decision

1. **Keep** catalog/manifest min/max as the authority for **segment** prune
   (no object open).
2. Persist aligned row-group arrays beside the scalar segment bounds. PostgreSQL
   uses existing scalar B-tree indexes to find candidate segments; Rust then
   intersects packed row-group arrays for the candidate IDs before any Parquet
   object is opened.
3. Derive segment bounds, row-group bounds, null counts, row counts, and SeqId
   ranges from the `ParquetMetaData` returned by `ArrowWriter::close()`.
4. Convert supported statistics directly into Sort Key V1 bytes. Remove
   `indexed_bounds`, per-cell JSON conversion, and source-row min/max
   accumulation.
5. Extraction and publish use the finalized bytes and in-memory metadata from
   the same writer close. They never upload and download the object to recover
   statistics, and write-path validation does not parse the footer again.
6. Mirror catalog arrays in Manifest V2 shard entries as hexadecimal Sort Key
   bytes. Shards are immutable and content-addressed with a 128-bit SHA-256
   filename prefix; the root retains the complete digest. The thin root is
   published only after its shards, then obsolete unreferenced shard versions
   are removed.

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
- Catalog size remains `O(segments × indexed columns)`; row-group detail is
  stored in TOAST-able arrays rather than a child row per row group.
- Segment bounds remain SQL-indexable. Array positions are evaluated in Rust;
  there is no `unnest()` or GIN range-search path.
- Proven all-null groups are excluded for ordinary comparisons. Unknown
  statistics retain the segment/group conservatively. Required non-null PK and
  SeqId statistics fail a new flush when incomplete.
- Manifest V2 and catalog rows share the same scalar/array shape, which keeps
  database/file serialization straightforward.

## Implementation

Completed on 2026-07-29. The bootstrap catalog adds packed arrays to
`cold_segments` and `cold_segment_index`; the flush writer persists them from
finalized footer metadata; merge scans refine scalar candidates in Rust; and
Manifest V2 mirrors the same metadata in content-addressed shards.
