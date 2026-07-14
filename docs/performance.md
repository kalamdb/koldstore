# Performance

## Benchmarks

Storage-comparison results, methodology, and throughput trade-offs live in
[benchmarks](benchmarks/README.md). Re-run with
`scripts/run-storage-comparison.sh`.

Additional suite:

```bash
cargo run -p pg-koldstore-benchmarks -- --suite all
```

That suite compares regular heap tables with managed tables for hot insert,
update, delete, PK select hot-only, PK select cold-required, flush throughput,
and demigration throughput. The post-flush cold PK gap (Parquet open + merge
setup vs B-tree) is the main read-path focus.

## Success Criteria

- SC-002: hot DML latency stays within 10 percent of a regular heap table.
- SC-006: PK point lookups skip at least 90 percent of cold row groups.

## Priority order (accepted direction)

1. **Cold PK point lookups** — backend Parquet footer/reader cache; cold-native
   emit that skips the JSON merge path when a PK equality hits cold only.
   Dominates the hot+cold ops/s gap after flush.
2. **Footer-derived catalog segment stats** — stop double-computing min/max on
   flush (`indexed_bounds` vs writer chunk stats). Catalog still owns
   prune-before-open; `byte_size` is already single-source from publish.
   See [ADR-002](decisions/002-footer-derived-catalog-stats.md).
3. Segment sizing / page indexes / streaming merge polish — secondary levers
   once (1) lands.

Tracked on the [roadmap](roadmap.md).

## Tracing

Important span families are SQL API calls, DML hook work, flush phases, cold
reader pruning, merge execution, and object-store I/O.

Use `EXPLAIN (ANALYZE)` on managed SELECTs and inspect KoldMergeScan properties
(`Parquet segment` `read_ms`, row-group selection, bloom mode, PK probe) to
separate footer-open cost from merge/SPI overhead.

## Investigation Workflow

Start with heap baseline comparison, then inspect PostgreSQL plans, row-group
pruning, manifest state, object-store timing, RSS, and allocation counters. Use
heaptrack output and PostgreSQL memory-context snapshots when repeated scans or
flushes grow memory over time.
