# Performance

## Benchmarks

Run the full suite with:

```bash
cargo run -p pg-koldstore-benchmarks -- --suite all
```

The suite compares regular heap tables with managed tables for hot insert,
update, delete, PK select hot-only, PK select cold-required, flush throughput,
and demigration throughput.

## Success Criteria

- SC-002: hot DML latency stays within 10 percent of a regular heap table.
- SC-006: PK point lookups skip at least 90 percent of cold row groups.

## Tracing

Important span families are SQL API calls, DML hook work, flush phases, cold
reader pruning, merge execution, and object-store I/O.

## Investigation Workflow

Start with heap baseline comparison, then inspect PostgreSQL plans, row-group
pruning, manifest state, object-store timing, RSS, and allocation counters. Use
heaptrack output and PostgreSQL memory-context snapshots when repeated scans or
flushes grow memory over time.
