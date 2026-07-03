# Heap and RSS Profiling

Run memory profiles against the benchmark and E2E workflows after the extension
is installed into the local PostgreSQL matrix.

```bash
heaptrack cargo run -p pg-koldstore-benchmarks -- --suite all
tests/memory/run_memory_checks.sh
```

CI should upload heaptrack, RSS, and PostgreSQL memory-context snapshots for
benchmark runs, repeated merge scans, cold reader scans, flushes, and
demigration loops.
