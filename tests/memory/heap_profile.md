# Heap and RSS Profiling

## Automated leak gates

```bash
# Unit probe arithmetic + growth budgets
cargo nextest run -p koldstore-memory-tests

# Deep lifecycle gate (flush, DML, merge-scan; MinIO when enabled)
tests/memory/run_memory_checks.sh
```

The deep gate lives in `tests/e2e/suite/memory_leak.rs` and uses the shared
`TestDb` / MinIO harness. After warmup cycles it samples:

- `pg_backend_memory_contexts` totals for the SQL session backend
- process RSS for that backend and matching PostgreSQL workers

It fails when absolute or per-cycle retained growth exceeds
`koldstore_memory::GrowthBudget` (overridable via env):

| Variable | Default role |
|---|---|
| `KOLDSTORE_MEMORY_WARMUP_CYCLES` | cycles discarded before measuring (default 3) |
| `KOLDSTORE_MEMORY_MEASURE_CYCLES` | post-warmup samples (default 12, min 2) |
| `KOLDSTORE_MEMORY_BATCH_ROWS` | rows inserted per cycle (default 128) |
| `KOLDSTORE_MEMORY_SCAN_REPS` | merge-scan SELECT bursts per cycle (default 8) |
| `KOLDSTORE_MEMORY_MAX_CONTEXT_GROWTH_BYTES` | absolute context budget |
| `KOLDSTORE_MEMORY_MAX_RSS_GROWTH_BYTES` | absolute RSS budget |
| `KOLDSTORE_MEMORY_MAX_CONTEXT_BYTES_PER_CYCLE` | context slope budget |
| `KOLDSTORE_MEMORY_MAX_RSS_BYTES_PER_CYCLE` | RSS slope budget |
| `KOLDSTORE_MINIO=1` | enable MinIO flush + parquet GET path |
| `KOLDSTORE_MEMORY_SKIP_E2E=1` | unit probes only |

## Plain Postgres vs koldstore comparison table

`suite::memory_leak::memory_overhead_vs_plain_postgres_reports_spikes_and_deltas`
prints two tables at the end of the run:

1. Per-workload before / after / Δ / spike for **plain** and **koldstore**
2. Overhead rows (`koldstore − plain`) for idle, DML, and hot-only query

Workloads covered: idle, DML, query hot-only, flush, query hot+cold.

## Manual profiles

```bash
heaptrack cargo run -p pg-koldstore-benchmarks -- --suite all
```

CI should upload heaptrack, RSS, and PostgreSQL memory-context snapshots for
benchmark runs and the deep memory leak nextest filter `test(memory_leak::)`.
