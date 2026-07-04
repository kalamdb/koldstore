# pgKalam Benchmarks

This directory contains the benchmark system for comparing plain PostgreSQL with pgKalam/pg-koldstore modes:

- baseline PostgreSQL without the extension
- extension hot-only mode
- extension hot+cold setup
- extension cold-only archive/read mode

The primary benchmark path is real PostgreSQL behavior through SQL and `pgbench`. Criterion.rs benchmarks under `benchmarks/benches/` cover extension internals only.

## Prerequisites

- Rust stable
- `cargo-pgrx` matching the repository pgrx version
- `pgbench`

`benchmarks/scripts/run.sh` starts pgrx-managed PostgreSQL 16 by default, installs the extension from this source tree, and recreates a dedicated benchmark database named `koldstore_pgrx_bench`.

If pgrx is not initialized yet:

```bash
cargo pgrx init
```

## Run Everything

```bash
./benchmarks/scripts/run.sh
```

`benchmarks/scripts/run.sh` starts pgrx PostgreSQL, installs `pg_koldstore`, verifies the connection, runs `CREATE EXTENSION IF NOT EXISTS koldstore`, checks `koldstore_version()`, then runs Criterion and `pgbench` for every configured PostgreSQL mode. It always generates the benchmark reports when it finishes, even if a mode fails and only partial raw data is available. Results are written to:

- `benchmarks/results/summary.json`
- `benchmarks/results/report.md`
- `benchmarks/results/report.html`
- `benchmarks/results/<version>-<UTC time>.html`
- `target/criterion/report/`

Useful knobs:

```bash
BENCH_ROWS=100000 BENCH_SECONDS=30 BENCH_MIXED_SECONDS=120 ./benchmarks/scripts/run.sh
```

For quick harness/debug runs that still generate the full report shape:

```bash
./benchmarks/scripts/run.sh --mini
```

Mini mode defaults to 5,000 rows, 1-second pgbench workloads, 3-second mixed workload, 2 clients/jobs, and skips Criterion. It is for validating benchmark plumbing and report generation, not publishable performance numbers.

By default the runner compacts each mode after setup and before size snapshots:

```bash
KOLDSTORE_BENCH_COMPACT_AFTER_SETUP=0 ./benchmarks/scripts/run.sh
```

Leave compaction enabled for storage comparisons. It removes migration-backfill bloat with PostgreSQL `VACUUM FULL` and `REINDEX`, so size rows reflect the steady hot table/index footprint instead of temporary migration tombstones.

Useful pgrx overrides:

```bash
KOLDSTORE_BENCH_PGVERSION=17 ./benchmarks/scripts/run.sh
KOLDSTORE_BENCH_PGPORT=28817 ./benchmarks/scripts/run.sh
KOLDSTORE_PGRX_INSTALL_SUDO=1 ./benchmarks/scripts/run.sh
```

To run against a PostgreSQL server that you started yourself:

```bash
export DATABASE_URL="host=127.0.0.1 port=28816 user=$USER dbname=postgres"
KOLDSTORE_BENCH_START_PGRX=0 ./benchmarks/scripts/run.sh
```

## Run Only Criterion

```bash
cargo bench
cargo bench -p pg-koldstore-benchmarks
```

Criterion reports are generated under `target/criterion/report/`. These benchmarks cover manifest pruning, cold lookup/miss logic, footer parsing, flush candidate planning, serialization, deduplication, object path generation, policy evaluation, and query-mode decision logic.

## pgbench Modes

The full runner executes all pgbench modes internally from the same script: baseline, extension hot-only, extension hot+cold, and extension cold-only. Each mode creates its own schema, loads the same 100,000-row `bench_events` table by default, applies the same indexes, compacts setup bloat, runs workload scripts, captures approximate system stats, captures table/index/cold sizes, and records representative `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` plans.

## Workloads

The pgbench suite includes at least these groups:

- single hot query
- batch hot query
- hot+cold query
- cold-only query
- cold miss query
- single insert
- batch insert 100, 500, and 1000
- single update
- batch update
- single delete
- batch delete
- mixed workload with 20 clients

The mixed workload uses:

```bash
pgbench -c 20 -j 4 -T 120 -f benchmarks/pgbench/mixed_20_clients.sql
```

## Interpreting Results

Use `report.md` or `report.html` for quick comparisons. The main table shows TPS by mode and p95 latency overhead for extension hot-only versus baseline.

On macOS, open the latest HTML report with:

```bash
open benchmarks/results/report.html
```

Each run also writes a timestamped copy such as `benchmarks/results/0.1.0-20260704T145900Z.html`.

`summary.json` includes more detail:

- average, p50, p95, and p99 latency
- transactions per second
- estimated rows processed
- hot table, hot index, extension metadata, and cold storage sizes
- approximate PostgreSQL RSS and CPU time
- plan file paths
- failed, skipped, or not-applicable benchmark reasons

Rows processed are estimates based on query `LIMIT` or batch size. Memory and CPU are approximate process measurements from `ps`/`pgrep`.

## Comparing Baseline Vs Extension

For overhead, compare baseline TPS and p95 latency against extension hot-only first. That is the safest signal for normal application overhead.

For storage savings, compare baseline hot table/index size with extension hot+cold hot table/index size plus cold storage size. The report intentionally omits whole-database size because it includes unrelated PostgreSQL storage noise.

## Known Limitations

- Query benchmarks run in every mode. Cold-range query rows currently still come from the PostgreSQL heap because `flush_table` writes cold metadata/files but does not prune hot rows from the heap.
- DML workloads are marked N/A in cold-only mode because that mode represents an archive/read-only tier.
- GitHub Actions runners are noisy and shared. Treat CI numbers as trend checks and artifact generation checks, not absolute performance claims.
- CPU and memory measurements are approximate and intended for coarse comparison only.
- The report is static HTML by design so it can be uploaded as a GitHub Actions artifact or published with GitHub Pages.

## Recommended Local Process

Run benchmarks on an otherwise quiet machine, plugged into power, with a local PostgreSQL data directory on stable storage. Use longer durations than CI:

```bash
BENCH_ROWS=100000 BENCH_SECONDS=60 BENCH_MIXED_SECONDS=120 ./benchmarks/scripts/run.sh
```

Run the same command at least three times and compare medians. Use CI artifacts to confirm the harness keeps working, not to publish absolute throughput claims.

## Layout Note

Cargo's default convention is a root `benches/` directory, but this repository keeps benchmark code under the `pg-koldstore-benchmarks` package. The Criterion files live in `benchmarks/benches/` and are wired through explicit `[[bench]]` entries in `benchmarks/Cargo.toml`, so all benchmark assets stay under `benchmarks/`.
