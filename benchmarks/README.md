# pgKalam Benchmarks

This directory contains the benchmark system for comparing plain PostgreSQL with pgKalam/pg-koldstore modes:

- **baseline** — PostgreSQL without the extension
- **extension hot-only** — managed + mirrored, all rows still hot; merge scan disabled so queries use normal Index/Seq Scan (mirror overhead vs baseline)
- **extension hot+cold** — newest `BENCH_HOT_LIMIT` rows stay hot; older rows flushed to zstd Parquet
- **extension cold-only** — archive/read tier; 1 hot sentinel + rest in Parquet (empty heap falls back to Seq Scan); DML is N/A

The primary benchmark path is real PostgreSQL behavior through SQL and `pgbench`. Criterion.rs benchmarks under `benchmarks/benches/` cover extension internals only and do **not** feed the comparison tables.

For **in-process** extension benches (`#[pg_bench]` inside a live backend), use:

```bash
scripts/run-pgrx-bench.sh 16
scripts/run-pgrx-bench.sh 16 --list
```

Those live in `crates/pg_koldstore/src/pg_benches/` and are separate from this pgbench suite.

## Prerequisites

- Rust stable
- `cargo-pgrx` matching the repository pgrx version
- `pgbench`

`benchmarks/scripts/run.sh` starts pgrx-managed PostgreSQL 16 by default, installs a **release** build of the extension (fair timings), and recreates a dedicated benchmark database named `koldstore_pgrx_bench`.

If pgrx is not initialized yet:

```bash
cargo pgrx init
```

## Run Everything

```bash
./benchmarks/scripts/run.sh
```

By default the runner:

1. Starts pgrx PostgreSQL and installs `pg_koldstore` (`--release`)
2. Verifies `CREATE EXTENSION` / `koldstore_version()`
3. Runs baseline → hot-only → hot+cold → cold-only
4. Generates comparison reports even if a mode fails (partial data)

Results:

- `benchmarks/results/summary.json`
- `benchmarks/results/report.md`
- `benchmarks/results/report.html`
- `benchmarks/results/<version>-<UTC time>.html`
- `target/criterion/report/` (only when Criterion is enabled)

Useful knobs:

```bash
BENCH_ROWS=25000 BENCH_HOT_LIMIT=5000 BENCH_SECONDS=5 BENCH_MIXED_SECONDS=15 ./benchmarks/scripts/run.sh
```

For quick harness/debug runs that still generate the full report shape:

```bash
./benchmarks/scripts/run.sh --mini
```

Mini mode defaults to 5,000 rows, hot limit 1,000, 1-second pgbench workloads, 3-second mixed workload, 2 clients/jobs, and skips Criterion. Use it to validate plumbing — not for publishable numbers.

Leave compaction enabled for storage comparisons:

```bash
KOLDSTORE_BENCH_COMPACT_AFTER_SETUP=0 ./benchmarks/scripts/run.sh   # skip VACUUM FULL / REINDEX
KOLDSTORE_PGRX_INSTALL_RELEASE=0 ./benchmarks/scripts/run.sh        # debug extension (slower cold path)
```

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
cargo bench -p pg-koldstore-benchmarks
```

Criterion reports land under `target/criterion/report/`. These microbenchmarks cover manifest pruning, cold lookup/miss logic, footer parsing, flush candidate planning, serialization, deduplication, object path generation, policy evaluation, and query-mode decision logic. They are useful for internal regressions, not for the PostgreSQL vs extension comparison table.

## Mode semantics

| Mode | Setup | Workloads |
|---|---|---|
| baseline | plain `bench_events` | full query + DML suite |
| extension-hot | `manage_table`, no flush; `enable_merge_scan=off` | full query + DML suite (normal PG plans) |
| extension-hot-cold | `manage_table(hot_row_limit=BENCH_HOT_LIMIT)` + `flush_table` | queries + DML on retained hot set |
| extension-cold-only | `manage_table(hot_row_limit=1)` + flush | queries only; DML = N/A |

After flush, the harness verifies `jobs.rows_flushed > 0`, a committed `manifest.json`, and the expected hot-row shape (retained hot rows for hot+cold; 1 sentinel for cold-only so the planner prefers KoldMergeScan).

## Workloads

- single / batch hot query
- hot+cold query
- cold-only query
- cold miss query
- single / batch insert (100, 500, 1000)
- single / batch update
- single / batch delete
- mixed workload with 20 clients

## Interpreting Results

Open `report.md` or `report.html`. The report has three comparison tables:

1. **Query throughput** — TPS and p95 across all four modes (real measurements)
2. **DML throughput** — baseline vs hot-only vs hot+cold (cold-only N/A)
3. **Size comparison** — PostgreSQL heap/index vs cold Parquet, with savings % vs baseline

On macOS:

```bash
open benchmarks/results/report.html
```

`summary.json` includes average/p50/p95/p99 latency, TPS, estimated rows processed, sizes, approximate RSS/CPU, plan paths, and failed/skipped/N/A reasons.

## Recommended Local Process

Run on a quiet machine, plugged in, with a local data directory on stable storage:

```bash
BENCH_ROWS=100000 BENCH_HOT_LIMIT=10000 BENCH_SECONDS=20 BENCH_MIXED_SECONDS=60 \
  KOLDSTORE_BENCH_SKIP_CRITERION=0 ./benchmarks/scripts/run.sh
```

Run at least three times and compare medians. Treat CI numbers as harness/artifact checks, not absolute performance claims.

## Layout Note

Benchmark code lives in the `pg-koldstore-benchmarks` package. Criterion files are under `benchmarks/benches/` via explicit `[[bench]]` entries in `benchmarks/Cargo.toml`.
