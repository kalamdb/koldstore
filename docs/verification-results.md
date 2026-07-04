# Verification Results

Date: 2026-07-04

## Commands

| Command | Result | Notes |
| --- | --- | --- |
| `cargo fmt --all` | PASS | Formatting completed successfully. |
| `cargo clippy --workspace --all-targets --all-features` | BLOCKED | pgrx rejects enabling `pg15`, `pg16`, and `pg17` together: "Multiple `pg$VERSION` features found." |
| `cargo clippy --workspace --all-targets --no-default-features` | PASS | Valid non-pgrx workspace clippy path. |
| `cargo clippy -p pg_koldstore --all-targets --no-default-features --features pg16` | PASS | Valid PG16 pgrx feature clippy path. |
| `cargo nextest run --workspace --no-default-features --exclude e2e` | PASS | Workspace unit, integration, and doc tests passed; pgrx-backed E2E tests remain covered by the local E2E runner. |
| `tests/e2e/run_pg_matrix.sh` | PASS | Uses local pgrx PostgreSQL 16, installs `koldstore`, recreates `koldstore_pgrx_e2e`, and runs `cargo nextest run -p e2e --test-threads 1` serially against the pgrx-managed server with no ignored tests. No Docker dependency. |
| `cargo pgrx install -p pg_koldstore --no-default-features --features pg16 --pg-config "$(cargo pgrx info pg-config 16)"` | PASS | Valid local pgrx extension install path. Direct `cargo pgrx test` is intentionally avoided because native pg-feature test binaries can link unresolved PostgreSQL backend symbols outside the server. |
| `cargo run -p pg-koldstore-benchmarks -- --database-url "host=127.0.0.1 port=28816 user=$USER dbname=koldstore_pgrx_bench" --rows 1000 --clients 2 --jobs 2 --seconds 3` | PASS | Runs real `pgbench` workloads against a fresh local pgrx benchmark database, reports p50/p95/p99 latency plus throughput, and exits nonzero if any benchmark verdict fails. |
| `scripts/run-all-tests.sh` | PASS | Full local verification passes with fmt, no-default-feature clippy/tests, pgrx feature compile/install, local pgrx E2E, memory checks, and benchmarks. |

## Residual Notes

The default E2E loop is now local pgrx rather than Docker. Docker-specific
validation remains under `docker/`.

`SNOWFLAKE_ID()` now uses a Snowflake-style timestamp/worker/sequence layout.
Runtime PostgreSQL builds derive the worker id from the active PostgreSQL
backend identity; `pg_test` builds use worker 0 to avoid linking PostgreSQL
backend globals into native Rust test binaries.

Benchmarks are no longer scaffold-only. The Rust runner prepares real heap and
koldstore tables, delegates load generation to PostgreSQL `pgbench`, parses
`--log` transaction latency files, and emits JSON/HTML reports with p50/p95/p99
latency and throughput.
