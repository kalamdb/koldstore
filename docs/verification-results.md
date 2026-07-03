# Verification Results

Date: 2026-07-04

## Commands

| Command | Result | Notes |
| --- | --- | --- |
| `cargo fmt --all` | PASS | Formatting completed successfully. |
| `cargo clippy --workspace --all-targets --all-features` | BLOCKED | pgrx rejects enabling `pg15`, `pg16`, and `pg17` together: "Multiple `pg$VERSION` features found." |
| `cargo clippy --workspace --all-targets --no-default-features` | PASS | Valid non-pgrx workspace clippy path. |
| `cargo clippy -p pg_koldstore --all-targets --no-default-features --features pg16` | PASS | Valid PG16 pgrx feature clippy path. |
| `cargo test --workspace --no-default-features` | PASS | Workspace unit, integration, and doc tests passed; pgrx-backed ignored tests remain covered by the local E2E runner. |
| `tests/e2e/run_pg_matrix.sh` | PASS | Uses local pgrx PostgreSQL 16, installs `koldstore`, recreates `koldstore_pgrx_e2e`, and runs E2E tests with ignored pgrx tests included. No Docker dependency. |
| `cargo pgrx test --no-default-features --features pg16` | BLOCKED | Fails during native test-binary linking with unresolved PostgreSQL server symbols from normal Rust integration tests under pg16 feature builds. |
| `cargo run -p pg-koldstore-benchmarks -- --rows 1000 --clients 2 --jobs 2 --seconds 1 --output-json target/pg-koldstore-bench.json --output-html target/pg-koldstore-bench.html` | PASS | Runs real `pgbench` workloads, parses per-transaction logs, and writes JSON/HTML reports. |

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
