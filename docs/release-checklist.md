# Release Checklist

- Build the full workspace with `cargo check --workspace --all-targets --no-default-features`.
- Run `cargo fmt --all` and `cargo clippy --workspace --all-targets --no-default-features`.
- Run Rust unit and regression tests with `cargo nextest run --workspace --no-default-features --exclude koldstore-e2e`.
- Run pgrx feature compile/install checks for supported PostgreSQL versions.
- Run the local pgrx-backed E2E matrix with `tests/e2e/run_pg_matrix.sh`.
- Run MinIO-backed flush and merge tests.
- Run memory and leak gates with `tests/memory/run_memory_checks.sh`.
- Run benchmarks and confirm SC-002 and SC-006 thresholds.
- Verify extension install, upgrade SQL, and `DROP EXTENSION` behavior.
- Review backup/PITR documentation and object-store backup warnings.
- Confirm SQL API, architecture, performance, and operations docs are updated.
