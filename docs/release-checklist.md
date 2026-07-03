# Release Checklist

- Build the full workspace with `cargo check --workspace --all-targets`.
- Run `cargo fmt --all` and `cargo clippy --workspace --all-targets --all-features`.
- Run Rust unit and regression tests with `cargo test --workspace`.
- Run pgrx SQL tests with `cargo pgrx test`.
- Run the PostgreSQL 15, 16, and 17 E2E matrix with `tests/e2e/run_pg_matrix.sh`.
- Run MinIO-backed flush and merge tests.
- Run memory and leak gates with `tests/memory/run_memory_checks.sh`.
- Run benchmarks and confirm SC-002 and SC-006 thresholds.
- Verify extension install, upgrade SQL, and `DROP EXTENSION` behavior.
- Review backup/PITR documentation and object-store backup warnings.
- Confirm SQL API, architecture, performance, and operations docs are updated.
