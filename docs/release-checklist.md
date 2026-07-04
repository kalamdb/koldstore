# Release Checklist

- Build the full workspace with `cargo check --workspace --all-targets --no-default-features`.
- Run `cargo fmt --all` and `cargo clippy --workspace --all-targets --no-default-features`.
- Run Rust unit and regression tests with `cargo nextest run --workspace --no-default-features --exclude e2e`.
- Run the supported PostgreSQL 15 through 18 pgrx matrix with `scripts/run-pgrx-matrix.sh`, which runs pgrx feature clippy, extension install, and E2E checks for each version.
- Use `scripts/run-pgrx-matrix.sh --download-missing` when a supported PostgreSQL major is not already initialized in the local pgrx config; add `--without-icu` only for local downloaded builds on machines without ICU development packages.
- Run MinIO-backed flush and merge tests.
- Run memory and leak gates with `tests/memory/run_memory_checks.sh`.
- Run benchmarks and confirm SC-002 and SC-006 thresholds.
- Verify extension install, upgrade SQL, and `DROP EXTENSION` behavior.
- Review backup/PITR documentation and object-store backup warnings.
- Confirm SQL API, architecture, performance, and operations docs are updated.
