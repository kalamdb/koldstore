# Contributing to KoldStore

Thank you for helping improve KoldStore.

KoldStore is still in early development, so bug reports, documentation fixes, tests, benchmarks, and focused code contributions are all valuable.

Please follow the [Code of Conduct](CODE_OF_CONDUCT.md) when participating.

## Before You Start

* Search existing issues before opening a new one.
* For large features or architecture changes, open an issue first so the design can be discussed.
* Keep pull requests small and focused on one problem.

## Development Setup

You will need:

* Rust 1.96 or newer
* PostgreSQL 15, 16, 17, or 18
* `cargo-nextest`
* `cargo-pgrx` 0.19.1

```bash
cargo install cargo-nextest --locked
cargo install cargo-pgrx --version 0.19.1 --locked
cargo pgrx init
```

## Run the Checks

Run the PostgreSQL-free checks first:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --no-default-features -- -D warnings
cargo nextest run --workspace --no-default-features \
  --exclude e2e \
  --exclude examples \
  --exclude storage-comparison
```

Run in-server pgrx tests (`#[pg_test]` inside `crates/pg_koldstore/src/pg_tests/`):

```bash
RUST_TEST_THREADS=1 cargo pgrx test --manifest-path crates/pg_koldstore/Cargo.toml pg16
```

`RUST_TEST_THREADS=1` is required: `#[pg_test]` shares one database and one async logical slot, and PostgreSQL's SQL slot APIs error (instead of waiting) when the slot is busy.

The Cargo feature `pg_test` is the standard pgrx switch enabled by `cargo pgrx test`. It is distinct from the `#[pg_test]` attribute macro. Keep the feature name; do not rename it.

The extension crate's library target is named `koldstore` (see `[lib] name` in `crates/pg_koldstore/Cargo.toml`) so `cargo pgrx test` issues `CREATE EXTENSION koldstore`. Dependents import it as `koldstore` (`package = "pg_koldstore"`).

PostgreSQL-free shell/contract tests for the extension adapter live in `crates/pg_koldstore-shell-tests` so they do not break `cargo pgrx test` linking.

Run the full in-server matrix:

```bash
for v in 15 16 17 18; do
  RUST_TEST_THREADS=1 cargo pgrx test --manifest-path crates/pg_koldstore/Cargo.toml pg$v
done
```

Run the extension E2E tests against one PostgreSQL version:

```bash
scripts/run-pg-e2e.sh 16
```

Run the full PostgreSQL 15–18 matrix when changing extension integration, SQL, hooks, scans, migration, or flushing:

```bash
scripts/run-pgrx-matrix.sh
```

Additional KoldStore-specific layers (SQL regression, isolation, crash recovery, fuzz, integrity, stress):

```bash
scripts/run-sql-regression.sh 16
scripts/readiness/run-isolation.sh 16
scripts/readiness/run-crash-recovery.sh 16
scripts/readiness/run-sqlsmith.sh 16          # KOLDSTORE_SQLSMITH_SECONDS=…
scripts/readiness/run-integrity-checks.sh 16  # pg_amcheck + KoldStore checks
scripts/readiness/run-hammerdb.sh 16          # weekly/RC; skips if HammerDB missing
scripts/readiness/run-readiness-report.sh 16
```

Do not use `cargo pgrx test` as a substitute for the external E2E suite. E2E still covers multi-session, object-store, and restart scenarios via `scripts/run-pg-e2e.sh`.

For MinIO-backed tests, see [docs/development.md](docs/development.md#minio--s3-backed-e2e).

## Where Code Belongs

KoldStore uses layered Rust crates:

* Put reusable PostgreSQL-free domain logic in the lowest suitable `koldstore-*` crate.
* Keep `pgrx`, SPI, hooks, custom scan integration, and `#[pg_extern]` entrypoints inside `crates/pg_koldstore`.
* No library crate should depend on `pg_koldstore`.
* Avoid duplicate types, unused helpers, and unnecessarily public APIs.

Read [the crate architecture guide](docs/architecture/crate-architecture.md) before adding a new module or dependency.

## Tests and Documentation

A contribution should normally include:

* Tests for new behavior and regressions
* E2E coverage when PostgreSQL-visible behavior changes
* Documentation for SQL APIs, limitations, configuration, or operational behavior
* Updated architecture documentation when ownership or dependencies change

Document important invariants and error behavior in logic-bearing functions.

## Pull Requests

In the pull request description, include:

* The problem being solved
* The approach taken
* How the change was tested
* Any compatibility, performance, recovery, or storage implications

Before submitting, make sure formatting, Clippy, tests, and relevant E2E checks pass.

## Reporting Bugs

Please include enough information to reproduce the problem:

* KoldStore version or commit
* PostgreSQL version
* Operating system
* Storage backend
* Minimal SQL or test case
* Expected and actual behavior
* Relevant logs, errors, or `EXPLAIN` output

Never include passwords, access keys, connection strings, or other secrets.
