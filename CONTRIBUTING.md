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

Run the extension E2E tests against one PostgreSQL version:

```bash
scripts/run-pg-e2e.sh 16
```

Run the full PostgreSQL 15–18 matrix when changing extension integration, SQL, hooks, scans, migration, or flushing:

```bash
scripts/run-pgrx-matrix.sh
```

Do not use `cargo pgrx test` for the current E2E suite. The repository scripts install the extension and run the tests serially with the expected configuration.

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
