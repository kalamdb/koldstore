# pg-koldstore Development

## Local Build

```bash
cargo fmt --all
cargo check --workspace --all-targets --no-default-features
cargo test --workspace --no-default-features
```

The extension crate is structured so pure Rust tests compile without a local PostgreSQL install. PostgreSQL-specific pgrx builds use the `pg15`, `pg16`, or `pg17` feature when `cargo pgrx` is configured.

## pgrx Setup

```bash
cargo install cargo-pgrx
cargo pgrx init
tests/e2e/run_pg_matrix.sh
```

The SQL extension name is `koldstore`; public SQL lives in the `koldstore` schema. The local pgrx E2E runner installs the extension into pgrx-managed PostgreSQL and runs the ignored real-PostgreSQL tests. Avoid direct `cargo pgrx test` for now because normal Rust integration tests link as native pg-feature test binaries and can fail on unresolved PostgreSQL server symbols.

## Local pgrx PostgreSQL Matrix

```bash
tests/e2e/run_pg_matrix.sh
```

The default matrix target is pgrx-managed PostgreSQL 16 on port `28816`. Override with `KOLDSTORE_E2E_PGVERSION`, `KOLDSTORE_E2E_PGPORT`, or the other `KOLDSTORE_E2E_PG*` environment variables when needed.

## Benchmark Thresholds

Hot DML benchmark scenarios compare a plain heap table with an equivalent pg-koldstore managed table. The release threshold is at most 10 percent overhead for hot INSERT, UPDATE, and DELETE paths that do not require cold lookup. PK cold lookup pruning must skip at least 90 percent of row groups in the benchmark fixture.

## Memory Checks

```bash
tests/memory/run_memory_checks.sh
```

The script runs Rust tests and opportunistically reports Valgrind and heaptrack availability. PostgreSQL memory-context checks are represented by `tests/memory/memory_probe.rs` and are intended to be wired into pgrx/E2E tests as the extension glue matures.

