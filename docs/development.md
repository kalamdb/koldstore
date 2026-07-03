# pg-koldstore Development

## Local Build

```bash
cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace
```

The extension crate is structured so pure Rust tests compile without a local PostgreSQL install. PostgreSQL-specific pgrx builds use the `pg15`, `pg16`, or `pg17` feature when `cargo pgrx` is configured.

## pgrx Setup

```bash
cargo install cargo-pgrx
cargo pgrx init
cargo pgrx test
```

The SQL extension name is `koldstore`; public SQL lives in the `koldstore` schema.

## PostgreSQL Matrix and MinIO

```bash
docker compose -f tests/docker-compose.yml up -d
tests/e2e/run_pg_matrix.sh
```

The matrix exposes PostgreSQL on ports `5515`, `5516`, and `5517`, with MinIO on `9000`.

## Benchmark Thresholds

Hot DML benchmark scenarios compare a plain heap table with an equivalent pg-koldstore managed table. The release threshold is at most 10 percent overhead for hot INSERT, UPDATE, and DELETE paths that do not require cold lookup. PK cold lookup pruning must skip at least 90 percent of row groups in the benchmark fixture.

## Memory Checks

```bash
tests/memory/run_memory_checks.sh
```

The script runs Rust tests and opportunistically reports Valgrind and heaptrack availability. PostgreSQL memory-context checks are represented by `tests/memory/memory_probe.rs` and are intended to be wired into pgrx/E2E tests as the extension glue matures.

