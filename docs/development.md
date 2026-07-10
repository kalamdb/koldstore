# pg-koldstore Development

## Local Build

```bash
cargo fmt --all
cargo check --workspace --all-targets --no-default-features
cargo nextest run --workspace --no-default-features --exclude e2e --exclude examples --exclude storage-comparison
```

`e2e`, `examples`, and `storage-comparison` need a running pgrx PostgreSQL; run them via `scripts/run-pg-e2e.sh`, `scripts/run-examples.sh`, and `scripts/run-storage-comparison.sh`.

The extension crate is structured so pure Rust tests compile without a local PostgreSQL install. PostgreSQL-specific pgrx builds use the `pg15`, `pg16`, `pg17`, or `pg18` feature when `cargo pgrx` is configured.

## pgrx Setup

```bash
cargo install cargo-pgrx
cargo pgrx init
scripts/run-pg-e2e.sh
```

The SQL extension name is `koldstore`; public SQL lives in the `koldstore` schema. The local pgrx E2E runner installs the extension into pgrx-managed PostgreSQL and runs the E2E crate serially against that server. Avoid direct `cargo pgrx test` for now because normal Rust integration tests link as native pg-feature test binaries and can fail on unresolved PostgreSQL server symbols.

## Local pgrx PostgreSQL Matrix

```bash
scripts/run-pgrx-matrix.sh
```

The matrix runner executes non-E2E workspace tests once, then loops over PostgreSQL 15, 16, 17, and 18 for pgrx feature clippy, extension install, and E2E checks. Use `scripts/run-pgrx-matrix.sh --download-missing` to let cargo-pgrx download missing PostgreSQL versions. On local machines without ICU development packages, add `--without-icu` for downloaded PostgreSQL builds.

For a single version, use `scripts/run-pg-e2e.sh 18` or `scripts/run-pgrx-matrix.sh --pg-versions 18`. After the E2E runner prepares PostgreSQL and installs the extension, it executes `cargo nextest run -p e2e --test-threads 1`.

Every E2E test now calls a shared pgrx gate before running. The gate connects to the configured PostgreSQL port, verifies the server major version and listening port, and ensures `koldstore` is installed in the E2E database. If pgrx PostgreSQL is stopped or unreachable, the suite fails fast instead of letting contract-only tests pass.

## MinIO / S3-backed E2E

Most E2E fixtures use local filesystem cold storage. The `flush_minio` test exercises flush + merge-scan against a real S3-compatible MinIO endpoint. It is opt-in and skipped unless enabled:

```bash
# Start MinIO + create the koldstore-test bucket (Docker required):
bash scripts/ci/start-minio.sh

export KOLDSTORE_MINIO=1
export KOLDSTORE_MINIO_ENDPOINT=http://127.0.0.1:9000
export KOLDSTORE_MINIO_ACCESS_KEY=minioadmin
export KOLDSTORE_MINIO_SECRET_KEY=minioadmin
export KOLDSTORE_MINIO_BUCKET=koldstore-test

scripts/run-pg-e2e.sh 16
```

CI starts MinIO before the pgrx E2E job so `flush_minio` runs on every PostgreSQL matrix entry.

Low-level storage-client MinIO tests remain available as:

```bash
KOLDSTORE_MINIO=1 cargo test -p koldstore-storage --test storage_minio
```

## Published try-it Docker image

Release builds can publish a PostgreSQL 16 image with prebuilt `koldstore` and
`pg_cron` (no extension rebuild) to Docker Hub (`jamals86/pg-koldstore` by
default). Enable `docker_push` on the Release workflow after setting
`DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN`.

```bash
docker pull jamals86/pg-koldstore:latest
docker run --rm -e POSTGRES_PASSWORD=postgres -p 5432:5432 jamals86/pg-koldstore:latest
# psql postgres://postgres:postgres@127.0.0.1:5432/koldstore
# koldstore + pg_cron are already created on first boot
# shared_preload_libraries includes pg_cron (koldstore is SQL-loaded; use pg_cron to schedule flush)
```

Local source builds still use `docker/run.sh` / `docker/Dockerfile` (compiles the
extension). The release image uses `docker/Dockerfile.release` and
`docker/test-release-image.sh`.

## pg_cron periodic flush (manual)

Flush is on-demand unless you schedule it. To verify the README `pg_cron` recipe
against local pgrx PostgreSQL (builds/installs `pg_cron` if needed, waits for a
one-minute cron tick):

```bash
scripts/run-test-with-cron.sh
scripts/run-test-with-cron.sh --pg-version 16
scripts/run-test-with-cron.sh --skip-prepare   # reuse an already-prepared DB
```

This is intentionally outside the default E2E/CI loop because `pg_cron` needs
`shared_preload_libraries` and a ~1–2 minute wait for the scheduler.

## Benchmark Thresholds

Hot DML benchmark scenarios compare a plain heap table with an equivalent pg-koldstore managed table. The release threshold is at most 10 percent overhead for hot INSERT, UPDATE, and DELETE paths that do not require cold lookup. PK cold lookup pruning must skip at least 90 percent of row groups in the benchmark fixture.

## Memory Checks

```bash
tests/memory/run_memory_checks.sh
```

The script runs Rust tests and opportunistically reports Valgrind and heaptrack availability. PostgreSQL memory-context checks are represented by `tests/memory/memory_probe.rs` and are intended to be wired into pgrx/E2E tests as the extension glue matures.

