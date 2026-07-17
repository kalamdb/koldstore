# pg-koldstore Development

## Full local aggregator

```bash
scripts/run-all-tests.sh
```

Runs fmt, clippy, workspace unit tests (nextest), pgrx compile/install,
in-server `#[pg_test]` via nextest, E2E in **both** `--mode strict` and
`--mode async` (nextest), examples, storage comparison, SQL regression,
memory checks, and a short benchmark. Use `--skip-*` flags to narrow the run;
example/storage sizing defaults match CI (`2000` / `10000` rows).

## Local Build

```bash
cargo fmt --all
cargo check --workspace --all-targets --no-default-features
cargo nextest run --workspace --no-default-features \
  --exclude e2e --exclude examples --exclude storage-comparison \
  --exclude pg-koldstore-benchmarks --exclude koldstore-memory-tests
```

`e2e`, `examples`, and `storage-comparison` need a running pgrx PostgreSQL; run them via `scripts/run-pg-e2e.sh`, `scripts/run-examples.sh`, and `scripts/run-storage-comparison.sh`.

## Production-readiness test layers

```bash
# Unit
cargo nextest run --workspace --no-default-features \
  --exclude e2e --exclude examples --exclude storage-comparison \
  --exclude pg-koldstore-benchmarks --exclude koldstore-memory-tests

# In-server pgrx #[pg_test]
RUST_TEST_THREADS=1 cargo pgrx test --manifest-path crates/pg_koldstore/Cargo.toml pg16

# KoldStore SQL regression (normalization rules in tests/sql/README.md)
scripts/run-sql-regression.sh 16

# E2E: run the same suite once per mirror mode
scripts/run-pg-e2e.sh 16 --mode strict
scripts/run-pg-e2e.sh 16 --mode async

# Isolation (two-session schedules; no sleep-based races)
scripts/readiness/run-isolation.sh 16

# Crash / failpoint recovery (GUC koldstore.failpoint; see failpoints.rs)
scripts/readiness/run-crash-recovery.sh 16
# Full matrix: KOLDSTORE_CRASH_FULL_MATRIX=1 scripts/readiness/run-crash-recovery.sh 16

# SQLsmith (skips if not installed). CI default 30s; nightly may use 600.
KOLDSTORE_SQLSMITH_SECONDS=30 scripts/readiness/run-sqlsmith.sh 16

# Integrity (pg_amcheck if available + KS catalog queries)
scripts/readiness/run-integrity-checks.sh 16

# Optional upstream PG installcheck — external confidence signal only
scripts/readiness/run-upstream-pg-regress.sh 16

# HammerDB (skips if not installed; manage append-heavy tables only)
scripts/readiness/run-hammerdb.sh 16

# Readiness report (never claims "production safe")
scripts/readiness/run-readiness-report.sh 16
```

Nightly workflow: `.github/workflows/nightly-readiness.yml` (isolation, crash, SQLsmith, integrity).
Weekly HammerDB: `.github/workflows/weekly-hammerdb.yml`.
Script layout: `scripts/README.md` (everyday runners at top level; readiness/CI/build in subfolders).

PR / main CI: `.github/workflows/ci-tests.yml` runs fmt/clippy/unit and
`cargo pgrx test` across PostgreSQL 15–18. Async E2E also covers PostgreSQL
15–18; strict E2E currently runs on PostgreSQL 16. Examples, storage comparison,
and SQL regression retain their PostgreSQL matrix. Manual workflow runs expose
two dropdowns: PostgreSQL (`All`, 15, 16, 17, or 18) and E2E mode (`Both`,
`Async`, or `Strict`). Selecting strict with a PostgreSQL filter other than
`All` or `16` intentionally schedules no strict E2E job.

The extension crate is structured so pure Rust tests compile without a local PostgreSQL install. PostgreSQL-specific pgrx builds use the `pg15`, `pg16`, `pg17`, or `pg18` feature when `cargo pgrx` is configured.

## pgrx Setup

```bash
cargo install cargo-pgrx
cargo pgrx init
scripts/run-pg-e2e.sh
```

The SQL extension name is `koldstore`; public SQL lives in the `koldstore` schema. The local pgrx E2E runner installs the extension into pgrx-managed PostgreSQL and runs the E2E crate serially against that server. `--mode strict` is the default; `--mode async` enables logical WAL and runs the same fixtures with async capture. Async-only publication, slot, and worker lifecycle assertions skip themselves in strict mode. Prefer `scripts/run-pg-e2e.sh` for multi-process E2E; use
`RUST_TEST_THREADS=1 cargo pgrx test` for in-server `#[pg_test]` modules under
`crates/pg_koldstore/src/pg_tests/` (one shared DB/slot; parallel tests race).

## Local pgrx PostgreSQL Matrix

```bash
scripts/run-pgrx-matrix.sh
```

The matrix runner executes non-E2E workspace tests once, then loops over PostgreSQL 15, 16, 17, and 18 for pgrx feature clippy, extension install, and E2E checks. Use `scripts/run-pgrx-matrix.sh --download-missing` to let cargo-pgrx download missing PostgreSQL versions. On local machines without ICU development packages, add `--without-icu` for downloaded PostgreSQL builds.

For a single version and mode, use `scripts/run-pg-e2e.sh 18 --mode async`; use `scripts/run-pgrx-matrix.sh --pg-versions 18` for the version matrix. After the E2E runner prepares PostgreSQL and installs the extension, it executes the E2E crate serially with the selected mode exported as `KOLDSTORE_E2E_MIRROR_CAPTURE_MODE`.

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
KOLDSTORE_MINIO=1 cargo nextest run -p koldstore-storage --test storage_minio
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

Flush is on-demand unless you schedule it. Operator recipe:
[operations/scheduling.md](operations/scheduling.md).

To verify that recipe against local pgrx PostgreSQL (builds/installs `pg_cron`
if needed, waits for a one-minute cron tick):

```bash
scripts/readiness/run-test-with-cron.sh
scripts/readiness/run-test-with-cron.sh --pg-version 16
scripts/readiness/run-test-with-cron.sh --skip-prepare   # reuse an already-prepared DB
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
