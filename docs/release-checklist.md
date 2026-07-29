# Release Checklist

- Bump `[workspace.package].version` in `Cargo.toml` (the Release workflow reads
  this and creates tag `v<version>` automatically). `koldstore.control` uses
  `default_version = '@CARGO_VERSION@'`, so packaged `extversion` tracks Cargo.
- Catalog DDL lives in `crates/pg_koldstore/sql/koldstore--0.1.0.sql`. During
  beta/development, edit that bootstrap fragment directly — do **not** add
  `koldstore--<from>--<to>.sql` upgrade edges. Introduce packaged UPDATE edges
  only when intentionally shipping a supported upgrade path.
- Build the full workspace with `cargo check --workspace --all-targets --no-default-features`.
- Run `cargo fmt --all` and `cargo clippy --workspace --all-targets --no-default-features`.
- Run Rust unit and regression tests with `cargo nextest run --workspace --no-default-features --exclude e2e --exclude examples --exclude storage-comparison --exclude stress`.
- Run the supported PostgreSQL 15 through 18 pgrx matrix with `scripts/run-pgrx-matrix.sh`, which runs pgrx feature clippy, extension install, and E2E checks for each version.
- Use `scripts/run-pgrx-matrix.sh --download-missing` when a supported PostgreSQL major is not already initialized in the local pgrx config; add `--without-icu` only for local downloaded builds on machines without ICU development packages.
- Run MinIO-backed flush and merge tests (`flush_minio` via CI, or locally with `bash scripts/ci/start-minio.sh` then `KOLDSTORE_MINIO=1 scripts/run-pg-e2e.sh`).
- Run memory and leak gates with `tests/memory/run_memory_checks.sh`.
- Run benchmarks and confirm SC-002 and SC-006 thresholds.
- Verify extension install (`CREATE EXTENSION`) and `DROP EXTENSION` behavior.
  In-place `ALTER EXTENSION … UPDATE` remains deferred until an upgrade edge is
  intentionally added.
- Review backup/PITR documentation and object-store backup warnings.
- Confirm SQL API, architecture, performance, and operations docs are updated.
- To publish the try-it Docker image, run the Release workflow with `docker_push=true`
  (requires `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` secrets). The Hub token needs
  `read`/`write`/`delete` scope so the job can update the repository overview.
  The job reuses the PG16 `ubuntu24.04` amd64 tarball, installs `pg_cron`, smoke-tests
  `CREATE EXTENSION` / `pg_extension`, then pushes both
  `jamals86/pg-koldstore` (Docker Hub) and `ghcr.io/kalamdb/pg-koldstore`
  (GitHub Packages / GHCR) with `:VERSION-pg16` and `:latest`. After promote it
  PATCHes Docker Hub short description + full overview from
  `docker/image-description.txt` (not the project README). After the first
  GHCR publish, set the package visibility to Public under
  [Packages](https://github.com/orgs/kalamdb/packages) if anonymous pulls should
  work without a GitHub token.
