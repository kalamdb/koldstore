#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PGRX_VERSION="0.19.1"
PG_VERSIONS="16"
SKIP_FMT=0
SKIP_LINT=0
SKIP_UNIT=0
SKIP_PGRX=0
SKIP_PG_TEST=0
SKIP_E2E=0
SKIP_EXAMPLES=0
SKIP_STORAGE=0
SKIP_SQL=0
SKIP_MEMORY=0
SKIP_BENCHMARKS=0

usage() {
  cat <<'EOF'
Run the full pg-koldstore verification suite.

Usage:
  scripts/run-all-tests.sh [options]

Runs (in order):
  fmt, clippy, workspace unit tests (nextest), pgrx feature compile/install,
  #[pg_test] via nextest, E2E (strict + async, nextest), examples (strict + async),
  storage comparison, SQL regression, memory checks, short benchmarks.

Options:
  --pg-versions LIST   Comma-separated PostgreSQL majors (default: 16)
  --skip-fmt           Skip cargo fmt --check
  --skip-lint          Skip cargo clippy
  --skip-unit          Skip workspace unit tests
  --skip-pgrx          Skip pgrx feature compile/install checks
  --skip-pg-test       Skip in-server #[pg_test] suite (nextest)
  --skip-e2e           Skip local pgrx-backed E2E (both modes)
  --skip-examples      Skip real-world example scenarios
  --skip-storage       Skip storage comparison harness
  --skip-sql           Skip KoldStore SQL regression
  --skip-memory        Skip memory checks
  --skip-benchmarks    Skip benchmark runner
  -h, --help           Show this help text

Environment (optional overrides for heavy suites; CI-friendly defaults apply):
  KOLDSTORE_EXAMPLE_ROWS / CLIENTS / SCOPES / TIMEOUT_SECS
  KOLDSTORE_STORAGE_ROWS / HOT_LIMIT / DML_SAMPLE

Examples:
  scripts/run-all-tests.sh
  scripts/run-all-tests.sh --pg-versions 15,16,17,18
  scripts/run-all-tests.sh --skip-benchmarks --skip-examples
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pg-versions)
      PG_VERSIONS="${2:?missing value for --pg-versions}"
      shift 2
      ;;
    --skip-fmt) SKIP_FMT=1; shift ;;
    --skip-lint) SKIP_LINT=1; shift ;;
    --skip-unit) SKIP_UNIT=1; shift ;;
    --skip-pgrx) SKIP_PGRX=1; shift ;;
    --skip-pg-test) SKIP_PG_TEST=1; shift ;;
    --skip-e2e) SKIP_E2E=1; shift ;;
    --skip-examples) SKIP_EXAMPLES=1; shift ;;
    --skip-storage) SKIP_STORAGE=1; shift ;;
    --skip-sql) SKIP_SQL=1; shift ;;
    --skip-memory) SKIP_MEMORY=1; shift ;;
    --skip-benchmarks) SKIP_BENCHMARKS=1; shift ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

step() {
  echo
  echo "==> $*"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

ensure_cargo_nextest() {
  if ! command -v cargo-nextest >/dev/null 2>&1; then
    step "installing cargo-nextest"
    cargo install cargo-nextest --locked
  fi
}

ensure_cargo_pgrx() {
  if ! command -v cargo-pgrx >/dev/null 2>&1; then
    step "installing cargo-pgrx ${PGRX_VERSION}"
    cargo install cargo-pgrx --version "${PGRX_VERSION}" --locked
    return
  fi

  local installed_version
  installed_version="$(cargo pgrx --version | awk '{print $2}')"
  if [[ "${installed_version}" != "${PGRX_VERSION}" ]]; then
    step "installing cargo-pgrx ${PGRX_VERSION} (found ${installed_version})"
    cargo install cargo-pgrx --version "${PGRX_VERSION}" --locked
  fi
}

configured_pg_config() {
  local pg="$1"
  cargo pgrx info pg-config "${pg}" 2>/dev/null
}

ensure_pgrx_postgres() {
  local pg="$1"
  if configured_pg_config "${pg}" >/dev/null; then
    return 0
  fi

  local pg_config=""
  if command -v "pg_config" >/dev/null 2>&1; then
    local major
    major="$(pg_config --version | sed -E 's/.* ([0-9]+).*/\1/')"
    if [[ "${major}" == "${pg}" ]]; then
      pg_config="$(command -v pg_config)"
    fi
  fi

  if [[ -z "${pg_config}" && -x "/usr/lib/postgresql/${pg}/bin/pg_config" ]]; then
    pg_config="/usr/lib/postgresql/${pg}/bin/pg_config"
  fi

  if [[ -z "${pg_config}" ]]; then
    echo "warning: PostgreSQL ${pg} is not configured for pgrx; skipping pgrx-backed suites for pg${pg}" >&2
    echo "         run: cargo pgrx init --pg${pg} /path/to/pg_config" >&2
    return 1
  fi

  step "initializing pgrx for PostgreSQL ${pg}"
  cargo pgrx init --pg"${pg}" "${pg_config}"
}

cargo_pgrx_install_koldstore() {
  local pg_feature="$1"
  local pg_config="$2"
  local install_args=(
    -p pg_koldstore
    --no-default-features
    --features "${pg_feature} s3"
    --pg-config "${pg_config}"
  )

  if [[ "${KOLDSTORE_PGRX_INSTALL_SUDO:-}" == "1" || "${KOLDSTORE_PGRX_INSTALL_SUDO:-}" == "true" ]]; then
    install_args+=(--sudo)
  fi

  cargo pgrx install "${install_args[@]}"
}

run_workspace_unit_tests() {
  # Exclude suites that need a prepared pgrx cluster or are covered later.
  local unit_excludes=(
    --exclude e2e
    --exclude examples
    --exclude storage-comparison
    --exclude pg-koldstore-benchmarks
    --exclude koldstore-memory-tests
    --exclude stress
  )

  ensure_cargo_nextest
  step "workspace unit tests (cargo nextest)"
  cargo nextest run --workspace --no-default-features "${unit_excludes[@]}"
}

run_local_pgrx_e2e() {
  local pg="$1"
  local mode="$2"
  local pg_config
  pg_config="$(configured_pg_config "${pg}")"
  local port="${KOLDSTORE_E2E_PGPORT:-288${pg}}"

  step "local pgrx E2E PostgreSQL ${pg} (--mode ${mode})"
  KOLDSTORE_E2E_PGVERSION="${pg}" \
    KOLDSTORE_E2E_PGPORT="${port}" \
    PGRX_PG_CONFIG="${pg_config}" \
    scripts/run-pg-e2e.sh "${pg}" --mode "${mode}"
}

run_local_pg_test() {
  local pg="$1"
  local features="pg${pg} pg_test s3"
  local manifest="${ROOT_DIR}/crates/pg_koldstore/Cargo.toml"
  local target_dir="${CARGO_TARGET_DIR:-${ROOT_DIR}/target}"

  ensure_cargo_nextest
  step "#[pg_test] via cargo nextest (PostgreSQL ${pg})"
  # Mirror the env `cargo pgrx test` passes into its inner `cargo test`, but run
  # nextest directly — setting CARGO=shim fails because outer cargo resets CARGO.
  CARGO_TARGET_DIR="${target_dir}" \
    PGRX_FEATURES="${features}" \
    PGRX_NO_DEFAULT_FEATURES=true \
    PGRX_ALL_FEATURES=false \
    PGRX_BUILD_PROFILE=dev \
    PGRX_NO_SCHEMA=false \
    PGRX_MANIFEST_PATH="${manifest}" \
    cargo nextest run \
      --manifest-path "${manifest}" \
      --features "${features}" \
      --no-default-features \
      --test-threads 1
}

run_local_examples() {
  local pg="$1"
  local mode="$2"

  step "example scenarios PostgreSQL ${pg} (--mode ${mode})"
  # CI-friendly defaults when the caller has not sized the suite.
  KOLDSTORE_EXAMPLE_PGVERSION="${pg}" \
    KOLDSTORE_EXAMPLE_ROWS="${KOLDSTORE_EXAMPLE_ROWS:-2000}" \
    KOLDSTORE_EXAMPLE_CLIENTS="${KOLDSTORE_EXAMPLE_CLIENTS:-4}" \
    KOLDSTORE_EXAMPLE_SCOPES="${KOLDSTORE_EXAMPLE_SCOPES:-8}" \
    KOLDSTORE_EXAMPLE_TIMEOUT_SECS="${KOLDSTORE_EXAMPLE_TIMEOUT_SECS:-600}" \
    scripts/run-examples.sh --mode "${mode}"
}

run_local_storage() {
  local pg="$1"

  step "storage comparison PostgreSQL ${pg}"
  KOLDSTORE_STORAGE_ROWS="${KOLDSTORE_STORAGE_ROWS:-10000}" \
    KOLDSTORE_STORAGE_HOT_LIMIT="${KOLDSTORE_STORAGE_HOT_LIMIT:-2000}" \
    KOLDSTORE_STORAGE_DML_SAMPLE="${KOLDSTORE_STORAGE_DML_SAMPLE:-1000}" \
    scripts/run-storage-comparison.sh --all-sides --pg-version "${pg}"
}

run_local_sql() {
  local pg="$1"
  local pg_config
  pg_config="$(configured_pg_config "${pg}")"

  step "SQL regression PostgreSQL ${pg}"
  PGRX_PG_CONFIG="${pg_config}" \
    scripts/run-sql-regression.sh "${pg}"
}

first_pg_version() {
  local pg
  for pg in "${pg_versions[@]}"; do
    pg="$(echo "${pg}" | xargs)"
    if [[ -n "${pg}" ]]; then
      echo "${pg}"
      return 0
    fi
  done
  return 1
}

BENCHMARK_DATABASE_URL=""

prepare_benchmark_database() {
  local pg="$1"
  local pg_config
  pg_config="$(configured_pg_config "${pg}")"
  local psql
  psql="$(dirname "${pg_config}")/psql"
  local pg_feature="pg${pg}"
  local host="${KOLDSTORE_BENCH_PGHOST:-127.0.0.1}"
  local port="${KOLDSTORE_BENCH_PGPORT:-288${pg}}"
  local user="${KOLDSTORE_BENCH_PGUSER:-$(whoami)}"
  local database="${KOLDSTORE_BENCH_PGDATABASE:-koldstore_pgrx_bench}"

  export PATH="$(dirname "${pg_config}"):${PATH}"

  step "preparing local pgrx benchmark database PostgreSQL ${pg}"
  cargo pgrx start "${pg_feature}" --postgresql-conf wal_level=logical
  cargo_pgrx_install_koldstore "${pg_feature}" "${pg_config}"
  cargo pgrx stop "${pg_feature}" >/dev/null 2>&1 || true
  cargo pgrx start "${pg_feature}" \
    --postgresql-conf wal_level=logical \
    --postgresql-conf shared_preload_libraries=koldstore

  "${psql}" -h "${host}" -p "${port}" -d postgres -v ON_ERROR_STOP=1 \
    -c "DROP DATABASE IF EXISTS ${database}" \
    -c "CREATE DATABASE ${database}"

  BENCHMARK_DATABASE_URL="host=${host} port=${port} user=${user} dbname=${database}"
}

require_command cargo
ensure_cargo_nextest

if [[ "${SKIP_FMT}" -eq 0 ]]; then
  step "cargo fmt --check"
  cargo fmt --all -- --check
fi

if [[ "${SKIP_LINT}" -eq 0 ]]; then
  step "cargo clippy --workspace --no-default-features"
  cargo clippy --workspace --all-targets --no-default-features -- -D warnings
fi

if [[ "${SKIP_UNIT}" -eq 0 ]]; then
  run_workspace_unit_tests
fi

IFS=',' read -r -a pg_versions <<<"${PG_VERSIONS}"

if [[ "${SKIP_PGRX}" -eq 0 ]]; then
  ensure_cargo_pgrx
  for pg in "${pg_versions[@]}"; do
    pg="$(echo "${pg}" | xargs)"
    [[ -z "${pg}" ]] && continue
    if ensure_pgrx_postgres "${pg}"; then
      step "pgrx feature compile check pg${pg}"
      cargo clippy -p pg_koldstore --all-targets --no-default-features --features "pg${pg} pg_test s3" -- -D warnings

      step "pgrx install check pg${pg}"
      cargo_pgrx_install_koldstore "pg${pg}" "$(configured_pg_config "${pg}")"
    fi
  done
fi

if [[ "${SKIP_PG_TEST}" -eq 0 ]]; then
  ensure_cargo_pgrx
  for pg in "${pg_versions[@]}"; do
    pg="$(echo "${pg}" | xargs)"
    [[ -z "${pg}" ]] && continue
    if ensure_pgrx_postgres "${pg}"; then
      run_local_pg_test "${pg}"
      # #[pg_test] builds with the pg_test feature and can overwrite the installed
      # shared library. Reinstall the production feature set before E2E/examples.
      step "reinstall production koldstore after #[pg_test] (PostgreSQL ${pg})"
      cargo_pgrx_install_koldstore "pg${pg}" "$(configured_pg_config "${pg}")"
    fi
  done
fi

if [[ "${SKIP_E2E}" -eq 0 ]]; then
  ensure_cargo_pgrx
  for pg in "${pg_versions[@]}"; do
    pg="$(echo "${pg}" | xargs)"
    [[ -z "${pg}" ]] && continue
    if ensure_pgrx_postgres "${pg}"; then
      run_local_pgrx_e2e "${pg}" strict
      run_local_pgrx_e2e "${pg}" async
    fi
  done
fi

if [[ "${SKIP_EXAMPLES}" -eq 0 ]]; then
  ensure_cargo_pgrx
  for pg in "${pg_versions[@]}"; do
    pg="$(echo "${pg}" | xargs)"
    [[ -z "${pg}" ]] && continue
    if ensure_pgrx_postgres "${pg}"; then
      run_local_examples "${pg}" strict
      run_local_examples "${pg}" async
    fi
  done
fi

if [[ "${SKIP_STORAGE}" -eq 0 ]]; then
  ensure_cargo_pgrx
  for pg in "${pg_versions[@]}"; do
    pg="$(echo "${pg}" | xargs)"
    [[ -z "${pg}" ]] && continue
    if ensure_pgrx_postgres "${pg}"; then
      run_local_storage "${pg}"
    fi
  done
fi

if [[ "${SKIP_SQL}" -eq 0 ]]; then
  ensure_cargo_pgrx
  for pg in "${pg_versions[@]}"; do
    pg="$(echo "${pg}" | xargs)"
    [[ -z "${pg}" ]] && continue
    if ensure_pgrx_postgres "${pg}"; then
      run_local_sql "${pg}"
    fi
  done
fi

if [[ "${SKIP_MEMORY}" -eq 0 ]]; then
  step "memory checks"
  tests/memory/run_memory_checks.sh
fi

if [[ "${SKIP_BENCHMARKS}" -eq 0 ]]; then
  benchmark_database_url="${DATABASE_URL:-}"
  if [[ -z "${benchmark_database_url}" ]]; then
    ensure_cargo_pgrx
    benchmark_pg="$(first_pg_version)"
    if ensure_pgrx_postgres "${benchmark_pg}"; then
      prepare_benchmark_database "${benchmark_pg}"
      benchmark_database_url="${BENCHMARK_DATABASE_URL}"
    fi
  fi
  if [[ -z "${benchmark_database_url}" ]]; then
    echo "error: no PostgreSQL database URL available for benchmarks" >&2
    exit 1
  fi

  step "benchmarks"
  cargo run -p pg-koldstore-benchmarks -- \
    --database-url "${benchmark_database_url}" \
    --rows 1000 \
    --clients 2 \
    --jobs 2 \
    --seconds 10
fi

step "all requested test suites passed"
