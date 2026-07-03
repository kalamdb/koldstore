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
SKIP_E2E=0
SKIP_MEMORY=0
SKIP_BENCHMARKS=0

usage() {
  cat <<'EOF'
Run the full pg-koldstore verification suite.

Usage:
  scripts/run-all-tests.sh [options]

Options:
  --pg-versions LIST   Comma-separated PostgreSQL majors for pgrx tests (default: 16)
  --skip-fmt           Skip cargo fmt --check
  --skip-lint          Skip cargo clippy
  --skip-unit          Skip cargo test --workspace
  --skip-pgrx          Skip cargo pgrx test
  --skip-e2e           Skip Docker-backed E2E matrix
  --skip-memory        Skip memory checks
  --skip-benchmarks    Skip benchmark runner
  -h, --help           Show this help text

Examples:
  scripts/run-all-tests.sh
  scripts/run-all-tests.sh --pg-versions 15,16,17
  scripts/run-all-tests.sh --skip-e2e --skip-benchmarks
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
    --skip-e2e) SKIP_E2E=1; shift ;;
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

ensure_pgrx_postgres() {
  local pg="$1"
  if cargo pgrx info 2>/dev/null | grep -q "pg${pg} "; then
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
    echo "warning: PostgreSQL ${pg} is not configured for pgrx; skipping pgrx tests for pg${pg}" >&2
    echo "         run: cargo pgrx init --pg${pg} /path/to/pg_config" >&2
    return 1
  fi

  step "initializing pgrx for PostgreSQL ${pg}"
  cargo pgrx init --pg"${pg}" "${pg_config}"
}

require_command cargo

if [[ "${SKIP_FMT}" -eq 0 ]]; then
  step "cargo fmt --check"
  cargo fmt --all -- --check
fi

if [[ "${SKIP_LINT}" -eq 0 ]]; then
  step "cargo clippy"
  cargo clippy --workspace --all-targets -- -D warnings
fi

if [[ "${SKIP_UNIT}" -eq 0 ]]; then
  step "cargo test --workspace"
  cargo test --workspace
fi

if [[ "${SKIP_PGRX}" -eq 0 ]]; then
  ensure_cargo_pgrx
  IFS=',' read -r -a pg_versions <<<"${PG_VERSIONS}"
  for pg in "${pg_versions[@]}"; do
    pg="$(echo "${pg}" | xargs)"
    [[ -z "${pg}" ]] && continue
    if ensure_pgrx_postgres "${pg}"; then
      step "cargo pgrx test pg${pg}"
      cargo pgrx test "pg${pg}" -p pg_koldstore --no-default-features --features "pg${pg}"
    fi
  done
fi

if [[ "${SKIP_E2E}" -eq 0 ]]; then
  require_command docker
  step "E2E PostgreSQL matrix"
  tests/e2e/run_pg_matrix.sh
fi

if [[ "${SKIP_MEMORY}" -eq 0 ]]; then
  step "memory checks"
  tests/memory/run_memory_checks.sh
fi

if [[ "${SKIP_BENCHMARKS}" -eq 0 ]]; then
  step "benchmarks"
  cargo run -p pg-koldstore-benchmarks -- --suite all
fi

step "all requested test suites passed"
