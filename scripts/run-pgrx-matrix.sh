#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PGRX_VERSION="${PGRX_VERSION:-0.19.1}"
PG_VERSIONS="${PG_VERSIONS:-15,16,17,18}"
DOWNLOAD_MISSING=0
SKIP_UNIT=0
SKIP_CLIPPY=0
SKIP_INSTALL=0
SKIP_E2E=0
CONFIGURE_FLAGS=()

usage() {
  cat <<'EOF'
Run pg-koldstore tests against the supported pgrx PostgreSQL matrix.

Usage:
  scripts/run-pgrx-matrix.sh [options]

Options:
  --pg-versions LIST   Comma-separated PostgreSQL majors (default: 15,16,17,18)
  --download-missing   Let cargo-pgrx download a missing PostgreSQL version
  --skip-unit          Skip non-E2E workspace tests
  --skip-clippy        Skip pg_koldstore pgrx feature clippy checks
  --skip-install       Skip cargo pgrx install checks
  --skip-e2e           Skip local pgrx-backed E2E tests
  --configure-flag ARG Extra flag for cargo pgrx init downloads; repeatable
  --without-icu        Shortcut for --configure-flag=--without-icu
  -h, --help           Show this help text

Examples:
  scripts/run-pgrx-matrix.sh
  scripts/run-pgrx-matrix.sh --download-missing
  scripts/run-pgrx-matrix.sh --download-missing --without-icu
  scripts/run-pgrx-matrix.sh --pg-versions 16,17,18
  scripts/run-pgrx-matrix.sh --pg-versions 18 --skip-unit --skip-e2e
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pg-versions)
      PG_VERSIONS="${2:?missing value for --pg-versions}"
      shift 2
      ;;
    --download-missing) DOWNLOAD_MISSING=1; shift ;;
    --skip-unit) SKIP_UNIT=1; shift ;;
    --skip-clippy) SKIP_CLIPPY=1; shift ;;
    --skip-install) SKIP_INSTALL=1; shift ;;
    --skip-e2e) SKIP_E2E=1; shift ;;
    --configure-flag)
      CONFIGURE_FLAGS+=("${2:?missing value for --configure-flag}")
      shift 2
      ;;
    --without-icu) CONFIGURE_FLAGS+=("--without-icu"); shift ;;
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
  echo >&2
  echo "==> $*" >&2
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

pg_config_major() {
  local pg_config="$1"
  "${pg_config}" --version | sed -E 's/.* ([0-9]+).*/\1/'
}

is_server_pg_config() {
  local pg_config="$1"
  local bindir
  bindir="$(dirname "${pg_config}")"
  [[ -x "${bindir}/postgres" && -x "${bindir}/initdb" ]]
}

print_pg_config_if_usable() {
  local pg="$1"
  local pg_config="$2"

  if [[ ! -x "${pg_config}" ]]; then
    return 1
  fi
  if [[ "$(pg_config_major "${pg_config}")" != "${pg}" ]]; then
    return 1
  fi
  if ! is_server_pg_config "${pg_config}"; then
    echo "warning: ignoring ${pg_config}; PostgreSQL server binaries are not installed beside it" >&2
    return 1
  fi

  echo "${pg_config}"
}

find_pg_config() {
  local pg="$1"
  local candidate

  for candidate in \
    "/usr/lib/postgresql/${pg}/bin/pg_config" \
    "/opt/homebrew/opt/postgresql@${pg}/bin/pg_config" \
    "/usr/local/opt/postgresql@${pg}/bin/pg_config"
  do
    if print_pg_config_if_usable "${pg}" "${candidate}"; then
      return 0
    fi
  done

  if command -v pg_config >/dev/null 2>&1; then
    candidate="$(command -v pg_config)"
    if print_pg_config_if_usable "${pg}" "${candidate}"; then
      return 0
    fi
  fi

  return 1
}

ensure_pgrx_postgres() {
  local pg="$1"
  local pg_config

  if pg_config="$(configured_pg_config "${pg}")"; then
    echo "${pg_config}"
    return 0
  fi

  if pg_config="$(find_pg_config "${pg}")"; then
    step "initializing pgrx for PostgreSQL ${pg}"
    cargo pgrx init --pg"${pg}" "${pg_config}" >&2 || return 1
    configured_pg_config "${pg}" || return 1
    return 0
  fi

  if [[ "${DOWNLOAD_MISSING}" -eq 1 ]]; then
    local init_args=(--pg"${pg}" download)
    local configure_flags=(${CONFIGURE_FLAGS[@]+"${CONFIGURE_FLAGS[@]}"})
    if macos_download_needs_without_icu "${configure_flags[@]+"${configure_flags[@]}"}"; then
      step "ICU development packages are unavailable; downloading PostgreSQL ${pg} with --without-icu"
      configure_flags+=("--without-icu")
    fi
    if ((${#configure_flags[@]} > 0)); then
      local flag
      for flag in "${configure_flags[@]}"; do
        init_args+=(--configure-flag="${flag}")
      done
    fi

    step "downloading pgrx-managed PostgreSQL ${pg}"
    cargo pgrx init "${init_args[@]}" >&2 || return 1
    configured_pg_config "${pg}" || return 1
    return 0
  fi

  echo "error: PostgreSQL ${pg} is not configured for pgrx" >&2
  echo "       run: cargo pgrx init --pg${pg} /path/to/pg_config" >&2
  echo "       or:  scripts/run-pgrx-matrix.sh --download-missing" >&2
  return 1
}

macos_download_needs_without_icu() {
  if [[ "$(uname -s)" != "Darwin" ]]; then
    return 1
  fi
  if pkg-config --exists icu-uc icu-i18n 2>/dev/null; then
    return 1
  fi
  local flag
  for flag in "$@"; do
    if [[ "${flag}" == "--without-icu" ]]; then
      return 1
    fi
  done
  return 0
}

cargo_pgrx_install_koldstore() {
  local pg="$1"
  local pg_config="$2"
  local install_args=(
    -p pg_koldstore
    --no-default-features
    --features "pg${pg}"
    --pg-config "${pg_config}"
  )

  if [[ "${KOLDSTORE_PGRX_INSTALL_SUDO:-}" == "1" || "${KOLDSTORE_PGRX_INSTALL_SUDO:-}" == "true" ]]; then
    install_args+=(--sudo)
  fi

  cargo pgrx install "${install_args[@]}"
}

run_pg_version() {
  local pg="$1"
  local pg_config
  if ! pg_config="$(ensure_pgrx_postgres "${pg}")"; then
    return 1
  fi

  if [[ "${SKIP_CLIPPY}" -eq 0 ]]; then
    step "pgrx clippy pg${pg}"
    cargo clippy -p pg_koldstore --all-targets --no-default-features --features "pg${pg}" -- -D warnings
  fi

  if [[ "${SKIP_INSTALL}" -eq 0 ]]; then
    step "pgrx install pg${pg}"
    cargo_pgrx_install_koldstore "${pg}" "${pg_config}"
  fi

  if [[ "${SKIP_E2E}" -eq 0 ]]; then
    step "pgrx E2E pg${pg}"
    PGRX_PG_CONFIG="${pg_config}" scripts/run-pg-e2e.sh "${pg}"
  fi
}

require_command cargo
ensure_cargo_pgrx

if [[ "${SKIP_UNIT}" -eq 0 ]]; then
  ensure_cargo_nextest
  step "workspace non-E2E tests"
  cargo nextest run --workspace --no-default-features --exclude e2e --exclude examples --exclude storage-comparison
fi

IFS=',' read -r -a pg_versions <<<"${PG_VERSIONS}"
for pg in "${pg_versions[@]}"; do
  pg="$(echo "${pg}" | xargs)"
  [[ -z "${pg}" ]] && continue
  run_pg_version "${pg}"
done
