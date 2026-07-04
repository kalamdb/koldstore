#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"
# shellcheck source=scripts/build-release-common.sh
source "${ROOT_DIR}/scripts/build-release-common.sh"

PG_VER="18"
ARCH="arm64"
DISTRO="macos"
FORMATS="tar.gz"
VERSION=""
PG_CONFIG="${PG_CONFIG:-}"
INSTALL_DEPS=0
SKIP_BUILD=0

usage() {
  cat <<'EOF'
Usage: scripts/build-macos-arm64.sh [options]

Build pg_koldstore release tarball for macOS ARM64.

Options:
  --pg, -v <N>         PostgreSQL major version (default: 18)
  --version <X.Y.Z>    Release version (default: workspace Cargo.toml version)
  --pg-config <path>   pg_config path (default: Homebrew postgresql@N)
  --install-deps       Install LLVM and PostgreSQL via Homebrew
  --skip-build         Package only; skip cargo pgrx package
  -h, --help           Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pg|-v)
      PG_VER="${2:-}"
      shift 2
      ;;
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --pg-config)
      PG_CONFIG="${2:-}"
      shift 2
      ;;
    --install-deps)
      INSTALL_DEPS=1
      shift
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${VERSION}" ]]; then
  VERSION="$(read_workspace_version)"
fi

if [[ "${INSTALL_DEPS}" -eq 1 ]]; then
  require_command brew
  brew install llvm "postgresql@${PG_VER}"
  export LIBCLANG_PATH="$(brew --prefix llvm)/lib"
  export LLVM_CONFIG_PATH="$(brew --prefix llvm)/bin/llvm-config"
  PG_CONFIG="$(brew --prefix "postgresql@${PG_VER}")/bin/pg_config"
fi

if [[ -z "${PG_CONFIG}" ]]; then
  require_command brew
  PG_CONFIG="$(brew --prefix "postgresql@${PG_VER}")/bin/pg_config"
fi

if [[ ! -x "${PG_CONFIG}" ]]; then
  echo "error: pg_config not found or not executable: ${PG_CONFIG}" >&2
  exit 1
fi

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  echo "building pg_koldstore for PostgreSQL ${PG_VER} using ${PG_CONFIG}"
  run_cargo_pgrx_package "${PG_VER}" "${PG_CONFIG}"
fi

echo "packaging pg_koldstore v${VERSION} pg${PG_VER} ${DISTRO} ${ARCH} (${FORMATS})"
build_release_artifacts "${VERSION}" "${PG_VER}" "${DISTRO}" "${ARCH}" "${FORMATS}"
