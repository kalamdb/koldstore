#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"
# shellcheck source=scripts/build-release-common.sh
source "${ROOT_DIR}/scripts/build-release-common.sh"

PG_VER=""
ARCH=""
VERSION=""
DISTRO=""
FORMATS=""
PG_CONFIG="${PG_CONFIG:-}"
INSTALL_DEPS=0
SKIP_BUILD=0

usage() {
  cat <<'EOF'
Usage: scripts/build-linux.sh --pg <15|16|17|18> --arch <amd64|arm64> [options]

Build pg_koldstore release artifacts for Linux.

Options:
  --pg, -v <N>         PostgreSQL major version (required)
  --arch <amd64|arm64> Target CPU architecture (required)
  --version <X.Y.Z>    Release version (default: workspace Cargo.toml version)
  --distro <name>      Distribution label for filenames (default: mapped from --pg)
  --formats <list>     Comma-separated formats: deb,rpm,tar.gz (default: mapped from --pg)
  --pg-config <path>   pg_config path (default: discovered after --install-deps)
  --install-deps       Install PostgreSQL/pgrx build dependencies for the selected distro
  --skip-build         Package only; skip cargo pgrx package (expects existing target/release tree)
  -h, --help           Show this help

Examples:
  scripts/build-linux.sh --pg 16 --arch amd64 --install-deps
  scripts/build-linux.sh -v 18 --arch arm64 --distro rocky9 --install-deps
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pg|-v)
      PG_VER="${2:-}"
      shift 2
      ;;
    --arch)
      ARCH="${2:-}"
      shift 2
      ;;
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --distro)
      DISTRO="${2:-}"
      shift 2
      ;;
    --formats)
      FORMATS="${2:-}"
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

if [[ -z "${PG_VER}" || -z "${ARCH}" ]]; then
  echo "error: --pg and --arch are required" >&2
  usage >&2
  exit 1
fi

if [[ -z "${VERSION}" ]]; then
  VERSION="$(read_workspace_version)"
fi

if [[ -z "${DISTRO}" ]]; then
  DISTRO="$(default_distro_for_pg "${PG_VER}")"
fi

if [[ -z "${FORMATS}" ]]; then
  FORMATS="$(default_formats_for_pg "${PG_VER}")"
fi

install_ubuntu_deps() {
  local pg="$1"
  require_command sudo
  sudo apt-get update
  sudo apt-get install -y --no-install-recommends ca-certificates curl gnupg lsb-release
  sudo install -d /usr/share/postgresql-common/pgdg
  curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    | sudo gpg --batch --yes --dearmor -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.gpg
  echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.gpg] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
    | sudo tee /etc/apt/sources.list.d/pgdg.list >/dev/null
  sudo apt-get update
  sudo apt-get install -y --no-install-recommends \
    build-essential \
    clang \
    dpkg-dev \
    libclang-dev \
    libreadline-dev \
    libssl-dev \
    pkg-config \
    "postgresql-${pg}" \
    "postgresql-server-dev-${pg}"
}

install_debian_deps() {
  local pg="$1"
  require_command apt-get
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends ca-certificates curl gnupg
  install -d /usr/share/keyrings
  curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    | gpg --dearmor -o /usr/share/keyrings/postgresql.gpg
  echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
    > /etc/apt/sources.list.d/pgdg.list
  apt-get update
  apt-get install -y --no-install-recommends \
    build-essential \
    clang \
    curl \
    dpkg-dev \
    git \
    libclang-dev \
    libreadline-dev \
    libssl-dev \
    pkg-config \
    "postgresql-${pg}" \
    "postgresql-server-dev-${pg}"
}

install_rocky_deps() {
  local pg="$1"
  require_command dnf
  local rpm_arch
  rpm_arch="$(rpm_arch_name "${ARCH}")"
  dnf -y install epel-release
  dnf config-manager --set-enabled crb
  dnf -y install \
    "https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-${rpm_arch}/pgdg-redhat-repo-latest.noarch.rpm"
  dnf -qy module disable postgresql || true
  dnf -y install --allowerasing \
    curl \
    gcc \
    gcc-c++ \
    git \
    make \
    openssl-devel \
    perl-IPC-Run \
    pkgconfig \
    redhat-rpm-config \
    rpm-build \
    "postgresql${pg}" \
    "postgresql${pg}-devel"
}

install_linux_build_deps() {
  case "${DISTRO}" in
    ubuntu22.04|ubuntu24.04) install_ubuntu_deps "${PG_VER}" ;;
    debian12) install_debian_deps "${PG_VER}" ;;
    rocky9) install_rocky_deps "${PG_VER}" ;;
    *)
      echo "error: --install-deps is not supported for distro ${DISTRO}" >&2
      exit 1
      ;;
  esac
}

if [[ "${INSTALL_DEPS}" -eq 1 ]]; then
  install_linux_build_deps
fi

if [[ -z "${PG_CONFIG}" ]]; then
  case "${DISTRO}" in
    ubuntu22.04|ubuntu24.04|debian12)
      PG_CONFIG="/usr/lib/postgresql/${PG_VER}/bin/pg_config"
      ;;
    rocky9)
      PG_CONFIG="/usr/pgsql-${PG_VER}/bin/pg_config"
      ;;
    *)
      echo "error: set --pg-config for distro ${DISTRO}" >&2
      exit 1
      ;;
  esac
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
