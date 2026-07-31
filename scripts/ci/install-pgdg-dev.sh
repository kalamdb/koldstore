#!/usr/bin/env bash
# Install PGDG PostgreSQL + common build deps on Ubuntu GitHub runners.
#
# Usage:
#   scripts/ci/install-pgdg-dev.sh <pg_major> [extra apt packages...]
#   scripts/ci/install-pgdg-dev.sh 16
#   scripts/ci/install-pgdg-dev.sh 16 postgresql-client-16 postgresql-contrib-16
#   scripts/ci/install-pgdg-dev.sh 16 --no-install-recommends clang libclang-dev
#
# Always installs: build-essential, libssl-dev, pkg-config,
# postgresql-<ver>, postgresql-server-dev-<ver>, plus bootstrap tools for PGDG.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <pg_major> [--no-install-recommends] [extra packages...]" >&2
  exit 2
fi

PG_VER="$1"
shift

if ! [[ "${PG_VER}" =~ ^[0-9]+$ ]]; then
  echo "error: pg_major must be an integer (got '${PG_VER}')" >&2
  exit 2
fi

NO_INSTALL_RECOMMENDS=0
EXTRA=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-install-recommends)
      NO_INSTALL_RECOMMENDS=1
      shift
      ;;
    --)
      shift
      EXTRA+=("$@")
      break
      ;;
    *)
      EXTRA+=("$1")
      shift
      ;;
  esac
done

bash "${ROOT_DIR}/scripts/ci/configure-apt.sh"

APT_GET=(bash "${ROOT_DIR}/scripts/ci/apt-get-retry.sh")

"${APT_GET[@]}" update -y -qq
"${APT_GET[@]}" install -y --no-install-recommends \
  ca-certificates curl gnupg lsb-release wget

sudo install -d /usr/share/postgresql-common/pgdg
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  | sudo gpg --batch --yes --dearmor -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.gpg
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.gpg] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
  | sudo tee /etc/apt/sources.list.d/pgdg.list >/dev/null

"${APT_GET[@]}" update -y -qq

INSTALL_ARGS=(install -y)
if (( NO_INSTALL_RECOMMENDS )); then
  INSTALL_ARGS+=(--no-install-recommends)
fi
INSTALL_ARGS+=(
  build-essential
  libssl-dev
  pkg-config
  "postgresql-${PG_VER}"
  "postgresql-server-dev-${PG_VER}"
)
if ((${#EXTRA[@]})); then
  INSTALL_ARGS+=("${EXTRA[@]}")
fi

"${APT_GET[@]}" "${INSTALL_ARGS[@]}"
