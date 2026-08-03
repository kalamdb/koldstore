#!/usr/bin/env bash
# Fresh PG18 demo entrypoint: install latest GitHub-release koldstore, then start Postgres.
set -euo pipefail

export PG_MAJOR="${PG_MAJOR:-18}"
export PATH="/usr/lib/postgresql/${PG_MAJOR}/bin:${PATH}"

# Download/install extension files before the first (or any) start.
# Use KOLDSTORE_FORCE_REINSTALL=1 to refresh to a newer GitHub release.
if [[ "${KOLDSTORE_FORCE_REINSTALL:-0}" == "1" ]]; then
  /usr/local/bin/install-koldstore.sh --force
else
  /usr/local/bin/install-koldstore.sh
fi

export POSTGRES_DB="${POSTGRES_DB:-koldstore}"
export POSTGRES_USER="${POSTGRES_USER:-postgres}"

if [[ "${1:-}" == "postgres" ]]; then
  shift
  exec docker-entrypoint.sh postgres \
    -c listen_addresses='*' \
    -c shared_preload_libraries=koldstore \
    -c wal_level=logical \
    "$@"
fi

exec docker-entrypoint.sh "$@"
