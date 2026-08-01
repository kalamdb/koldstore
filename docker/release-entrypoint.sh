#!/usr/bin/env bash
# Default POSTGRES_DB for the try-it image when callers omit it.
#
# Force listen_addresses=* so published host ports reach Postgres (PGDG
# defaults to localhost). Always preload koldstore so KoldMergeScan hooks
# install in every backend (CREATE EXTENSION alone is not enough).
# wal_level=logical is required for manage_table / async mirror capture.
# pg_cron is packaged but not preloaded.
set -euo pipefail

export POSTGRES_DB="${POSTGRES_DB:-koldstoredb}"
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
