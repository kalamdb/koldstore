#!/usr/bin/env bash
# Inject pg_cron + koldstore preload flags so cron.database_name tracks POSTGRES_DB.
set -euo pipefail

DB="${POSTGRES_DB:-koldstore}"
# Default DB for try-it image when callers omit POSTGRES_DB.
export POSTGRES_DB="${DB}"
export POSTGRES_USER="${POSTGRES_USER:-postgres}"

if [[ "${1:-}" == "postgres" ]]; then
  shift
  exec docker-entrypoint.sh postgres \
    -c shared_preload_libraries=pg_cron,koldstore \
    -c "cron.database_name=${DB}" \
    "$@"
fi

exec docker-entrypoint.sh "$@"
