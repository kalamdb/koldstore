#!/usr/bin/env bash
# Inject pg_cron preload so cron.database_name tracks POSTGRES_DB.
#
# Do not preload koldstore here: initdb loads shared_preload_libraries before
# CREATE EXTENSION runs, and koldstore's hooks query koldstore.schemas.
# SQL + pg_cron scheduling work without koldstore in shared_preload_libraries.
set -euo pipefail

DB="${POSTGRES_DB:-koldstore}"
# Default DB for try-it image when callers omit POSTGRES_DB.
export POSTGRES_DB="${DB}"
export POSTGRES_USER="${POSTGRES_USER:-postgres}"

if [[ "${1:-}" == "postgres" ]]; then
  shift
  exec docker-entrypoint.sh postgres \
    -c shared_preload_libraries=pg_cron \
    -c "cron.database_name=${DB}" \
    "$@"
fi

exec docker-entrypoint.sh "$@"
