#!/usr/bin/env bash
# Default POSTGRES_DB for the try-it image when callers omit it.
#
# Force listen_addresses=* so published host ports reach Postgres (PGDG
# defaults to localhost). Always preload koldstore so KoldMergeScan hooks
# install in every backend (CREATE EXTENSION alone is not enough).
# wal_level=logical is required for manage_table / async mirror capture.
# pg_cron is packaged but not preloaded.
#
# Cold filesystem paths: Docker Desktop (Windows/macOS) bind mounts often
# arrive as root-owned with no write for the postgres user. Ensure common
# cold roots exist and are writable before Postgres starts, or flush fails
# with "Permission denied" on .tmp segment dirs.
set -euo pipefail

export POSTGRES_DB="${POSTGRES_DB:-koldstoredb}"
export POSTGRES_USER="${POSTGRES_USER:-postgres}"

ensure_writable_cold_roots() {
  local dir
  # /koldstore-data is the common Windows mapped-drive mount point from docs
  # and operator recipes; /tmp/koldstore-demo is the image quick-start path.
  for dir in \
    /koldstore-data \
    /koldstore-data/cold \
    /tmp/koldstore-demo
  do
    mkdir -p "${dir}" 2>/dev/null || true
    if chown -R postgres:postgres "${dir}" 2>/dev/null; then
      chmod -R u+rwX,g+rwX "${dir}" 2>/dev/null || true
    else
      # Bind mounts (esp. Docker Desktop on Windows) may reject chown; fall
      # back to world-writable so the postgres OS user can create Parquet dirs.
      chmod -R a+rwX "${dir}" 2>/dev/null || true
    fi
  done
}

ensure_writable_cold_roots

if [[ "${1:-}" == "postgres" ]]; then
  shift
  exec docker-entrypoint.sh postgres \
    -c listen_addresses='*' \
    -c shared_preload_libraries=koldstore \
    -c wal_level=logical \
    -c log_min_messages=info \
    "$@"
fi

exec docker-entrypoint.sh "$@"
