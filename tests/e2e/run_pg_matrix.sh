#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${KOLDSTORE_E2E_PGVERSION:-16}"
PG_FEATURE="pg${PG_VERSION}"
PG_PORT="${KOLDSTORE_E2E_PGPORT:-288${PG_VERSION}}"
PG_HOST="${KOLDSTORE_E2E_PGHOST:-127.0.0.1}"
PG_USER="${KOLDSTORE_E2E_PGUSER:-$(whoami)}"
PG_DATABASE="${KOLDSTORE_E2E_PGDATABASE:-koldstore_pgrx_e2e}"
PG_PASSWORD="${KOLDSTORE_E2E_PGPASSWORD:-}"
PG_CONFIG="${PGRX_PG_CONFIG:-$(cargo pgrx info pg-config "$PG_VERSION")}"
PSQL="$(dirname "$PG_CONFIG")/psql"

echo "starting pgrx-managed PostgreSQL ${PG_VERSION}"
cargo pgrx start "$PG_FEATURE"

echo "installing koldstore into pgrx PostgreSQL ${PG_VERSION}"
INSTALL_ARGS=(
  -p pg_koldstore
  --no-default-features
  --features "$PG_FEATURE"
  --pg-config "$PG_CONFIG"
)
if [[ "${KOLDSTORE_PGRX_INSTALL_SUDO:-}" == "1" || "${KOLDSTORE_PGRX_INSTALL_SUDO:-}" == "true" ]]; then
  INSTALL_ARGS+=(--sudo)
fi
cargo pgrx install "${INSTALL_ARGS[@]}"

echo "recreating local E2E database ${PG_DATABASE} on ${PG_HOST}:${PG_PORT}"
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -c "DROP DATABASE IF EXISTS ${PG_DATABASE}" \
  -c "CREATE DATABASE ${PG_DATABASE}"

export KOLDSTORE_E2E_PGVERSION="$PG_VERSION"
export KOLDSTORE_E2E_PGHOST="$PG_HOST"
export KOLDSTORE_E2E_PGPORT="$PG_PORT"
export KOLDSTORE_E2E_PGUSER="$PG_USER"
export KOLDSTORE_E2E_PGPASSWORD="$PG_PASSWORD"
export KOLDSTORE_E2E_PGDATABASE="$PG_DATABASE"

echo "running pg-koldstore E2E tests against local pgrx PostgreSQL ${PG_VERSION}"
cargo test -p koldstore-e2e -- --include-ignored
