#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${1:-${KOLDSTORE_E2E_PGVERSION:-16}}"
PREPARE_ONLY="${KOLDSTORE_E2E_PREPARE_ONLY:-0}"
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
if [[ "${KOLDSTORE_PGRX_INSTALL_RELEASE:-}" == "1" || "${KOLDSTORE_PGRX_INSTALL_RELEASE:-}" == "true" ]]; then
  # release-pg: optimized + panic=unwind (plain --release uses panic=abort and
  # aborts on PostgreSQL ereport/longjmp from extension hooks).
  INSTALL_ARGS+=(--profile release-pg)
fi
if [[ "${KOLDSTORE_PGRX_INSTALL_SUDO:-}" == "1" || "${KOLDSTORE_PGRX_INSTALL_SUDO:-}" == "true" ]]; then
  INSTALL_ARGS+=(--sudo)
fi
cargo pgrx install "${INSTALL_ARGS[@]}"

echo "restarting pgrx-managed PostgreSQL ${PG_VERSION} to load extension"
cargo pgrx stop "$PG_FEATURE"
cargo pgrx start "$PG_FEATURE"

echo "recreating local E2E database ${PG_DATABASE} on ${PG_HOST}:${PG_PORT}"
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -c "DROP DATABASE IF EXISTS ${PG_DATABASE}" \
  -c "CREATE DATABASE ${PG_DATABASE}"
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 \
  -c "CREATE EXTENSION IF NOT EXISTS koldstore;"

server_version="$("$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -tAc "SHOW server_version")"
if [[ ! "${server_version}" =~ ^${PG_VERSION} ]]; then
  echo "error: expected PostgreSQL ${PG_VERSION} on ${PG_HOST}:${PG_PORT}, got '${server_version}'" >&2
  exit 1
fi
echo "verified pgrx PostgreSQL ${PG_VERSION} on ${PG_HOST}:${PG_PORT} (${server_version})"

export KOLDSTORE_E2E_PGVERSION="$PG_VERSION"
export KOLDSTORE_E2E_PGHOST="$PG_HOST"
export KOLDSTORE_E2E_PGPORT="$PG_PORT"
export KOLDSTORE_E2E_PGUSER="$PG_USER"
export KOLDSTORE_E2E_PGPASSWORD="$PG_PASSWORD"
export KOLDSTORE_E2E_PGDATABASE="$PG_DATABASE"
export KOLDSTORE_E2E_WAIT_FOR_STARTUP=1

if [[ "${PREPARE_ONLY}" == "1" || "${PREPARE_ONLY}" == "true" ]]; then
  echo "E2E PostgreSQL ${PG_VERSION} is ready (prepare-only; skipping cargo nextest)"
  exit 0
fi

echo "running pg-koldstore E2E tests against pgrx PostgreSQL ${PG_VERSION} on ${PG_HOST}:${PG_PORT}"
if ! cargo nextest --version >/dev/null 2>&1; then
  echo "error: cargo-nextest is required; install with: cargo install cargo-nextest --locked" >&2
  exit 1
fi

NEXT_ARGS=(-p e2e --test-threads 1)
if [[ "${KOLDSTORE_E2E_VERBOSE:-}" == "1" || "${KOLDSTORE_E2E_VERBOSE:-}" == "true" ]]; then
  echo "E2E verbose logging enabled (KOLDSTORE_E2E_VERBOSE); showing live test output"
  NEXT_ARGS+=(--no-capture)
fi

cargo nextest run "${NEXT_ARGS[@]}"
