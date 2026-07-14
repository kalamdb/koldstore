#!/usr/bin/env bash
# Run KoldStore-specific SQL regression cases against pgrx PostgreSQL.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${1:-${KOLDSTORE_E2E_PGVERSION:-16}}"
PG_FEATURE="pg${PG_VERSION}"
PG_PORT="${KOLDSTORE_E2E_PGPORT:-288${PG_VERSION}}"
PG_HOST="${KOLDSTORE_E2E_PGHOST:-127.0.0.1}"
PG_USER="${KOLDSTORE_E2E_PGUSER:-$(whoami)}"
PG_DATABASE="${KOLDSTORE_SQL_REGRESSION_DB:-koldstore_sql_regression}"
PG_CONFIG="${PGRX_PG_CONFIG:-$(cargo pgrx info pg-config "$PG_VERSION")}"
PSQL="$(dirname "$PG_CONFIG")/psql"
SQL_DIR="${ROOT_DIR}/tests/sql"
EXPECTED_DIR="${SQL_DIR}/expected"
UPDATE_EXPECTED="${KOLDSTORE_SQL_REGRESSION_UPDATE:-0}"
STORAGE_ROOT="${KOLDSTORE_SQL_REGRESSION_STORAGE:-$(mktemp -d "${TMPDIR:-/tmp}/koldstore-sqlreg.XXXXXX")}"

normalize_output() {
  # Strip unstable identifiers while keeping relational assertions intact.
  sed -E \
    -e 's/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/<UUID>/g' \
    -e 's#(/private)?/[^[:space:]]+/(koldstore[^[:space:]]*|pg-koldstore[^[:space:]]*|sqlreg[^[:space:]]*)#<PATH>#g' \
    -e 's/cost=[0-9.]+(\.\.[0-9.]+)?/<COST>/g' \
    -e 's/actual time=[0-9.]+(\.\.[0-9.]+)?/<TIME>/g' \
    -e 's/rows=[0-9]+/<ROWS>/g' \
    -e 's/[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9:.+-]+/<TS>/g' \
    -e 's/[[:space:]]+$//' \
    | awk 'NF {blank=0; print} !NF {if (!blank++) print}'
}

prepare_cluster() {
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

  echo "restarting pgrx-managed PostgreSQL ${PG_VERSION} to load extension"
  cargo pgrx stop "$PG_FEATURE"
  cargo pgrx start "$PG_FEATURE"

  echo "recreating SQL regression database ${PG_DATABASE}"
  "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
    -c "DROP DATABASE IF EXISTS ${PG_DATABASE}" \
    -c "CREATE DATABASE ${PG_DATABASE}"
  "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 \
    -c "CREATE EXTENSION IF NOT EXISTS koldstore;"
}

run_case() {
  local sql_file="$1"
  local name
  name="$(basename "$sql_file" .sql)"
  local expected="${EXPECTED_DIR}/${name}.out"
  local actual
  actual="$(mktemp)"

  mkdir -p "$STORAGE_ROOT" "$EXPECTED_DIR"

  {
    "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 \
      -v "STORAGE_ROOT=${STORAGE_ROOT}" \
      -f "${SQL_DIR}/setup.sql"
    "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 \
      -f "$sql_file"
  } 2>&1 | normalize_output >"$actual"

  if [[ "${UPDATE_EXPECTED}" == "1" || "${UPDATE_EXPECTED}" == "true" ]]; then
    cp "$actual" "$expected"
    echo "updated expected: ${expected}"
    rm -f "$actual"
    return 0
  fi

  if [[ ! -f "$expected" ]]; then
    echo "error: missing expected output ${expected}" >&2
    echo "actual output:" >&2
    cat "$actual" >&2
    rm -f "$actual"
    return 1
  fi

  if ! diff -u "$expected" "$actual"; then
    echo "error: SQL regression mismatch for ${name}" >&2
    rm -f "$actual"
    return 1
  fi
  echo "ok: ${name}"
  rm -f "$actual"
}

prepare_cluster

failures=0
shopt -s nullglob
for sql_file in "${SQL_DIR}"/*.sql; do
  name="$(basename "$sql_file")"
  if [[ "$name" == "setup.sql" ]]; then
    continue
  fi
  if ! run_case "$sql_file"; then
    failures=$((failures + 1))
  fi
done

if [[ "$failures" -ne 0 ]]; then
  echo "SQL regression failed: ${failures} case(s)" >&2
  exit 1
fi

echo "SQL regression passed for PostgreSQL ${PG_VERSION}"
