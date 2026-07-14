#!/usr/bin/env bash
# Optional SQLsmith fuzz against a KoldStore-managed table.
# Skips gracefully when sqlsmith is not installed.
# Fails hard on PANIC / segfault / Rust panic patterns in captured logs.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${1:-${KOLDSTORE_E2E_PGVERSION:-16}}"
SECONDS_LIMIT="${KOLDSTORE_SQLSMITH_SECONDS:-30}"
SEED="${KOLDSTORE_SQLSMITH_SEED:-$(date +%s)}"
PG_FEATURE="pg${PG_VERSION}"
PG_PORT="${KOLDSTORE_E2E_PGPORT:-288${PG_VERSION}}"
PG_HOST="${KOLDSTORE_E2E_PGHOST:-127.0.0.1}"
PG_USER="${KOLDSTORE_E2E_PGUSER:-$(whoami)}"
PG_DATABASE="${KOLDSTORE_SQLSMITH_DB:-koldstore_sqlsmith}"
PG_CONFIG="${PGRX_PG_CONFIG:-$(cargo pgrx info pg-config "$PG_VERSION")}"
PSQL="$(dirname "$PG_CONFIG")/psql"
LOG_DIR="${KOLDSTORE_SQLSMITH_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/koldstore-sqlsmith.XXXXXX")}"
STORAGE_ROOT="${KOLDSTORE_SQLSMITH_STORAGE:-$(mktemp -d "${TMPDIR:-/tmp}/koldstore-sqlsmith-store.XXXXXX")}"
SQLSMITH_BIN="${SQLSMITH_BIN:-sqlsmith}"

if ! command -v "${SQLSMITH_BIN}" >/dev/null 2>&1; then
  echo "sqlsmith not installed; skipping SQLsmith fuzz (set SQLSMITH_BIN to override path)"
  exit 0
fi

mkdir -p "$LOG_DIR"
echo "SQLsmith: seconds=${SECONDS_LIMIT} seed=${SEED} log_dir=${LOG_DIR}"

export KOLDSTORE_E2E_PREPARE_ONLY=1
# Reuse E2E install path, then recreate a dedicated fuzz database.
bash scripts/run-pg-e2e.sh "$PG_VERSION"

"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -c "DROP DATABASE IF EXISTS ${PG_DATABASE}" \
  -c "CREATE DATABASE ${PG_DATABASE}"
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 \
  -c "CREATE EXTENSION IF NOT EXISTS koldstore;"

"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 \
  -v "STORAGE_ROOT=${STORAGE_ROOT}" \
  -f "${ROOT_DIR}/scripts/sqlsmith/setup.sql"

CONN="host=${PG_HOST} port=${PG_PORT} user=${PG_USER} dbname=${PG_DATABASE}"
SQLSMITH_LOG="${LOG_DIR}/sqlsmith.log"
PG_LOG_HINT="${LOG_DIR}/README.txt"
cat >"$PG_LOG_HINT" <<EOF
SQLsmith run against ${CONN}
Inspect PostgreSQL logs under ~/.pgrx/${PG_VERSION}*/ if backends die.
EOF

set +e
timeout "${SECONDS_LIMIT}" "${SQLSMITH_BIN}" \
  --verbose \
  --seed="${SEED}" \
  --target="postgres://${PG_USER}@${PG_HOST}:${PG_PORT}/${PG_DATABASE}" \
  >"${SQLSMITH_LOG}" 2>&1
rc=$?
set -e

# timeout exits 124 on expiry — treat as success for bounded CI runs.
if [[ "$rc" -ne 0 && "$rc" -ne 124 ]]; then
  echo "warning: sqlsmith exited with ${rc}; checking logs for fatal patterns" >&2
fi

FATAL_PATTERNS='PANIC:|FATAL:.*(segfault|signal 11)|trap invalid opcode|Rust panic|panicked at|server process .* was terminated|Abort trap'
if grep -Eiq "${FATAL_PATTERNS}" "${SQLSMITH_LOG}"; then
  echo "error: fatal pattern detected in SQLsmith log ${SQLSMITH_LOG}" >&2
  grep -Ein "${FATAL_PATTERNS}" "${SQLSMITH_LOG}" >&2 || true
  exit 1
fi

echo "SQLsmith completed without observed PANIC/segfault patterns (seed=${SEED})"
