#!/usr/bin/env bash
# Dual-DB SQLsmith differential compare: plain heap vs KoldStore-managed twin.
#
# Relies on external SQLsmith (anse1/sqlsmith) — does not vendor a query corpus.
# Skips gracefully when sqlsmith is not installed.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${1:-${KOLDSTORE_E2E_PGVERSION:-16}}"
SECONDS_LIMIT="${KOLDSTORE_DIFF_SQLSMITH_SECONDS:-${KOLDSTORE_SQLSMITH_SECONDS:-30}}"
SEED="${KOLDSTORE_DIFF_SQLSMITH_SEED:-$(date +%s)}"
PG_PORT="${KOLDSTORE_E2E_PGPORT:-288${PG_VERSION}}"
PG_HOST="${KOLDSTORE_E2E_PGHOST:-127.0.0.1}"
PG_USER="${KOLDSTORE_E2E_PGUSER:-$(whoami)}"
PG_DATABASE="${KOLDSTORE_DIFF_SQLSMITH_DB:-koldstore_diff_sqlsmith}"
PG_CONFIG="${PGRX_PG_CONFIG:-$(cargo pgrx info pg-config "$PG_VERSION")}"
PSQL="$(dirname "$PG_CONFIG")/psql"
LOG_DIR="${KOLDSTORE_DIFF_SQLSMITH_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/koldstore-diff-sqlsmith.XXXXXX")}"
STORAGE_ROOT="${KOLDSTORE_DIFF_SQLSMITH_STORAGE:-$(mktemp -d "${TMPDIR:-/tmp}/koldstore-diff-store.XXXXXX")}"
SQLSMITH_BIN="${SQLSMITH_BIN:-sqlsmith}"
STATE="${KOLDSTORE_DIFF_STATE:-mixed}" # hot | mixed | cold

psql_db() {
  "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 "$@"
}

if ! command -v "${SQLSMITH_BIN}" >/dev/null 2>&1; then
  echo "sqlsmith not installed; skipping differential SQLsmith compare"
  echo "Install via scripts/ci/install-sqlsmith.sh or set SQLSMITH_BIN"
  exit 0
fi

mkdir -p "$LOG_DIR"
echo "differential SQLsmith compare: state=${STATE} seconds=${SECONDS_LIMIT} seed=${SEED}"

export KOLDSTORE_E2E_PREPARE_ONLY=1
bash scripts/run-pg-e2e.sh "$PG_VERSION"

"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -c "DROP DATABASE IF EXISTS ${PG_DATABASE}" \
  -c "CREATE DATABASE ${PG_DATABASE}"
psql_db -c "CREATE EXTENSION IF NOT EXISTS koldstore;"

psql_db -v "STORAGE_ROOT=${STORAGE_ROOT}" \
  -f "${ROOT_DIR}/scripts/differential/setup.sql"

case "${STATE}" in
  hot)
    echo "state=hot: skipping flush"
    ;;
  mixed)
    psql_db -c "SELECT koldstore.flush_table('diff_ks.managed'::regclass);"
    psql_db -c "
      INSERT INTO diff_ks.baseline (id, flag, qty, amount, label)
      SELECT gs, true, 1, 0.5, 'hot-' || gs::text FROM generate_series(1000, 1010) AS gs;
      INSERT INTO diff_ks.managed (id, flag, qty, amount, label)
      SELECT gs, true, 1, 0.5, 'hot-' || gs::text FROM generate_series(1000, 1010) AS gs;
    "
    ;;
  cold)
    psql_db -c "SELECT koldstore.flush_table('diff_ks.managed'::regclass);"
    psql_db -c "
      INSERT INTO diff_ks.baseline (id, flag, qty, amount, label)
      SELECT gs, true, 1, 0.5, 'hot-' || gs::text FROM generate_series(1000, 1010) AS gs;
      INSERT INTO diff_ks.managed (id, flag, qty, amount, label)
      SELECT gs, true, 1, 0.5, 'hot-' || gs::text FROM generate_series(1000, 1010) AS gs;
    "
    psql_db -c "SELECT koldstore.flush_table('diff_ks.managed'::regclass);"
    ;;
  *)
    echo "error: unknown KOLDSTORE_DIFF_STATE=${STATE} (use hot|mixed|cold)" >&2
    exit 1
    ;;
esac

SQLSMITH_LOG="${LOG_DIR}/sqlsmith.log"
COMPARE_LOG="${LOG_DIR}/compare.log"

set +e
timeout "${SECONDS_LIMIT}" "${SQLSMITH_BIN}" \
  --verbose \
  --seed="${SEED}" \
  --target="postgres://${PG_USER}@${PG_HOST}:${PG_PORT}/${PG_DATABASE}" \
  >"${SQLSMITH_LOG}" 2>&1
rc=$?
set -e

if [[ "$rc" -ne 0 && "$rc" -ne 124 ]]; then
  echo "warning: sqlsmith exited with ${rc}; continuing to log scan" >&2
fi

FATAL_PATTERNS='PANIC:|FATAL:.*(segfault|signal 11)|trap invalid opcode|Rust panic|panicked at|server process .* was terminated|Abort trap'
if grep -Eiq "${FATAL_PATTERNS}" "${SQLSMITH_LOG}"; then
  echo "error: fatal pattern detected in ${SQLSMITH_LOG}" >&2
  grep -Ein "${FATAL_PATTERNS}" "${SQLSMITH_LOG}" >&2 || true
  exit 1
fi

psql_db -f "${ROOT_DIR}/scripts/differential/compare_hashes.sql" >"${COMPARE_LOG}" 2>&1

echo "differential SQLsmith compare completed (seed=${SEED}, log=${LOG_DIR})"
