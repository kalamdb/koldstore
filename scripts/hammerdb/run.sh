#!/usr/bin/env bash
# Optional HammerDB TPROC-C stress against a pgrx cluster with selective KoldStore manage.
#
# Order: prepare cluster → buildschema → manage HISTORY only → timed run.
# Skips gracefully when hammerdbcli is not installed.
# Exit 0 = survived without observed crash. Never claim "production safe".
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${1:-${KOLDSTORE_E2E_PGVERSION:-16}}"
PG_PORT="${KOLDSTORE_E2E_PGPORT:-288${PG_VERSION}}"
PG_HOST="${KOLDSTORE_E2E_PGHOST:-127.0.0.1}"
PG_USER="${KOLDSTORE_E2E_PGUSER:-$(whoami)}"
# HammerDB Tcl arg packing breaks on empty passwords; default a local-only password.
PG_PASSWORD="${KOLDSTORE_HAMMERDB_PASSWORD:-hammerdb}"
PG_DATABASE="${KOLDSTORE_HAMMERDB_DB:-koldstore_hammerdb}"
PG_CONFIG="${PGRX_PG_CONFIG:-$(cargo pgrx info pg-config "$PG_VERSION")}"
PSQL="$(dirname "$PG_CONFIG")/psql"
HAMMERDB_BIN="${HAMMERDB_BIN:-}"
# HammerDB pg_duration / pg_rampup are minutes (not seconds).
DURATION="${KOLDSTORE_HAMMERDB_MINUTES:-${KOLDSTORE_HAMMERDB_SECONDS:-1}}"
RAMPUP="${KOLDSTORE_HAMMERDB_RAMPUP:-0}"
WAREHOUSES="${KOLDSTORE_HAMMERDB_WAREHOUSES:-2}"
VIRTUAL_USERS="${KOLDSTORE_HAMMERDB_VU:-2}"
# Schema build requires build VU <= warehouse count.
BUILD_VU="$VIRTUAL_USERS"
if (( BUILD_VU > WAREHOUSES )); then
  BUILD_VU="$WAREHOUSES"
fi
SKIP_BUILD="${KOLDSTORE_HAMMERDB_SKIP_BUILD:-0}"
STORAGE_ROOT="${KOLDSTORE_HAMMERDB_STORAGE:-$(mktemp -d "${TMPDIR:-/tmp}/koldstore-hammerdb.XXXXXX")}"
OUT_DIR="${KOLDSTORE_HAMMERDB_OUT:-${ROOT_DIR}/target/hammerdb}"
BUILD_TCL_SRC="${ROOT_DIR}/scripts/hammerdb/tprocc_build.tcl"
RUN_TCL_SRC="${ROOT_DIR}/scripts/hammerdb/tprocc_run.tcl"
MANAGE_SQL="${ROOT_DIR}/scripts/hammerdb/manage_history.sql"
BUILD_TCL="${OUT_DIR}/tprocc_build.generated.tcl"
RUN_TCL="${OUT_DIR}/tprocc_run.generated.tcl"
RUN_LOG="${OUT_DIR}/hammerdb.log"

fill_tcl() {
  local src="$1"
  local dest="$2"
  sed \
    -e "s|{{PG_HOST}}|${PG_HOST}|g" \
    -e "s|{{PG_PORT}}|${PG_PORT}|g" \
    -e "s|{{PG_USER}}|${PG_USER}|g" \
    -e "s|{{PG_PASSWORD}}|${PG_PASSWORD}|g" \
    -e "s|{{PG_DATABASE}}|${PG_DATABASE}|g" \
    -e "s|{{WAREHOUSES}}|${WAREHOUSES}|g" \
    -e "s|{{VIRTUAL_USERS}}|${VIRTUAL_USERS}|g" \
    -e "s|{{BUILD_VU}}|${BUILD_VU}|g" \
    -e "s|{{RAMPUP}}|${RAMPUP}|g" \
    -e "s|{{DURATION}}|${DURATION}|g" \
    "$src" >"$dest"
}

run_hammer() {
  local tcl="$1"
  local hammer_dir
  hammer_dir="$(cd "$(dirname "$HAMMERDB_BIN")" && pwd)"
  echo "running: ${HAMMERDB_BIN} auto ${tcl}"
  # HammerDB expects to be invoked from its install directory.
  (
    cd "$hammer_dir"
    ./hammerdbcli auto "$tcl"
  ) >>"$RUN_LOG" 2>&1
}

if [[ -z "${HAMMERDB_BIN}" ]]; then
  for candidate in hammerdbcli hammerdb HammerDB; do
    if command -v "${candidate}" >/dev/null 2>&1; then
      HAMMERDB_BIN="$(command -v "${candidate}")"
      break
    fi
  done
fi

if [[ -z "${HAMMERDB_BIN}" ]]; then
  echo "HammerDB not installed; skipping stress run"
  echo "Install HammerDB and set HAMMERDB_BIN, or place hammerdbcli on PATH"
  exit 0
fi

for required in "$BUILD_TCL_SRC" "$RUN_TCL_SRC" "$MANAGE_SQL"; do
  if [[ ! -f "$required" ]]; then
    echo "error: missing ${required}" >&2
    exit 1
  fi
done

mkdir -p "$OUT_DIR" "$STORAGE_ROOT"
: >"$RUN_LOG"
echo "HammerDB: bin=${HAMMERDB_BIN} warehouses=${WAREHOUSES} vu=${VIRTUAL_USERS} duration=${DURATION}m rampup=${RAMPUP}m"

export KOLDSTORE_E2E_PREPARE_ONLY=1
bash scripts/run-pg-e2e.sh "$PG_VERSION"

echo "recreating HammerDB database ${PG_DATABASE}"
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -c "ALTER USER \"${PG_USER}\" PASSWORD '${PG_PASSWORD}'" \
  -c "DROP DATABASE IF EXISTS ${PG_DATABASE}" \
  -c "CREATE DATABASE ${PG_DATABASE}"
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 \
  -c "CREATE EXTENSION IF NOT EXISTS koldstore;"
# shared_preload_libraries=koldstore is set by run-pg-e2e.sh (required for merge scan).

fill_tcl "$BUILD_TCL_SRC" "$BUILD_TCL"
fill_tcl "$RUN_TCL_SRC" "$RUN_TCL"

set +e
if [[ "${SKIP_BUILD}" != "1" && "${SKIP_BUILD}" != "true" ]]; then
  run_hammer "$BUILD_TCL"
  build_rc=$?
  if [[ "$build_rc" -ne 0 ]]; then
    echo "error: HammerDB schema build exited ${build_rc}; see ${RUN_LOG}" >&2
    tail -n 80 "$RUN_LOG" >&2 || true
    exit "$build_rc"
  fi
else
  echo "skipping schema build (KOLDSTORE_HAMMERDB_SKIP_BUILD=1)"
fi

echo "applying selective KoldStore manage on HISTORY"
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 \
  -v "STORAGE_ROOT=${STORAGE_ROOT}" \
  -f "$MANAGE_SQL" >>"$RUN_LOG" 2>&1
manage_rc=$?
if [[ "$manage_rc" -ne 0 ]]; then
  echo "error: manage_history.sql failed (is HISTORY present?); see ${RUN_LOG}" >&2
  tail -n 40 "$RUN_LOG" >&2 || true
  exit "$manage_rc"
fi

run_hammer "$RUN_TCL"
hammer_rc=$?
set -e

FATAL_PATTERNS='PANIC:|FATAL:.*(segfault|signal 11)|trap invalid opcode|Rust panic|panicked at|server process .* was terminated|Abort trap'
if grep -Eiq "${FATAL_PATTERNS}" "$RUN_LOG"; then
  echo "error: fatal pattern detected in HammerDB log ${RUN_LOG}" >&2
  grep -Ein "${FATAL_PATTERNS}" "$RUN_LOG" >&2 || true
  exit 1
fi

if [[ "$hammer_rc" -ne 0 ]]; then
  echo "error: HammerDB run exited with ${hammer_rc}; see ${RUN_LOG}" >&2
  tail -n 80 "$RUN_LOG" >&2 || true
  exit "$hammer_rc"
fi

echo "HammerDB completed without observed PANIC/segfault (log=${RUN_LOG})"
echo "Capture NOPM/TPM from the log for scripts/readiness/run-readiness-report.sh"
echo "Success wording: survival without observed crash — never 'production safe'."
