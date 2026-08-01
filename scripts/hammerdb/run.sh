#!/usr/bin/env bash
# Optional HammerDB TPROC-C stress against a pgrx cluster with selective KoldStore manage.
#
# Order: prepare → buildschema → manage HISTORY → timed run → flush → EXPLAIN proof.
# Skips gracefully when neither hammerdbcli nor Docker is available.
# Exit 0 = survived without observed crash + managed hot/cold plan proof.
# Never claim "TPC certified" or "production safe".
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${1:-${KOLDSTORE_E2E_PGVERSION:-16}}"
PG_PORT="${KOLDSTORE_E2E_PGPORT:-288${PG_VERSION}}"
PG_HOST_LOCAL="${KOLDSTORE_E2E_PGHOST:-127.0.0.1}"
PG_USER="${KOLDSTORE_E2E_PGUSER:-$(whoami)}"
# HammerDB Tcl arg packing breaks on empty passwords; default a local-only password.
PG_PASSWORD="${KOLDSTORE_HAMMERDB_PASSWORD:-hammerdb}"
PG_DATABASE="${KOLDSTORE_HAMMERDB_DB:-koldstore_hammerdb}"
PG_CONFIG="${PGRX_PG_CONFIG:-$(cargo pgrx info pg-config "$PG_VERSION")}"
PSQL="$(dirname "$PG_CONFIG")/psql"
HAMMERDB_BIN="${HAMMERDB_BIN:-}"
DOCKER_IMAGE="${KOLDSTORE_HAMMERDB_DOCKER_IMAGE:-tpcorg/hammerdb:v4.12}"
HAMMER_MODE=native
# Host seen by HammerDB (host.docker.internal when using the Docker image).
PG_HOST_HAMMER="$PG_HOST_LOCAL"
# HammerDB pg_duration / pg_rampup are minutes (not seconds).
DURATION="${KOLDSTORE_HAMMERDB_MINUTES:-${KOLDSTORE_HAMMERDB_SECONDS:-1}}"
RAMPUP="${KOLDSTORE_HAMMERDB_RAMPUP:-0}"
WAREHOUSES="${KOLDSTORE_HAMMERDB_WAREHOUSES:-2}"
VIRTUAL_USERS="${KOLDSTORE_HAMMERDB_VU:-2}"
READ_ITERS="${KOLDSTORE_HAMMERDB_READ_ITERS:-50}"
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
SUMMARY_JSON="${OUT_DIR}/summary.json"
EXPLAIN_OUT="${OUT_DIR}/explain_post_run.txt"
READS_JSON="${OUT_DIR}/reads_post_run.json"

psql_db() {
  "$PSQL" -h "$PG_HOST_LOCAL" -p "$PG_PORT" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 "$@"
}

fill_tcl() {
  local src="$1"
  local dest="$2"
  sed \
    -e "s|{{PG_HOST}}|${PG_HOST_HAMMER}|g" \
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
  echo "running: hammerdb auto $(basename "$tcl") (${HAMMER_MODE})"
  if [[ "${HAMMER_MODE}" == docker ]]; then
    # Mount OUT_DIR so generated Tcl is visible inside the container.
    docker run --rm --platform linux/amd64 \
      -v "${OUT_DIR}:/work:ro" \
      -w /home/HammerDB-4.12 \
      "${DOCKER_IMAGE}" \
      ./hammerdbcli auto "/work/$(basename "$tcl")" >>"$RUN_LOG" 2>&1
  else
    local hammer_dir
    hammer_dir="$(cd "$(dirname "$HAMMERDB_BIN")" && pwd)"
    # HammerDB expects to be invoked from its install directory.
    (
      cd "$hammer_dir"
      ./hammerdbcli auto "$tcl"
    ) >>"$RUN_LOG" 2>&1
  fi
}

parse_nopm_tpm() {
  python3 - <<PY
import re
text = open("${RUN_LOG}", encoding="utf-8", errors="replace").read()
m = re.search(r"System achieved\s+(\d+)\s+NOPM from\s+(\d+)\s+PostgreSQL TPM", text)
print("0 0" if not m else f"{m.group(1)} {m.group(2)}")
PY
}

flush_history() {
  echo "flushing managed HISTORY so post-run EXPLAIN can prove cold open"
  psql_db \
    -c "SELECT koldstore.flush_table('public.history'::regclass);" \
    -c "SELECT id, job_type, status, left(coalesce(error_trace,''), 240) AS err FROM koldstore.jobs WHERE job_type='flush' ORDER BY created_at DESC LIMIT 1;" \
    -c "SELECT count(*) AS active_segments, coalesce(sum(row_count),0) AS cold_rows, coalesce(sum(byte_size),0) AS cold_bytes FROM koldstore.cold_segments WHERE status='active';" \
    | tee "${OUT_DIR}/flush.log"
  local segs
  segs="$(psql_db -Atc "SELECT count(*) FROM koldstore.cold_segments WHERE status='active';")"
  if [[ "${segs}" -lt 1 ]]; then
    echo "error: flush produced 0 active cold segments; see ${OUT_DIR}/flush.log" >&2
    psql_db -c "SELECT id, status, left(coalesce(error_trace,''), 400) FROM koldstore.jobs WHERE job_type='flush' ORDER BY created_at DESC LIMIT 3;" >&2 || true
    exit 1
  fi
}

prove_hot_cold_read() {
  echo "proving HISTORY PK uses KoldMergeScan with opened>=1 cold segment"
  python3 "${ROOT_DIR}/scripts/hammerdb/read_bench.py" \
    --arm post_run \
    --psql "${PSQL}" \
    --host "${PG_HOST_LOCAL}" \
    --port "${PG_PORT}" \
    --database "${PG_DATABASE}" \
    --iters "${READ_ITERS}" \
    --expect-merge \
    --json-out "${READS_JSON}" \
    --explain-out "${EXPLAIN_OUT}"
  echo "reads/post_run: $(tr -d '\n' <"${READS_JSON}")"
}

if [[ -z "${HAMMERDB_BIN}" ]]; then
  for candidate in hammerdbcli hammerdb HammerDB; do
    if command -v "${candidate}" >/dev/null 2>&1; then
      HAMMERDB_BIN="$(command -v "${candidate}")"
      break
    fi
  done
fi

if [[ -z "${HAMMERDB_BIN}" ]] && command -v docker >/dev/null 2>&1; then
  HAMMER_MODE=docker
  PG_HOST_HAMMER="host.docker.internal"
  echo "using Docker HammerDB (${DOCKER_IMAGE}) → ${PG_HOST_HAMMER}:${PG_PORT}"
  docker pull --platform linux/amd64 "${DOCKER_IMAGE}" >/dev/null
elif [[ -z "${HAMMERDB_BIN}" ]]; then
  echo "HammerDB not installed; skipping stress run"
  echo "Install HammerDB and set HAMMERDB_BIN, place hammerdbcli on PATH, or install Docker"
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
if [[ "${HAMMER_MODE}" == docker ]]; then
  echo "HammerDB: mode=docker image=${DOCKER_IMAGE} warehouses=${WAREHOUSES} vu=${VIRTUAL_USERS} duration=${DURATION}m rampup=${RAMPUP}m"
else
  echo "HammerDB: mode=native bin=${HAMMERDB_BIN} warehouses=${WAREHOUSES} vu=${VIRTUAL_USERS} duration=${DURATION}m rampup=${RAMPUP}m"
fi

export KOLDSTORE_E2E_PREPARE_ONLY=1
bash scripts/run-pg-e2e.sh "$PG_VERSION"

echo "recreating HammerDB database ${PG_DATABASE}"
"$PSQL" -h "$PG_HOST_LOCAL" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -c "ALTER USER \"${PG_USER}\" PASSWORD '${PG_PASSWORD}'" \
  -c "DROP DATABASE IF EXISTS ${PG_DATABASE}" \
  -c "CREATE DATABASE ${PG_DATABASE}"
psql_db -c "CREATE EXTENSION IF NOT EXISTS koldstore;"
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
  if ! grep -q "FINISHED SUCCESS" "$RUN_LOG"; then
    echo "error: HammerDB schema build missing FINISHED SUCCESS; see ${RUN_LOG}" >&2
    tail -n 80 "$RUN_LOG" >&2 || true
    exit 1
  fi
else
  echo "skipping schema build (KOLDSTORE_HAMMERDB_SKIP_BUILD=1)"
fi

echo "applying selective KoldStore manage on HISTORY"
psql_db -v "STORAGE_ROOT=${STORAGE_ROOT}" -f "$MANAGE_SQL" >>"$RUN_LOG" 2>&1
manage_rc=$?
if [[ "$manage_rc" -ne 0 ]]; then
  echo "error: manage_history.sql failed (is HISTORY present?); see ${RUN_LOG}" >&2
  tail -n 40 "$RUN_LOG" >&2 || true
  exit "$manage_rc"
fi
# Prove HISTORY is managed before the timed run (workflow contract).
managed_count="$(psql_db -Atc \
  "SELECT count(*) FROM koldstore.schemas WHERE table_oid = 'public.history'::regclass AND active;")"
if [[ "${managed_count}" != "1" ]]; then
  echo "error: expected public.history to be actively managed; active_schemas=${managed_count}" >&2
  exit 1
fi
echo "confirmed public.history is managed (koldstore.schemas active=1)"

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

if ! grep -Eq "TEST RESULT|HAMMERDB RUN COMPLETE|System achieved" "$RUN_LOG"; then
  echo "error: timed run missing success markers; see ${RUN_LOG}" >&2
  tail -n 80 "$RUN_LOG" >&2 || true
  exit 1
fi

# Still managed after the OLTP mix (capture must not drop the catalog entry).
managed_after="$(psql_db -Atc \
  "SELECT count(*) FROM koldstore.schemas WHERE table_oid = 'public.history'::regclass AND active;")"
if [[ "${managed_after}" != "1" ]]; then
  echo "error: public.history unmanaged after timed run; active_schemas=${managed_after}" >&2
  exit 1
fi

read -r NOPM TPM < <(parse_nopm_tpm)
echo "timed result: ${NOPM} NOPM / ${TPM} TPM"

# TPROC-C mostly inserts HISTORY; flush + EXPLAIN prove hot+cold merge after the mix.
flush_history
prove_hot_cold_read

python3 - <<PY
import json
from pathlib import Path

out = Path("${OUT_DIR}")
reads = json.loads((out / "reads_post_run.json").read_text(encoding="utf-8"))
summary = {
    "postgresql_version": "${PG_VERSION}",
    "warehouses": int("${WAREHOUSES}"),
    "virtual_users": int("${VIRTUAL_USERS}"),
    "duration_minutes": int("${DURATION}"),
    "read_iters": int("${READ_ITERS}"),
    "hammer_mode": "${HAMMER_MODE}",
    "policy": "manage HISTORY only; customer/orders/stock remain unmanaged",
    "claim": (
        "KoldStore survived HammerDB TPROC-C with selective HISTORY manage, "
        "then proved KoldMergeScan opened cold Parquet on a HISTORY PK lookup. "
        "Not a TPC certification; not a claim of production safety."
    ),
    "nopm": int("${NOPM}"),
    "tpm": int("${TPM}"),
    "reads": reads,
}
(out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

echo "HammerDB completed without observed PANIC/segfault (log=${RUN_LOG})"
echo "Managed table check: public.history remained managed through TPROC-C"
echo "Hot+cold proof: ${EXPLAIN_OUT}"
echo "Summary: ${SUMMARY_JSON}"
echo "Claim wording: survived selective-manage TPROC-C + post-run merge/cold proof — never 'TPC certified' or 'production safe'."
