#!/usr/bin/env bash
# Three-arm HammerDB + KoldStore comparison:
#   1) baseline   — HISTORY unmanaged (plain heap)
#   2) hot_only   — HISTORY managed, not flushed (heap-only; merge scan sees 0 cold)
#   3) hot_cold   — HISTORY managed + flushed (KoldMergeScan hot+cold)
#
# HammerDB TPROC-C mainly INSERTs into HISTORY; it does NOT prove cold reads.
# This harness therefore also runs explicit HISTORY/customer read microbenches
# and captures EXPLAIN plans that must show KoldMergeScan + opened Parquet
# segments on the hot_cold arm.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${1:-${KOLDSTORE_E2E_PGVERSION:-16}}"
PG_PORT="${KOLDSTORE_E2E_PGPORT:-288${PG_VERSION}}"
PG_HOST_LOCAL="${KOLDSTORE_E2E_PGHOST:-127.0.0.1}"
PG_USER="${KOLDSTORE_E2E_PGUSER:-$(whoami)}"
PG_PASSWORD="${KOLDSTORE_HAMMERDB_PASSWORD:-hammerdb}"
PG_DATABASE="${KOLDSTORE_HAMMERDB_DB:-koldstore_hammerdb}"
PG_CONFIG="${PGRX_PG_CONFIG:-$(cargo pgrx info pg-config "$PG_VERSION")}"
PSQL="$(dirname "$PG_CONFIG")/psql"

DURATION="${KOLDSTORE_HAMMERDB_MINUTES:-2}"
RAMPUP="${KOLDSTORE_HAMMERDB_RAMPUP:-0}"
WAREHOUSES="${KOLDSTORE_HAMMERDB_WAREHOUSES:-2}"
VIRTUAL_USERS="${KOLDSTORE_HAMMERDB_VU:-4}"
BUILD_VU="$VIRTUAL_USERS"
if (( BUILD_VU > WAREHOUSES )); then
  BUILD_VU="$WAREHOUSES"
fi
READ_ITERS="${KOLDSTORE_HAMMERDB_READ_ITERS:-200}"

OUT_DIR="${KOLDSTORE_HAMMERDB_OUT:-${ROOT_DIR}/target/hammerdb/compare}"
CHART_DIR="${KOLDSTORE_HAMMERDB_CHART_DIR:-${ROOT_DIR}/docs/benchmarks/assets}"
# Fresh filesystem root each compare run — leftover Parquet from prior runs
# fails flush object-size validation ("exists with size X, expected Y").
STORAGE_ROOT="${KOLDSTORE_HAMMERDB_STORAGE:-$(mktemp -d "${TMPDIR:-/tmp}/koldstore-hammerdb-compare.XXXXXX")}"
DOCKER_IMAGE="${KOLDSTORE_HAMMERDB_DOCKER_IMAGE:-tpcorg/hammerdb:v4.12}"
RESULTS_JSON="${OUT_DIR}/results.json"
mkdir -p "$OUT_DIR" "$CHART_DIR"
echo "cold storage root: ${STORAGE_ROOT}"
# Keep a pointer for debugging after the run.
echo "${STORAGE_ROOT}" >"${OUT_DIR}/cold_storage_path.txt"

HAMMER_MODE=native
HAMMERDB_BIN="${HAMMERDB_BIN:-}"
PG_HOST_HAMMER="$PG_HOST_LOCAL"

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
  echo "error: hammerdbcli not found and docker unavailable" >&2
  exit 1
fi

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
  local log="$2"
  echo "→ hammerdb auto $(basename "$tcl")"
  if [[ "${HAMMER_MODE}" == docker ]]; then
    docker run --rm --platform linux/amd64 \
      -v "${OUT_DIR}:/work:ro" \
      -w /home/HammerDB-4.12 \
      "${DOCKER_IMAGE}" \
      ./hammerdbcli auto "/work/$(basename "$tcl")" >"$log" 2>&1
  else
    local hammer_dir
    hammer_dir="$(cd "$(dirname "$HAMMERDB_BIN")" && pwd)"
    (
      cd "$hammer_dir"
      ./hammerdbcli auto "$tcl"
    ) >"$log" 2>&1
  fi
}

parse_nopm_tpm() {
  local log="$1"
  python3 - <<PY
import re
text = open("${log}", encoding="utf-8", errors="replace").read()
m = re.search(r"System achieved\s+(\d+)\s+NOPM from\s+(\d+)\s+PostgreSQL TPM", text)
print("0 0" if not m else f"{m.group(1)} {m.group(2)}")
PY
}

prepare_db() {
  export KOLDSTORE_E2E_PREPARE_ONLY=1
  bash scripts/run-pg-e2e.sh "$PG_VERSION"
  "$PSQL" -h "$PG_HOST_LOCAL" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
    -c "ALTER USER \"${PG_USER}\" PASSWORD '${PG_PASSWORD}'" \
    -c "DROP DATABASE IF EXISTS ${PG_DATABASE}" \
    -c "CREATE DATABASE ${PG_DATABASE}"
  psql_db \
    -c "CREATE EXTENSION IF NOT EXISTS koldstore;" \
    -c "ALTER DATABASE ${PG_DATABASE} SET session_preload_libraries = 'koldstore';"
}

build_schema() {
  fill_tcl "${ROOT_DIR}/scripts/hammerdb/tprocc_build.tcl" "${OUT_DIR}/tprocc_build.generated.tcl"
  run_hammer "${OUT_DIR}/tprocc_build.generated.tcl" "${OUT_DIR}/build.log"
  if ! grep -q "FINISHED SUCCESS" "${OUT_DIR}/build.log"; then
    echo "error: schema build failed; see ${OUT_DIR}/build.log" >&2
    tail -n 60 "${OUT_DIR}/build.log" >&2 || true
    exit 1
  fi
  # Same surrogate PK on every arm so HISTORY point lookups are comparable.
  psql_db -f "${ROOT_DIR}/scripts/hammerdb/prepare_history_pk.sql" \
    >"${OUT_DIR}/prepare_pk.log"
}

run_timed() {
  local name="$1"
  fill_tcl "${ROOT_DIR}/scripts/hammerdb/tprocc_run.tcl" "${OUT_DIR}/tprocc_run.generated.tcl"
  run_hammer "${OUT_DIR}/tprocc_run.generated.tcl" "${OUT_DIR}/${name}.log"
  if ! grep -q "TEST RESULT" "${OUT_DIR}/${name}.log"; then
    echo "error: timed run ${name} missing TEST RESULT; see ${OUT_DIR}/${name}.log" >&2
    tail -n 80 "${OUT_DIR}/${name}.log" >&2 || true
    exit 1
  fi
}

manage_history() {
  psql_db -v "STORAGE_ROOT=${STORAGE_ROOT}" \
    -f "${ROOT_DIR}/scripts/hammerdb/manage_history.sql" | tee "${OUT_DIR}/manage.log"
}

flush_history() {
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

# Read microbench + EXPLAIN proof. expect_merge=1 requires KoldMergeScan + opened cold.
run_read_bench() {
  local arm="$1"
  local expect_merge="${2:-0}"
  local json_out="${OUT_DIR}/reads_${arm}.json"
  local explain_out="${OUT_DIR}/explain_${arm}.txt"
  local -a cmd=(
    python3 "${ROOT_DIR}/scripts/hammerdb/read_bench.py"
    --arm "${arm}"
    --psql "${PSQL}"
    --host "${PG_HOST_LOCAL}"
    --port "${PG_PORT}"
    --database "${PG_DATABASE}"
    --iters "${READ_ITERS}"
    --json-out "${json_out}"
    --explain-out "${explain_out}"
  )
  if [[ "${expect_merge}" == "1" ]]; then
    cmd+=(--expect-merge)
  fi
  "${cmd[@]}"
  echo "reads/${arm}: $(tr -d '\n' <"${json_out}")"
}

run_arm() {
  local arm="$1"
  local expect_merge="${2:-0}"
  echo "=== arm: ${arm} ==="
  run_timed "${arm}"
  read -r NOPM TPM < <(parse_nopm_tpm "${OUT_DIR}/${arm}.log")
  echo "${arm}: ${NOPM} NOPM / ${TPM} TPM"
  run_read_bench "${arm}" "${expect_merge}"
  # shellcheck disable=SC2034
  eval "${arm}_NOPM=${NOPM}"
  eval "${arm}_TPM=${TPM}"
  printf '%s' "${NOPM}" >"${OUT_DIR}/nopm_${arm}.txt"
  printf '%s' "${TPM}" >"${OUT_DIR}/tpm_${arm}.txt"
}

echo "=== HammerDB 3-arm compare: ${WAREHOUSES} WH / ${VIRTUAL_USERS} VU / ${DURATION}m ==="
echo "Arms: baseline → hot_only (managed, no flush) → hot_cold (managed + flush)"

# --- baseline ---
prepare_db
build_schema
run_arm baseline 0

# --- hot_only: rebuild fair schema, manage, do NOT flush ---
prepare_db
build_schema
manage_history
run_arm hot_only 0

# --- hot_cold: rebuild, manage, flush, then run ---
prepare_db
build_schema
manage_history
flush_history
run_arm hot_cold 1

python3 - <<PY
import json
from pathlib import Path

out_dir = Path("${OUT_DIR}")
chart_dir = Path("${CHART_DIR}")

def load_json(name):
    return json.loads((out_dir / name).read_text(encoding="utf-8"))

def nopm(arm):
    return int((out_dir / f"nopm_{arm}.txt").read_text().strip() or "0")

def tpm(arm):
    return int((out_dir / f"tpm_{arm}.txt").read_text().strip() or "0")

arms = ["baseline", "hot_only", "hot_cold"]
report = {
    "postgresql_version": "${PG_VERSION}",
    "warehouses": int("${WAREHOUSES}"),
    "virtual_users": int("${VIRTUAL_USERS}"),
    "duration_minutes": int("${DURATION}"),
    "read_iters": int("${READ_ITERS}"),
    "hammer_mode": "${HAMMER_MODE}",
    "policy": "manage HISTORY only; customer/orders/stock remain unmanaged",
    "note": "HammerDB TPROC-C mostly inserts HISTORY; read microbench + EXPLAIN prove hot/cold scan behavior.",
    "arms": {},
}
for arm in arms:
    report["arms"][arm] = {
        "nopm": nopm(arm),
        "tpm": tpm(arm),
        "reads": load_json(f"reads_{arm}.json"),
    }

base_nopm = report["arms"]["baseline"]["nopm"]
base_hist = report["arms"]["baseline"]["reads"]["history_pk_ms"]
base_cust = report["arms"]["baseline"]["reads"]["customer_pk_ms"]

def pct(new, old):
    return None if old == 0 else round(100.0 * (new - old) / old, 2)

report["delta_vs_baseline_pct"] = {
    arm: {
        "nopm": pct(report["arms"][arm]["nopm"], base_nopm),
        "history_pk_ms": pct(report["arms"][arm]["reads"]["history_pk_ms"], base_hist),
        "customer_pk_ms": pct(report["arms"][arm]["reads"]["customer_pk_ms"], base_cust),
    }
    for arm in ("hot_only", "hot_cold")
}

(out_dir / "results.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
print(json.dumps(report["delta_vs_baseline_pct"], indent=2))
print(f"wrote {out_dir / 'results.json'}")
PY

python3 "${ROOT_DIR}/scripts/hammerdb/chart_results.py" \
  --results "${RESULTS_JSON}" \
  --out-dir "${CHART_DIR}"

echo "compare complete"
echo "  results: ${RESULTS_JSON}"
echo "  explains: ${OUT_DIR}/explain_*.txt"
echo "  charts: ${CHART_DIR}/hammerdb-*.svg"
