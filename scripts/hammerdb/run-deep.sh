#!/usr/bin/env bash
# Deep HammerDB TPROC-C + optional CH-benCHmark-style analytics.
#
# Opt-in only — not used by weekly smoke CI.
# Claim: survived deep selective-manage TPROC-C (+ optional CH) with mid/post
# cold-open proofs. Never "TPC certified" or "production safe".
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
# shellcheck source=scripts/hammerdb/profiles.sh
source "${ROOT_DIR}/scripts/hammerdb/profiles.sh"

PROFILE="standard"
MANAGE_SET="append"
CH_MODE="after"
PG_VERSION=""

usage() {
  cat <<'EOF'
Usage: scripts/hammerdb/run-deep.sh [options] [PG_VERSION]

Options:
  --profile smoke|standard|heavy|custom   Scale profile (default: standard)
  --manage-set history|append|broad       Manage policy (default: append)
  --ch off|after|concurrent|only          CH mode (default: after)
  -h, --help                              Show help

Env overrides: KOLDSTORE_HAMMERDB_{WAREHOUSES,VU,MINUTES,RAMPUP,READ_ITERS}
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="$2"
      shift 2
      ;;
    --manage-set)
      MANAGE_SET="$2"
      shift 2
      ;;
    --ch)
      CH_MODE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -z "$PG_VERSION" && "$1" =~ ^[0-9]+$ ]]; then
        PG_VERSION="$1"
        shift
      else
        echo "error: unknown arg: $1" >&2
        usage >&2
        exit 1
      fi
      ;;
  esac
done

PG_VERSION="${PG_VERSION:-${KOLDSTORE_E2E_PGVERSION:-16}}"
resolve_hammerdb_profile "$PROFILE"

case "$MANAGE_SET" in
  history|append|broad) ;;
  *) echo "error: bad --manage-set ${MANAGE_SET}" >&2; exit 1 ;;
esac
case "$CH_MODE" in
  off|after|concurrent|only) ;;
  *) echo "error: bad --ch ${CH_MODE}" >&2; exit 1 ;;
esac

PG_PORT="${KOLDSTORE_E2E_PGPORT:-288${PG_VERSION}}"
PG_HOST_LOCAL="${KOLDSTORE_E2E_PGHOST:-127.0.0.1}"
PG_USER="${KOLDSTORE_E2E_PGUSER:-$(whoami)}"
PG_PASSWORD="${KOLDSTORE_HAMMERDB_PASSWORD:-hammerdb}"
PG_DATABASE="${KOLDSTORE_HAMMERDB_DB:-koldstore_hammerdb_deep}"
PG_CONFIG="${PGRX_PG_CONFIG:-$(cargo pgrx info pg-config "$PG_VERSION")}"
PSQL="$(dirname "$PG_CONFIG")/psql"
HAMMERDB_BIN="${HAMMERDB_BIN:-}"
DOCKER_IMAGE="${KOLDSTORE_HAMMERDB_DOCKER_IMAGE:-tpcorg/hammerdb:v4.12}"
HAMMER_MODE=native
PG_HOST_HAMMER="$PG_HOST_LOCAL"
SKIP_BUILD="${KOLDSTORE_HAMMERDB_SKIP_BUILD:-0}"
STORAGE_ROOT="${KOLDSTORE_HAMMERDB_STORAGE:-$(mktemp -d "${TMPDIR:-/tmp}/koldstore-hammerdb-deep.XXXXXX")}"
OUT_DIR="${KOLDSTORE_HAMMERDB_OUT:-${ROOT_DIR}/target/hammerdb-deep}"
BUILD_TCL_SRC="${ROOT_DIR}/scripts/hammerdb/tprocc_build.tcl"
RUN_TCL_SRC="${ROOT_DIR}/scripts/hammerdb/tprocc_run.tcl"
MANAGE_SQL="${ROOT_DIR}/scripts/hammerdb/manage_policy.sql"
CH_SCHEMA_SQL="${ROOT_DIR}/scripts/hammerdb/ch_schema.sql"
BUILD_TCL="${OUT_DIR}/tprocc_build.generated.tcl"
RUN_TCL="${OUT_DIR}/tprocc_run.generated.tcl"
RUN_LOG="${OUT_DIR}/hammerdb.log"
SUMMARY_JSON="${OUT_DIR}/summary.json"

# Supplier seed: full 10k for non-smoke; smaller for smoke speed.
if [[ "$PROFILE" == "smoke" ]]; then
  SUPPLIER_COUNT="${KOLDSTORE_HAMMERDB_SUPPLIER_COUNT:-1000}"
else
  SUPPLIER_COUNT="${KOLDSTORE_HAMMERDB_SUPPLIER_COUNT:-10000}"
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
  echo "running: hammerdb auto $(basename "$tcl") (${HAMMER_MODE})"
  if [[ "${HAMMER_MODE}" == docker ]]; then
    docker run --rm --platform linux/amd64 \
      -v "${OUT_DIR}:/work:ro" \
      -w /home/HammerDB-4.12 \
      "${DOCKER_IMAGE}" \
      ./hammerdbcli auto "/work/$(basename "$tcl")" >>"$RUN_LOG" 2>&1
  else
    local hammer_dir
    hammer_dir="$(cd "$(dirname "$HAMMERDB_BIN")" && pwd)"
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

run_proofs() {
  local phase="$1"
  local flush_flag=()
  local expect=()
  local flush_log="${OUT_DIR}/flush_${phase}.log"
  if [[ "${2:-}" == "flush" ]]; then
    flush_flag=(--flush --flush-log "${flush_log}")
    expect=(--expect-cold)
  fi
  python3 "${ROOT_DIR}/scripts/hammerdb/proofs.py" \
    --phase "$phase" \
    --psql "$PSQL" \
    --host "$PG_HOST_LOCAL" \
    --port "$PG_PORT" \
    --database "$PG_DATABASE" \
    "${flush_flag[@]}" \
    "${expect[@]}" \
    --json-out "${OUT_DIR}/proof_${phase}.json" \
    --explain-out "${OUT_DIR}/explain_${phase}.txt"
}

run_ch() {
  local mode="$1"
  local duration_s="${2:-0}"
  local loops="${3:-1}"
  echo "CH mode=${mode} duration_s=${duration_s} loops=${loops}"
  python3 "${ROOT_DIR}/scripts/hammerdb/ch_runner.py" \
    --mode "$mode" \
    --psql "$PSQL" \
    --host "$PG_HOST_LOCAL" \
    --port "$PG_PORT" \
    --database "$PG_DATABASE" \
    --user "$PG_USER" \
    --duration-seconds "$duration_s" \
    --loops "$loops" \
    --json-out "${OUT_DIR}/ch_${mode}.json"
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
  echo "HammerDB not installed; skipping deep run"
  echo "Install HammerDB and set HAMMERDB_BIN, place hammerdbcli on PATH, or install Docker"
  exit 0
fi

for required in "$BUILD_TCL_SRC" "$RUN_TCL_SRC" "$MANAGE_SQL" "$CH_SCHEMA_SQL"; do
  if [[ ! -f "$required" ]]; then
    echo "error: missing ${required}" >&2
    exit 1
  fi
done

mkdir -p "$OUT_DIR" "$STORAGE_ROOT"
: >"$RUN_LOG"
print_hammerdb_profile
echo "manage_set=${MANAGE_SET} ch_mode=${CH_MODE} out=${OUT_DIR}"

export KOLDSTORE_E2E_PREPARE_ONLY=1
bash scripts/run-pg-e2e.sh "$PG_VERSION"

echo "recreating HammerDB database ${PG_DATABASE}"
"$PSQL" -h "$PG_HOST_LOCAL" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -c "ALTER USER \"${PG_USER}\" PASSWORD '${PG_PASSWORD}'" \
  -c "DROP DATABASE IF EXISTS ${PG_DATABASE}" \
  -c "CREATE DATABASE ${PG_DATABASE}"
psql_db -c "CREATE EXTENSION IF NOT EXISTS koldstore;"

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
set -e

echo "applying manage policy set=${MANAGE_SET}"
psql_db \
  -v "STORAGE_ROOT=${STORAGE_ROOT}" \
  -v "MANAGE_SET=${MANAGE_SET}" \
  -f "$MANAGE_SQL" >>"$RUN_LOG" 2>&1

if [[ "$CH_MODE" != "off" ]]; then
  echo "installing CH extension tables (supplier_count=${SUPPLIER_COUNT})"
  psql_db -v "SUPPLIER_COUNT=${SUPPLIER_COUNT}" -f "$CH_SCHEMA_SQL" >>"$RUN_LOG" 2>&1
fi

NOPM=0
TPM=0
CH_PID=""
HAMMER_PID=""

cleanup_bg() {
  if [[ -n "${CH_PID}" ]] && kill -0 "$CH_PID" 2>/dev/null; then
    kill -TERM "$CH_PID" 2>/dev/null || true
    wait "$CH_PID" 2>/dev/null || true
  fi
  if [[ -n "${HAMMER_PID}" ]] && kill -0 "$HAMMER_PID" 2>/dev/null; then
    kill -TERM "$HAMMER_PID" 2>/dev/null || true
    wait "$HAMMER_PID" 2>/dev/null || true
  fi
}
trap cleanup_bg EXIT

if [[ "$CH_MODE" == "only" ]]; then
  echo "ch_mode=only: skipping timed TPROC-C"
  run_proofs pre_ch flush
  run_ch only 0 1
else
  # Timed TPROC-C in background so we can mid-run flush (+ optional concurrent CH).
  set +e
  run_hammer "$RUN_TCL" &
  HAMMER_PID=$!
  set -e

  RAMPUP_S=$(( RAMPUP * 60 ))
  DURATION_S=$(( DURATION * 60 ))
  MID_SLEEP_S=$(( RAMPUP_S + DURATION_S / 2 ))
  if (( MID_SLEEP_S < 30 )); then
    MID_SLEEP_S=30
  fi

  if [[ "$CH_MODE" == "concurrent" ]]; then
    if (( RAMPUP_S > 0 )); then
      echo "waiting ${RAMPUP_S}s rampup before concurrent CH"
      sleep "$RAMPUP_S"
    fi
    CH_DURATION_S=$DURATION_S
    if (( CH_DURATION_S < 60 )); then
      CH_DURATION_S=60
    fi
    run_ch concurrent "$CH_DURATION_S" 1 &
    CH_PID=$!
    # Remaining wait until mid-run checkpoint (already slept rampup).
    REMAIN_MID=$(( MID_SLEEP_S - RAMPUP_S ))
    if (( REMAIN_MID > 0 )); then
      echo "waiting ${REMAIN_MID}s more for mid-run flush/proof"
      sleep "$REMAIN_MID"
    fi
  else
    echo "waiting ${MID_SLEEP_S}s for mid-run flush/proof"
    sleep "$MID_SLEEP_S"
  fi

  if ! kill -0 "$HAMMER_PID" 2>/dev/null; then
    echo "warning: HammerDB finished before mid-run window; proving post-run only"
  else
    echo "mid-run flush + cold proof"
    run_proofs mid_run flush
  fi

  set +e
  wait "$HAMMER_PID"
  hammer_rc=$?
  HAMMER_PID=""
  set -e

  if [[ -n "${CH_PID}" ]]; then
    set +e
    wait "$CH_PID"
    ch_rc=$?
    CH_PID=""
    set -e
    if [[ "$ch_rc" -ne 0 ]]; then
      echo "error: concurrent CH runner failed (${ch_rc})" >&2
      exit "$ch_rc"
    fi
  fi

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

  read -r NOPM TPM < <(parse_nopm_tpm)
  echo "timed result: ${NOPM} NOPM / ${TPM} TPM"

  echo "post-run flush + cold proof"
  run_proofs post_run flush

  if [[ "$CH_MODE" == "after" ]]; then
    run_ch after 0 1
  fi
fi

python3 - <<PY
import json
from pathlib import Path

out = Path("${OUT_DIR}")
proofs = {}
for path in sorted(out.glob("proof_*.json")):
    proofs[path.stem] = json.loads(path.read_text(encoding="utf-8"))
ch = {}
for path in sorted(out.glob("ch_*.json")):
    ch[path.stem] = json.loads(path.read_text(encoding="utf-8"))

summary = {
    "kind": "deep-hammerdb",
    "postgresql_version": "${PG_VERSION}",
    "profile": "${PROFILE}",
    "manage_set": "${MANAGE_SET}",
    "ch_mode": "${CH_MODE}",
    "warehouses": int("${WAREHOUSES}"),
    "virtual_users": int("${VIRTUAL_USERS}"),
    "duration_minutes": int("${DURATION}"),
    "rampup_minutes": int("${RAMPUP}"),
    "read_iters": int("${READ_ITERS}"),
    "hammer_mode": "${HAMMER_MODE}",
    "supplier_count": int("${SUPPLIER_COUNT}"),
    "claim": (
        "KoldStore survived deep selective-manage HammerDB TPROC-C "
        "(profile=${PROFILE}, manage_set=${MANAGE_SET}, ch=${CH_MODE}), "
        "with mid/post-run cold Parquet open proofs. "
        "Not a TPC certification; not a claim of production safety."
    ),
    "nopm": int("${NOPM}"),
    "tpm": int("${TPM}"),
    "proofs": proofs,
    "ch": ch,
}
(out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

echo "Deep HammerDB completed"
echo "Summary: ${SUMMARY_JSON}"
echo "Claim wording: survived deep selective-manage TPROC-C (+ optional CH) with cold proofs — never 'TPC certified'."
trap - EXIT
