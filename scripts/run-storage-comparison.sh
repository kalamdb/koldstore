#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${KOLDSTORE_STORAGE_PGVERSION:-${KOLDSTORE_E2E_PGVERSION:-16}}"
PREPARE_ONLY="${KOLDSTORE_STORAGE_PREPARE_ONLY:-0}"
ROWS="${KOLDSTORE_STORAGE_ROWS:-100000}"
HOT_LIMIT="${KOLDSTORE_STORAGE_HOT_LIMIT:-10000}"
DML_SAMPLE="${KOLDSTORE_STORAGE_DML_SAMPLE:-1000}"
INSERT_BATCH_ROWS="${KOLDSTORE_STORAGE_INSERT_BATCH_ROWS:-100000}"
MIRROR_CAPTURE_MODE="${KOLDSTORE_STORAGE_MIRROR_CAPTURE_MODE:-strict}"
UPDATE_RESULTS=0
BOTH_MODES=0
RESULTS_DIR="${KOLDSTORE_STORAGE_RESULTS_DIR:-${ROOT_DIR}/docs/benchmarks/.storage-results}"
RESULTS_MD="${KOLDSTORE_STORAGE_RESULTS_MD:-${ROOT_DIR}/docs/benchmarks/RESULTS.md}"

usage() {
  cat <<'EOF'
Run the PostgreSQL vs KoldStore storage comparison harness (tests/storage/).

Usage:
  scripts/run-storage-comparison.sh [options]

Options:
  --rows N          Total rows seeded per table (default: 100000)
  --hot-limit N     Rows kept hot after flush (default: 10000)
  --dml-sample N    Rows used for timed UPDATE/DELETE samples (default: 1000)
  --insert-batch-rows N  Rows per committed insert batch (default: 100000)
  --mode MODE       strict or async (default: strict)
  --both-modes      Run async then strict (implies measuring both columns)
  --update-results  Merge this run into docs/benchmarks/RESULTS.md
                    (writes JSON under docs/benchmarks/.storage-results/)
  --pg-version N    PostgreSQL major version (default: 16)
  --prepare-only    Prepare pgrx + extension only, skip the test
  -h, --help        Show this help text

Environment overrides (used when the matching flag is omitted):
  KOLDSTORE_STORAGE_ROWS
  KOLDSTORE_STORAGE_HOT_LIMIT
  KOLDSTORE_STORAGE_DML_SAMPLE
  KOLDSTORE_STORAGE_INSERT_BATCH_ROWS
  KOLDSTORE_STORAGE_MIRROR_CAPTURE_MODE
  KOLDSTORE_STORAGE_PGVERSION / KOLDSTORE_E2E_PGVERSION
  KOLDSTORE_STORAGE_PREPARE_ONLY
  KOLDSTORE_STORAGE_RESULTS_DIR
  KOLDSTORE_STORAGE_RESULTS_MD

The harness prints a markdown comparison table with columns:
  PostgreSQL only | PG + KoldStore (async) | PG + KoldStore (strict)
Use the `release-pg` extension profile for fair hot+cold timings.

Examples:
  scripts/run-storage-comparison.sh
  scripts/run-storage-comparison.sh --rows 1000000
  scripts/run-storage-comparison.sh --rows 100000 --hot-limit 10000 --dml-sample 100000
  scripts/run-storage-comparison.sh --mode async --update-results
  scripts/run-storage-comparison.sh --both-modes --update-results \
    --rows 10000000 --hot-limit 100000 --dml-sample 50000
  scripts/run-storage-comparison.sh --prepare-only
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rows)
      ROWS="${2:?missing value for --rows}"
      shift 2
      ;;
    --hot-limit)
      HOT_LIMIT="${2:?missing value for --hot-limit}"
      shift 2
      ;;
    --dml-sample)
      DML_SAMPLE="${2:?missing value for --dml-sample}"
      shift 2
      ;;
    --insert-batch-rows)
      INSERT_BATCH_ROWS="${2:?missing value for --insert-batch-rows}"
      shift 2
      ;;
    --mode)
      MIRROR_CAPTURE_MODE="${2:?missing value for --mode}"
      shift 2
      ;;
    --mode=*)
      MIRROR_CAPTURE_MODE="${1#*=}"
      shift
      ;;
    --both-modes)
      BOTH_MODES=1
      shift
      ;;
    --update-results)
      UPDATE_RESULTS=1
      shift
      ;;
    --pg-version)
      PG_VERSION="${2:?missing value for --pg-version}"
      shift 2
      ;;
    --prepare-only)
      PREPARE_ONLY=1
      shift
      ;;
    -h|--help|help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${MIRROR_CAPTURE_MODE}" != "strict" && "${MIRROR_CAPTURE_MODE}" != "async" ]]; then
  echo "error: --mode must be strict or async (got: ${MIRROR_CAPTURE_MODE})" >&2
  exit 1
fi

# Async (and --both-modes) need logical WAL; preparing as async is safe for strict too.
PREPARE_MODE="${MIRROR_CAPTURE_MODE}"
if [[ "${BOTH_MODES}" == "1" ]]; then
  PREPARE_MODE="async"
fi

echo "preparing pgrx PostgreSQL ${PG_VERSION} for storage comparison (release extension, --mode ${PREPARE_MODE})"
E2E_ENV_FILE="${KOLDSTORE_E2E_ENV_FILE:-$ROOT_DIR/.e2e-env}"
KOLDSTORE_E2E_PGVERSION="${PG_VERSION}" \
  KOLDSTORE_E2E_PREPARE_ONLY=1 \
  KOLDSTORE_PGRX_INSTALL_RELEASE=1 \
  KOLDSTORE_E2E_MIRROR_CAPTURE_MODE="${PREPARE_MODE}" \
  scripts/run-pg-e2e.sh "${PG_VERSION}" --mode "${PREPARE_MODE}"
# Prepare-only creates worker DBs and writes pool env; source it for nextest.
# shellcheck disable=SC1090
source "${E2E_ENV_FILE}"

if [[ "${PREPARE_ONLY}" == "1" || "${PREPARE_ONLY}" == "true" ]]; then
  echo "storage comparison database is ready (prepare-only; skipping test)"
  exit 0
fi

if ! cargo nextest --version >/dev/null 2>&1; then
  echo "error: cargo-nextest is required; install with: cargo install cargo-nextest --locked" >&2
  exit 1
fi

run_one_mode() {
  local mode="$1"
  local results_json=""
  echo "running storage comparison (rows=${ROWS}, hot_limit=${HOT_LIMIT}, dml_sample=${DML_SAMPLE}, insert_batch_rows=${INSERT_BATCH_ROWS}, --mode ${mode})"
  if [[ "${UPDATE_RESULTS}" == "1" ]]; then
    mkdir -p "${RESULTS_DIR}"
    results_json="${RESULTS_DIR}/${mode}.json"
  fi
  KOLDSTORE_STORAGE_ROWS="${ROWS}" \
    KOLDSTORE_STORAGE_HOT_LIMIT="${HOT_LIMIT}" \
    KOLDSTORE_STORAGE_DML_SAMPLE="${DML_SAMPLE}" \
    KOLDSTORE_STORAGE_INSERT_BATCH_ROWS="${INSERT_BATCH_ROWS}" \
    KOLDSTORE_STORAGE_MIRROR_CAPTURE_MODE="${mode}" \
    KOLDSTORE_STORAGE_RESULTS_JSON="${results_json}" \
    cargo nextest run -p storage-comparison --test pg_vs_koldstore --no-capture --test-threads 1
}

if [[ "${BOTH_MODES}" == "1" ]]; then
  run_one_mode async
  run_one_mode strict
else
  run_one_mode "${MIRROR_CAPTURE_MODE}"
fi

if [[ "${UPDATE_RESULTS}" == "1" ]]; then
  python3 "${ROOT_DIR}/scripts/render-storage-comparison-results.py" \
    --async-json "${RESULTS_DIR}/async.json" \
    --strict-json "${RESULTS_DIR}/strict.json" \
    --out "${RESULTS_MD}"
  echo "updated ${RESULTS_MD}"
fi

echo "storage comparison passed"
