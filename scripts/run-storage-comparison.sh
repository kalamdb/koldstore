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
SIDE="${KOLDSTORE_STORAGE_SIDE:-}"
UPDATE_RESULTS=0
ALL_SIDES=0
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
  --mode MODE       strict or async (default: strict). Used for interleaved
                    smoke runs when --side / --all-sides are omitted.
  --side SIDE       Isolated fair measurement: pg | async | strict
                    Fresh-prepares PostgreSQL for that side alone.
  --all-sides       Run pg, then async, then strict — each on a fresh server
                    (stop → recreate DBs → measure one side). Preferred for
                    docs/benchmarks/RESULTS.md.
  --both-modes      Deprecated alias for --all-sides
  --update-results  Merge JSON snapshots into docs/benchmarks/RESULTS.md
  --pg-version N    PostgreSQL major version (default: 16)
  --prepare-only    Prepare pgrx + extension only, skip the test
  -h, --help        Show this help text

Environment overrides (used when the matching flag is omitted):
  KOLDSTORE_STORAGE_ROWS
  KOLDSTORE_STORAGE_HOT_LIMIT
  KOLDSTORE_STORAGE_DML_SAMPLE
  KOLDSTORE_STORAGE_INSERT_BATCH_ROWS
  KOLDSTORE_STORAGE_MIRROR_CAPTURE_MODE
  KOLDSTORE_STORAGE_SIDE
  KOLDSTORE_STORAGE_PGVERSION / KOLDSTORE_E2E_PGVERSION
  KOLDSTORE_STORAGE_PREPARE_ONLY
  KOLDSTORE_STORAGE_RESULTS_DIR
  KOLDSTORE_STORAGE_RESULTS_MD

Published methodology (--all-sides):
  1. PostgreSQL only — fresh cluster, no managed table, no logical WAL
  2. PG + KoldStore (async) — fresh cluster, wal_level=logical
  3. PG + KoldStore (strict) — fresh cluster, trigger capture (no logical WAL)

Interleaved dual-table smoke (default without --side/--all-sides) is fine for
local debugging but must not be published to RESULTS.md.

Examples:
  scripts/run-storage-comparison.sh
  scripts/run-storage-comparison.sh --all-sides --update-results \
    --rows 10000000 --hot-limit 100000 --dml-sample 50000
  scripts/run-storage-comparison.sh --side async --update-results \
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
    --side)
      SIDE="${2:?missing value for --side}"
      shift 2
      ;;
    --side=*)
      SIDE="${1#*=}"
      shift
      ;;
    --all-sides|--both-modes)
      ALL_SIDES=1
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

if [[ -n "${SIDE}" ]]; then
  case "${SIDE}" in
    pg|postgres|baseline|async|strict) ;;
    *)
      echo "error: --side must be pg, async, or strict (got: ${SIDE})" >&2
      exit 1
      ;;
  esac
fi

if [[ "${ALL_SIDES}" == "1" && -n "${SIDE}" ]]; then
  echo "error: use either --all-sides or --side, not both" >&2
  exit 1
fi

E2E_ENV_FILE="${KOLDSTORE_E2E_ENV_FILE:-$ROOT_DIR/.e2e-env}"
PG_FEATURE="pg${PG_VERSION}"

# Map measurement side → prepare mode (wal_level). pg/strict stay off logical WAL.
prepare_mode_for_side() {
  case "$1" in
    async) echo "async" ;;
    *) echo "strict" ;;
  esac
}

normalize_side() {
  case "$1" in
    postgres|baseline) echo "pg" ;;
    *) echo "$1" ;;
  esac
}

prepare_fresh_server() {
  local prepare_mode="$1"
  local skip_install="${2:-0}"
  echo "────────────────────────────────────────────────────────────"
  echo "fresh PostgreSQL ${PG_VERSION}: stop → restart → recreate DBs (prepare --mode ${prepare_mode}, skip_install=${skip_install})"
  echo "────────────────────────────────────────────────────────────"
  cargo pgrx stop "${PG_FEATURE}" || true
  # Drop OS-visible shared state from the prior side; recreating DBs in
  # run-pg-e2e clears relation data. Shared buffers start empty after start.
  if [[ "${skip_install}" == "1" ]]; then
    KOLDSTORE_E2E_SKIP_INSTALL=1 \
      KOLDSTORE_E2E_PGVERSION="${PG_VERSION}" \
      KOLDSTORE_E2E_PREPARE_ONLY=1 \
      KOLDSTORE_PGRX_INSTALL_RELEASE=1 \
      KOLDSTORE_E2E_MIRROR_CAPTURE_MODE="${prepare_mode}" \
      KOLDSTORE_E2E_THREADS="${KOLDSTORE_E2E_THREADS:-1}" \
      scripts/run-pg-e2e.sh "${PG_VERSION}" --mode "${prepare_mode}"
  else
    KOLDSTORE_E2E_PGVERSION="${PG_VERSION}" \
      KOLDSTORE_E2E_PREPARE_ONLY=1 \
      KOLDSTORE_PGRX_INSTALL_RELEASE=1 \
      KOLDSTORE_E2E_MIRROR_CAPTURE_MODE="${prepare_mode}" \
      KOLDSTORE_E2E_THREADS="${KOLDSTORE_E2E_THREADS:-1}" \
      scripts/run-pg-e2e.sh "${PG_VERSION}" --mode "${prepare_mode}"
  fi
  # shellcheck disable=SC1090
  source "${E2E_ENV_FILE}"
}

EXTENSION_INSTALLED=0

run_isolated_side() {
  local side
  side="$(normalize_side "$1")"
  local prepare_mode
  prepare_mode="$(prepare_mode_for_side "${side}")"
  local results_json=""
  local skip_install=0
  if [[ "${EXTENSION_INSTALLED}" == "1" ]]; then
    skip_install=1
  fi
  prepare_fresh_server "${prepare_mode}" "${skip_install}"
  EXTENSION_INSTALLED=1
  echo "running isolated storage comparison side=${side} (rows=${ROWS}, hot_limit=${HOT_LIMIT}, dml_sample=${DML_SAMPLE}, insert_batch_rows=${INSERT_BATCH_ROWS})"
  if [[ "${UPDATE_RESULTS}" == "1" ]]; then
    mkdir -p "${RESULTS_DIR}"
    results_json="${RESULTS_DIR}/${side}.json"
  fi
  KOLDSTORE_STORAGE_ROWS="${ROWS}" \
    KOLDSTORE_STORAGE_HOT_LIMIT="${HOT_LIMIT}" \
    KOLDSTORE_STORAGE_DML_SAMPLE="${DML_SAMPLE}" \
    KOLDSTORE_STORAGE_INSERT_BATCH_ROWS="${INSERT_BATCH_ROWS}" \
    KOLDSTORE_STORAGE_SIDE="${side}" \
    KOLDSTORE_STORAGE_RESULTS_JSON="${results_json}" \
    cargo nextest run -p storage-comparison --test pg_vs_koldstore --no-capture --test-threads 1
}

run_interleaved_smoke() {
  local mode="$1"
  local prepare_mode="$mode"
  local results_json=""
  echo "running interleaved smoke (--mode ${mode}; not for RESULTS.md)"
  prepare_fresh_server "${prepare_mode}" 0
  EXTENSION_INSTALLED=1
  if [[ "${UPDATE_RESULTS}" == "1" ]]; then
    echo "warning: --update-results with interleaved smoke overwrites only ${mode}.json; prefer --all-sides" >&2
    mkdir -p "${RESULTS_DIR}"
    results_json="${RESULTS_DIR}/${mode}.json"
  fi
  KOLDSTORE_STORAGE_ROWS="${ROWS}" \
    KOLDSTORE_STORAGE_HOT_LIMIT="${HOT_LIMIT}" \
    KOLDSTORE_STORAGE_DML_SAMPLE="${DML_SAMPLE}" \
    KOLDSTORE_STORAGE_INSERT_BATCH_ROWS="${INSERT_BATCH_ROWS}" \
    KOLDSTORE_STORAGE_MIRROR_CAPTURE_MODE="${mode}" \
    KOLDSTORE_STORAGE_SIDE=combined \
    KOLDSTORE_STORAGE_RESULTS_JSON="${results_json}" \
    cargo nextest run -p storage-comparison --test pg_vs_koldstore --no-capture --test-threads 1
}

render_results() {
  python3 "${ROOT_DIR}/scripts/render-storage-comparison-results.py" \
    --pg-json "${RESULTS_DIR}/pg.json" \
    --async-json "${RESULTS_DIR}/async.json" \
    --strict-json "${RESULTS_DIR}/strict.json" \
    --out "${RESULTS_MD}"
  echo "updated ${RESULTS_MD}"
}

if [[ "${PREPARE_ONLY}" == "1" || "${PREPARE_ONLY}" == "true" ]]; then
  prepare_mode="$(prepare_mode_for_side "${SIDE:-async}")"
  if [[ "${ALL_SIDES}" == "1" ]]; then
    prepare_mode="async"
  fi
  prepare_fresh_server "${prepare_mode}" 0
  echo "storage comparison database is ready (prepare-only; skipping test)"
  exit 0
fi

if ! cargo nextest --version >/dev/null 2>&1; then
  echo "error: cargo-nextest is required; install with: cargo install cargo-nextest --locked" >&2
  exit 1
fi

if [[ "${ALL_SIDES}" == "1" ]]; then
  # Order: pg-only first (coldest baseline), then async, then strict.
  run_isolated_side pg
  run_isolated_side async
  run_isolated_side strict
elif [[ -n "${SIDE}" ]]; then
  run_isolated_side "${SIDE}"
else
  run_interleaved_smoke "${MIRROR_CAPTURE_MODE}"
fi

if [[ "${UPDATE_RESULTS}" == "1" ]]; then
  render_results
fi

echo "storage comparison passed"
