#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
# shellcheck source=lib/pgrx-lifecycle.sh
source "${ROOT_DIR}/scripts/lib/pgrx-lifecycle.sh"

PG_VERSION="${KOLDSTORE_STORAGE_PGVERSION:-${KOLDSTORE_E2E_PGVERSION:-16}}"
PREPARE_ONLY="${KOLDSTORE_STORAGE_PREPARE_ONLY:-0}"
ROWS="${KOLDSTORE_STORAGE_ROWS:-100000}"
HOT_LIMIT="${KOLDSTORE_STORAGE_HOT_LIMIT:-10000}"
DML_SAMPLE="${KOLDSTORE_STORAGE_DML_SAMPLE:-1000}"
INSERT_BATCH_ROWS="${KOLDSTORE_STORAGE_INSERT_BATCH_ROWS:-100000}"
WARMUP_ROWS="${KOLDSTORE_STORAGE_WARMUP_ROWS:-}"
SIDE="${KOLDSTORE_STORAGE_SIDE:-}"
UPDATE_RESULTS=0
ALL_SIDES=0
RESULTS_DIR="${KOLDSTORE_STORAGE_RESULTS_DIR:-${ROOT_DIR}/docs/benchmarks/.storage-results}"
RESULTS_MD="${KOLDSTORE_STORAGE_RESULTS_MD:-${ROOT_DIR}/docs/benchmarks/RESULTS.md}"

usage() {
  cat <<'EOF'
Run the PostgreSQL vs KoldStore storage comparison harness (tests/storage/).

Exactly three measurements, each on a fresh pgrx PostgreSQL:

  1. pg      — PostgreSQL only
  2. async   — PG + KoldStore (async mirror)
  3. strict  — PG + KoldStore (strict / trigger mirror)

Usage:
  scripts/run-storage-comparison.sh --all-sides [options]
  scripts/run-storage-comparison.sh --side pg|async|strict [options]

Options:
  --rows N          Total rows seeded (default: 100000)
  --hot-limit N     Rows kept hot after flush (default: 10000)
  --dml-sample N    Rows for timed UPDATE/DELETE samples (default: 1000)
  --insert-batch-rows N  Rows per committed insert batch (default: 100000)
  --warmup-rows N   Untimed warm-up inserts before timed seed (default: scale-aware,
                      min(rows, max(1M, 5*batch)); 0 disables)
  --side SIDE       Run one side only: pg | async | strict
  --all-sides       Run all three sides once each (fresh server per side)
  --both-modes      Deprecated alias for --all-sides
  --update-results  Merge JSON into docs/benchmarks/RESULTS.md
  --pg-version N    PostgreSQL major version (default: 16)
  --prepare-only    Prepare pgrx + extension only, skip the test
  -h, --help        Show this help text

Examples:
  scripts/run-storage-comparison.sh --all-sides --update-results \
    --rows 10000000 --hot-limit 100000 --dml-sample 50000
  scripts/run-storage-comparison.sh --side async --rows 100000
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
    --warmup-rows)
      WARMUP_ROWS="${2:?missing value for --warmup-rows}"
      shift 2
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
    # Kept for older docs/scripts; ignored — sides are isolated now.
    --mode|--mode=*)
      if [[ "$1" == --mode ]]; then
        shift 2
      else
        shift
      fi
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

if [[ "${ALL_SIDES}" != "1" && -z "${SIDE}" && "${PREPARE_ONLY}" != "1" && "${PREPARE_ONLY}" != "true" ]]; then
  echo "error: pass --all-sides (run pg+async+strict once each) or --side pg|async|strict" >&2
  usage >&2
  exit 1
fi

E2E_ENV_FILE="${KOLDSTORE_E2E_ENV_FILE:-$ROOT_DIR/.e2e-env}"
PG_FEATURE="pg${PG_VERSION}"

normalize_side() {
  case "$1" in
    postgres|baseline) echo "pg" ;;
    *) echo "$1" ;;
  esac
}

# Async needs logical WAL; pg/strict are fine with the same prepare.
prepare_fresh_server() {
  local skip_install="${1:-0}"
  echo "────────────────────────────────────────────────────────────"
  echo "fresh PostgreSQL ${PG_VERSION} for next side (skip_install=${skip_install})"
  echo "────────────────────────────────────────────────────────────"
  # Async sides leave bgworkers that make a plain `cargo pgrx stop` race the
  # next start ("could not start server"). Force-stop until the port is free.
  pgrx_force_stop "${PG_VERSION}" || true
  if [[ "${skip_install}" == "1" ]]; then
    KOLDSTORE_E2E_SKIP_INSTALL=1 \
      KOLDSTORE_E2E_PGVERSION="${PG_VERSION}" \
      KOLDSTORE_E2E_PREPARE_ONLY=1 \
      KOLDSTORE_PGRX_INSTALL_RELEASE=1 \
      KOLDSTORE_E2E_MIRROR_CAPTURE_MODE=async \
      KOLDSTORE_E2E_THREADS=1 \
      scripts/run-pg-e2e.sh "${PG_VERSION}" --mode async
  else
    KOLDSTORE_E2E_PGVERSION="${PG_VERSION}" \
      KOLDSTORE_E2E_PREPARE_ONLY=1 \
      KOLDSTORE_PGRX_INSTALL_RELEASE=1 \
      KOLDSTORE_E2E_MIRROR_CAPTURE_MODE=async \
      KOLDSTORE_E2E_THREADS=1 \
      scripts/run-pg-e2e.sh "${PG_VERSION}" --mode async
  fi
  # shellcheck disable=SC1090
  source "${E2E_ENV_FILE}"
}

EXTENSION_INSTALLED=0

run_isolated_side() {
  local side
  side="$(normalize_side "$1")"
  local results_json=""
  local skip_install=0
  if [[ "${EXTENSION_INSTALLED}" == "1" ]]; then
    skip_install=1
  fi
  prepare_fresh_server "${skip_install}"
  EXTENSION_INSTALLED=1
  echo "running side=${side} once (rows=${ROWS}, hot_limit=${HOT_LIMIT}, dml_sample=${DML_SAMPLE}, insert_batch_rows=${INSERT_BATCH_ROWS}, warmup_rows=${WARMUP_ROWS:-auto})"
  if [[ "${UPDATE_RESULTS}" == "1" ]]; then
    mkdir -p "${RESULTS_DIR}"
    results_json="${RESULTS_DIR}/${side}.json"
  fi
  local git_commit
  local git_dirty=0
  git_commit="$(git -C "${ROOT_DIR}" rev-parse HEAD 2>/dev/null || true)"
  if [[ -n "${git_commit}" ]] && ! git -C "${ROOT_DIR}" diff --quiet 2>/dev/null; then
    git_dirty=1
  elif [[ -n "${git_commit}" ]] && ! git -C "${ROOT_DIR}" diff --cached --quiet 2>/dev/null; then
    git_dirty=1
  fi
  local -a env_args=(
    "KOLDSTORE_STORAGE_ROWS=${ROWS}"
    "KOLDSTORE_STORAGE_HOT_LIMIT=${HOT_LIMIT}"
    "KOLDSTORE_STORAGE_DML_SAMPLE=${DML_SAMPLE}"
    "KOLDSTORE_STORAGE_INSERT_BATCH_ROWS=${INSERT_BATCH_ROWS}"
    "KOLDSTORE_STORAGE_SIDE=${side}"
    "KOLDSTORE_STORAGE_GIT_COMMIT=${git_commit}"
    "KOLDSTORE_STORAGE_GIT_DIRTY=${git_dirty}"
    "KOLDSTORE_STORAGE_RESULTS_JSON=${results_json}"
  )
  if [[ -n "${WARMUP_ROWS}" ]]; then
    env_args+=("KOLDSTORE_STORAGE_WARMUP_ROWS=${WARMUP_ROWS}")
  fi
  env "${env_args[@]}" \
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
  prepare_fresh_server 0
  echo "storage comparison database is ready (prepare-only; skipping test)"
  exit 0
fi

if ! cargo nextest --version >/dev/null 2>&1; then
  echo "error: cargo-nextest is required; install with: cargo install cargo-nextest --locked" >&2
  exit 1
fi

if [[ "${ALL_SIDES}" == "1" ]]; then
  run_isolated_side pg
  run_isolated_side async
  run_isolated_side strict
else
  run_isolated_side "${SIDE}"
fi

if [[ "${UPDATE_RESULTS}" == "1" ]]; then
  render_results
fi

echo "storage comparison passed"
