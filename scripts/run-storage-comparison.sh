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
REPETITIONS="${KOLDSTORE_STORAGE_REPETITIONS:-1}"
SIDE="${KOLDSTORE_STORAGE_SIDE:-}"
UPDATE_RESULTS=0
RENDER_ONLY=0
ALL_SIDES=0
# Optional directory for per-side JSON without RESULTS.md publication gates (CI).
WRITE_JSON_DIR="${KOLDSTORE_STORAGE_WRITE_JSON_DIR:-}"
RESULTS_DIR="${KOLDSTORE_STORAGE_RESULTS_DIR:-${ROOT_DIR}/docs/benchmarks/.storage-results}"
RESULTS_MD="${KOLDSTORE_STORAGE_RESULTS_MD:-${ROOT_DIR}/docs/benchmarks/RESULTS.md}"
CURRENT_REPETITION=1
# Full publication requires a multiple of 6. Draft single-sample RESULTS updates
# set KOLDSTORE_STORAGE_DRAFT_RESULTS=1 (still requires --update-results + clean tree
# unless draft mode; draft stamps git_dirty=0 so the renderer can aggregate).
if [[ "${KOLDSTORE_STORAGE_DRAFT_RESULTS:-0}" == "1" ]]; then
  MIN_PUBLISH_REPETITIONS=1
else
  MIN_PUBLISH_REPETITIONS=6
fi

usage() {
  cat <<'EOF'
Run the PostgreSQL vs KoldStore storage comparison harness (tests/storage/).

Two isolated sides, each on a fresh pgrx PostgreSQL:

  1. pg      — PostgreSQL only
  2. async   — PG + KoldStore (WAL-only mirror)

Usage:
  scripts/run-storage-comparison.sh --all-sides [options]
  scripts/run-storage-comparison.sh --side pg|async [options]
  scripts/run-storage-comparison.sh --render-only --repetitions N

Options:
  --rows N          Total rows seeded (default: 100000)
  --hot-limit N     Rows kept hot after flush (default: 10000)
  --dml-sample N    Rows for timed UPDATE/DELETE samples (default: 1000)
  --insert-batch-rows N  Rows per committed insert batch (default: 100000)
  --warmup-rows N   Untimed warm-up inserts before timed seed (default: scale-aware,
                      min(rows, max(1M, 5*batch)); 0 disables)
  --repetitions N    Isolated samples per side (default: 1; publishing requires
                      a multiple of 6)
  --side SIDE       Run one side only: pg | async
  --all-sides       Run both sides per repetition (fresh server per side)
  --update-results  Merge JSON into docs/benchmarks/RESULTS.md and print it
                      (clean tree + multiple-of-6 repetitions required)
  --write-json-dir DIR  Write <side>.json under DIR after each side (no
                      RESULTS.md gates; for CI artifact upload)
  --render-only     Re-render RESULTS.md from existing run-NN/*.json under
                      docs/benchmarks/.storage-results (no benchmark). Implies
                      --update-results output path; still prints to console.
  --pg-version N    PostgreSQL major version (default: 16)
  --prepare-only    Prepare pgrx + extension only, skip the test
  -h, --help        Show this help text

Notes:
  The storage harness sizes koldstore_max_rows_per_flush to (rows - hot_limit)
  so one flush_table can drain the policy excess (override with
  KOLDSTORE_STORAGE_MAX_ROWS_PER_FLUSH). Product default 10k/wave × 64 waves
  only covers 640k rows per job — too small for published 10M runs.

  With --update-results, the final markdown is written to RESULTS.md and printed
  to the console.

Examples:
  scripts/run-storage-comparison.sh --all-sides --repetitions 6 --update-results \
    --rows 10000000 --hot-limit 100000 --dml-sample 50000
  scripts/run-storage-comparison.sh --side async --rows 100000
  scripts/run-storage-comparison.sh --render-only --repetitions 1
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
    --repetitions)
      REPETITIONS="${2:?missing value for --repetitions}"
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
    --all-sides)
      ALL_SIDES=1
      shift
      ;;
    --update-results)
      UPDATE_RESULTS=1
      shift
      ;;
    --write-json-dir)
      WRITE_JSON_DIR="${2:?missing value for --write-json-dir}"
      shift 2
      ;;
    --render-only)
      RENDER_ONLY=1
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

if [[ -n "${SIDE}" ]]; then
  case "${SIDE}" in
    pg|postgres|baseline|async) ;;
    *)
      echo "error: --side must be pg or async (got: ${SIDE})" >&2
      exit 1
      ;;
  esac
fi

if [[ "${ALL_SIDES}" == "1" && -n "${SIDE}" ]]; then
  echo "error: use either --all-sides or --side, not both" >&2
  exit 1
fi

if [[ "${RENDER_ONLY}" == "1" ]]; then
  if [[ "${ALL_SIDES}" == "1" || -n "${SIDE}" || "${PREPARE_ONLY}" == "1" || "${PREPARE_ONLY}" == "true" ]]; then
    echo "error: --render-only cannot be combined with --all-sides, --side, or --prepare-only" >&2
    exit 1
  fi
elif [[ "${ALL_SIDES}" != "1" && -z "${SIDE}" && "${PREPARE_ONLY}" != "1" && "${PREPARE_ONLY}" != "true" ]]; then
  echo "error: pass --all-sides (run pg+async per repetition) or --side pg|async" >&2
  usage >&2
  exit 1
fi

if ! [[ "${REPETITIONS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "error: --repetitions must be a positive integer (got: ${REPETITIONS})" >&2
  exit 1
fi

if [[ "${UPDATE_RESULTS}" == "1" && "${RENDER_ONLY}" != "1" && "${ALL_SIDES}" != "1" ]]; then
  echo "error: --update-results requires --all-sides" >&2
  exit 1
fi

if [[ "${UPDATE_RESULTS}" == "1" && "${RENDER_ONLY}" != "1" && "${REPETITIONS}" -lt "${MIN_PUBLISH_REPETITIONS}" ]]; then
  echo "error: --update-results requires at least ${MIN_PUBLISH_REPETITIONS} counterbalanced repetitions per side" >&2
  exit 1
fi

if [[ "${UPDATE_RESULTS}" == "1" && "${RENDER_ONLY}" != "1" && "${KOLDSTORE_STORAGE_DRAFT_RESULTS:-0}" != "1" && $((REPETITIONS % 6)) -ne 0 ]]; then
  echo "error: --update-results requires a multiple of 6 repetitions for balanced side ordering" >&2
  exit 1
fi

if [[ "${UPDATE_RESULTS}" == "1" && "${RENDER_ONLY}" != "1" && "${KOLDSTORE_STORAGE_DRAFT_RESULTS:-0}" != "1" ]] && [[ -n "$(git -C "${ROOT_DIR}" status --porcelain --untracked-files=normal)" ]]; then
  echo "error: --update-results requires a clean git worktree; commit or stash changes before publishing" >&2
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

# Managed sides need logical WAL; prepare the same way for every side.
prepare_fresh_server() {
  local skip_install="${1:-0}"
  echo "────────────────────────────────────────────────────────────"
  echo "fresh PostgreSQL ${PG_VERSION} for next side (skip_install=${skip_install})"
  echo "────────────────────────────────────────────────────────────"
  # Async sides leave bgworkers that make a plain `cargo pgrx stop` race the
  # next start ("could not start server"). Force-stop until the port is free.
  pgrx_force_stop "${PG_VERSION}" || true
  # Full cluster wipe (initdb on next start) so insert timing is not skewed by
  # leftover WAL, clog, or a dirty data directory from the previous side.
  pgrx_wipe_data "${PG_VERSION}" || true
  if [[ "${skip_install}" == "1" ]]; then
    KOLDSTORE_E2E_SKIP_INSTALL=1 \
      KOLDSTORE_E2E_PGVERSION="${PG_VERSION}" \
      KOLDSTORE_E2E_PREPARE_ONLY=1 \
      KOLDSTORE_PGRX_INSTALL_RELEASE=1 \
      KOLDSTORE_E2E_THREADS=1 \
      scripts/run-pg-e2e.sh "${PG_VERSION}"
  else
    KOLDSTORE_E2E_PGVERSION="${PG_VERSION}" \
      KOLDSTORE_E2E_PREPARE_ONLY=1 \
      KOLDSTORE_PGRX_INSTALL_RELEASE=1 \
      KOLDSTORE_E2E_THREADS=1 \
      scripts/run-pg-e2e.sh "${PG_VERSION}"
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
  echo "running repetition=${CURRENT_REPETITION}/${REPETITIONS} side=${side} (rows=${ROWS}, hot_limit=${HOT_LIMIT}, dml_sample=${DML_SAMPLE}, insert_batch_rows=${INSERT_BATCH_ROWS}, warmup_rows=${WARMUP_ROWS:-auto})"
  if [[ -n "${WRITE_JSON_DIR}" ]]; then
    mkdir -p "${WRITE_JSON_DIR}"
    results_json="${WRITE_JSON_DIR}/${side}.json"
  elif [[ "${UPDATE_RESULTS}" == "1" ]]; then
    local repetition_dir
    repetition_dir="${RESULTS_DIR}/run-$(printf '%02d' "${CURRENT_REPETITION}")"
    mkdir -p "${repetition_dir}"
    results_json="${repetition_dir}/${side}.json"
  fi
  local git_commit
  local git_dirty=0
  git_commit="$(git -C "${ROOT_DIR}" rev-parse HEAD 2>/dev/null || true)"
  # Draft RESULTS updates may keep local script/docs WIP; stamp the sample against
  # HEAD only so the renderer can aggregate. Full publication still requires a
  # clean tree (see gate above).
  if [[ "${KOLDSTORE_STORAGE_DRAFT_RESULTS:-0}" != "1" ]]; then
    if [[ -n "${git_commit}" ]] && ! git -C "${ROOT_DIR}" diff --quiet 2>/dev/null; then
      git_dirty=1
    elif [[ -n "${git_commit}" ]] && ! git -C "${ROOT_DIR}" diff --cached --quiet 2>/dev/null; then
      git_dirty=1
    fi
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
  local -a render_args=(--out "${RESULTS_MD}")
  local repetition
  for ((repetition = 1; repetition <= REPETITIONS; repetition++)); do
    local repetition_dir="${RESULTS_DIR}/run-$(printf '%02d' "${repetition}")"
    render_args+=(
      --pg-json "${repetition_dir}/pg.json"
      --async-json "${repetition_dir}/async.json"
    )
  done
  echo "────────────────────────────────────────────────────────────"
  echo "rendering ${RESULTS_MD} (also printed below)"
  echo "────────────────────────────────────────────────────────────"
  python3 "${ROOT_DIR}/scripts/render-storage-comparison-results.py" "${render_args[@]}"
}

counterbalanced_order() {
  case "$(( (CURRENT_REPETITION - 1) % 2 ))" in
    0) echo "pg async" ;;
    1) echo "async pg" ;;
  esac
}

if [[ "${RENDER_ONLY}" == "1" ]]; then
  render_results
  exit 0
fi

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
  for ((CURRENT_REPETITION = 1; CURRENT_REPETITION <= REPETITIONS; CURRENT_REPETITION++)); do
    for side_name in $(counterbalanced_order); do
      run_isolated_side "${side_name}"
    done
  done
else
  for ((CURRENT_REPETITION = 1; CURRENT_REPETITION <= REPETITIONS; CURRENT_REPETITION++)); do
    run_isolated_side "${SIDE}"
  done
fi

if [[ "${UPDATE_RESULTS}" == "1" ]]; then
  render_results
fi

echo "storage comparison passed"
