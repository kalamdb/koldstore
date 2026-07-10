#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${KOLDSTORE_STORAGE_PGVERSION:-${KOLDSTORE_E2E_PGVERSION:-16}}"
PREPARE_ONLY="${KOLDSTORE_STORAGE_PREPARE_ONLY:-0}"
ROWS="${KOLDSTORE_STORAGE_ROWS:-100000}"
HOT_LIMIT="${KOLDSTORE_STORAGE_HOT_LIMIT:-10000}"

usage() {
  cat <<'EOF'
Run the PostgreSQL vs KoldStore storage comparison harness (tests/storage/).

Usage:
  scripts/run-storage-comparison.sh [options]

Options:
  --rows N          Total rows seeded per table (default: 100000)
  --hot-limit N     Rows kept hot after flush (default: 10000)
  --pg-version N    PostgreSQL major version (default: 16)
  --prepare-only    Prepare pgrx + extension only, skip the test
  -h, --help        Show this help text

Environment overrides (used when the matching flag is omitted):
  KOLDSTORE_STORAGE_ROWS
  KOLDSTORE_STORAGE_HOT_LIMIT
  KOLDSTORE_STORAGE_PGVERSION / KOLDSTORE_E2E_PGVERSION
  KOLDSTORE_STORAGE_PREPARE_ONLY

The harness prints a markdown comparison table. Use the `release-pg` extension
profile for fair hot+cold timings (debug builds are ~3–7× slower; plain
`--release` uses `panic=abort` and breaks PostgreSQL ereport/longjmp).

Examples:
  scripts/run-storage-comparison.sh
  scripts/run-storage-comparison.sh --rows 1000000
  scripts/run-storage-comparison.sh --rows 1000000 --hot-limit 50000
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

echo "preparing pgrx PostgreSQL ${PG_VERSION} for storage comparison (release extension)"
KOLDSTORE_E2E_PGVERSION="${PG_VERSION}" \
  KOLDSTORE_E2E_PREPARE_ONLY=1 \
  KOLDSTORE_PGRX_INSTALL_RELEASE=1 \
  scripts/run-pg-e2e.sh "${PG_VERSION}"

if [[ "${PREPARE_ONLY}" == "1" || "${PREPARE_ONLY}" == "true" ]]; then
  echo "storage comparison database is ready (prepare-only; skipping test)"
  exit 0
fi

echo "running storage comparison (rows=${ROWS}, hot_limit=${HOT_LIMIT})"
KOLDSTORE_STORAGE_ROWS="${ROWS}" \
  KOLDSTORE_STORAGE_HOT_LIMIT="${HOT_LIMIT}" \
  cargo test -p storage-comparison --test pg_vs_koldstore -- --nocapture

echo "storage comparison passed"
