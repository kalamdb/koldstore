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
  -h, --help    Show this help text

Environment:
  KOLDSTORE_STORAGE_PGVERSION=16   PostgreSQL major version (default: 16)
  KOLDSTORE_STORAGE_ROWS=100000    Total rows seeded per table (default: 100000)
  KOLDSTORE_STORAGE_HOT_LIMIT=10000 Rows kept hot after flush (default: 10000)
  KOLDSTORE_STORAGE_PREPARE_ONLY=1 Prepare pgrx + extension only, skip the test

The harness prints a markdown comparison table. Use a release extension build
for fair hot+cold timings (debug builds are ~3–7× slower).

Examples:
  scripts/run-storage-comparison.sh
  KOLDSTORE_STORAGE_ROWS=1000000 scripts/run-storage-comparison.sh
  KOLDSTORE_STORAGE_PREPARE_ONLY=1 scripts/run-storage-comparison.sh
EOF
}

case "${1:-}" in
  -h|--help|help)
    usage
    exit 0
    ;;
  "")
    ;;
  *)
    echo "unknown option: $1" >&2
    usage >&2
    exit 1
    ;;
esac

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
