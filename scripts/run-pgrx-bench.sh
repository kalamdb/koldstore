#!/usr/bin/env bash
# Run in-process #[pg_bench] benchmarks for pg_koldstore via cargo pgrx bench.
#
# Unlike tests/e2e (client SQL over a prepared cluster) and benchmarks/
# (pgbench / Criterion), these functions execute inside a Postgres backend with
# the extension loaded. See crates/pg_koldstore/src/pg_benches/.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

usage() {
  cat <<'EOF'
Usage: scripts/run-pgrx-bench.sh [PG_VERSION] [BENCH_NAME] [cargo pgrx bench options...]

Runs `cargo pgrx bench` for pg_koldstore with the project feature/profile
defaults. PG_VERSION defaults to KOLDSTORE_BENCH_PGVERSION or
KOLDSTORE_E2E_PGVERSION or 16.

Examples:
  scripts/run-pgrx-bench.sh
  scripts/run-pgrx-bench.sh 16
  scripts/run-pgrx-bench.sh 16 managed_hot_count_scan
  scripts/run-pgrx-bench.sh 16 --list
  scripts/run-pgrx-bench.sh 16 --group-name before-opt
  scripts/run-pgrx-bench.sh 16 --compare-group before-opt --group-name after-opt
  scripts/run-pgrx-bench.sh 16 --wait 10
  scripts/run-pgrx-bench.sh 16 --json

Environment:
  KOLDSTORE_BENCH_PGVERSION   PostgreSQL major (default: E2E version or 16)
  KOLDSTORE_PGRX_BENCH_DEBUG  Set to 1 to build with --debug instead of release-pg
  KOLDSTORE_PGRX_BENCH_EXTRA  Extra args appended to cargo pgrx bench
EOF
}

PG_VERSION="${KOLDSTORE_BENCH_PGVERSION:-${KOLDSTORE_E2E_PGVERSION:-16}}"
BENCH_NAME=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      EXTRA_ARGS+=("$@")
      break
      ;;
    -*)
      EXTRA_ARGS+=("$1")
      shift
      ;;
    *)
      if [[ "$1" =~ ^[0-9]+$ ]]; then
        PG_VERSION="$1"
      elif [[ -z "$BENCH_NAME" ]]; then
        BENCH_NAME="$1"
      else
        echo "error: unexpected positional argument '$1'" >&2
        usage >&2
        exit 2
      fi
      shift
      ;;
  esac
done

if [[ -n "${KOLDSTORE_PGRX_BENCH_EXTRA:-}" ]]; then
  # shellcheck disable=SC2206
  EXTRA_ARGS+=(${KOLDSTORE_PGRX_BENCH_EXTRA})
fi

PG_FEATURE="pg${PG_VERSION}"
FEATURES="${PG_FEATURE} s3 pg_bench"

ARGS=(
  -p pg_koldstore
  --no-default-features
  --features "${FEATURES}"
  --postgresql-conf wal_level=logical
  --postgresql-conf max_worker_processes=16
  --postgresql-conf shared_preload_libraries=koldstore
)

if [[ "${KOLDSTORE_PGRX_BENCH_DEBUG:-0}" == "1" || "${KOLDSTORE_PGRX_BENCH_DEBUG:-}" == "true" ]]; then
  ARGS+=(--debug)
else
  # release-pg: optimized + panic=unwind (plain --release uses panic=abort).
  ARGS+=(--profile release-pg)
fi

ARGS+=("pg${PG_VERSION}")
if [[ -n "$BENCH_NAME" ]]; then
  ARGS+=("$BENCH_NAME")
fi
if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
  ARGS+=("${EXTRA_ARGS[@]}")
fi

echo "running cargo pgrx bench (PostgreSQL ${PG_VERSION}, features: ${FEATURES})"
exec cargo pgrx bench "${ARGS[@]}"
