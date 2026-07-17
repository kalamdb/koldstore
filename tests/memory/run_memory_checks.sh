#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

ensure_nextest() {
  if ! command -v cargo-nextest >/dev/null 2>&1 && ! cargo nextest --version >/dev/null 2>&1; then
    echo "error: required command not found: cargo-nextest" >&2
    exit 1
  fi
}

ensure_nextest

echo "running memory probe unit tests"
cargo nextest run -p koldstore-memory-tests

if [[ "${KOLDSTORE_MEMORY_SKIP_E2E:-}" == "1" || "${KOLDSTORE_MEMORY_SKIP_E2E:-}" == "true" ]]; then
  echo "skipping deep E2E memory leak gates (KOLDSTORE_MEMORY_SKIP_E2E=1)"
  exit 0
fi

PG_VERSION="${KOLDSTORE_E2E_PGVERSION:-${KOLDSTORE_MEMORY_PG_VERSION:-16}}"
E2E_ENV_FILE="${KOLDSTORE_E2E_ENV_FILE:-$ROOT_DIR/.e2e-env}"
if [[ -x "${ROOT_DIR}/scripts/run-pg-e2e.sh" ]]; then
  echo "preparing pgrx PostgreSQL ${PG_VERSION} for deep memory leak E2E"
  KOLDSTORE_E2E_PREPARE_ONLY=1 scripts/run-pg-e2e.sh "${PG_VERSION}"
  # Prepare-only creates worker DBs (`…_w0`…) and drops the shared name;
  # load the pool env so nextest connects to the right databases.
  # shellcheck disable=SC1090
  source "${E2E_ENV_FILE}"
fi

echo "running deep E2E memory leak gates (flush/DML/merge-scan; MinIO when enabled)"
echo "comparison metrics table is printed at the end of memory_overhead_vs_plain_postgres_*"
# Always show stdout/stderr so the plain-vs-koldstore memory table is visible.
cargo nextest run -p e2e -E 'test(memory_leak::)' --test-threads 1 --no-capture

if command -v valgrind >/dev/null 2>&1; then
  echo "valgrind is available; optional secondary pass: cargo pgrx test --valgrind"
else
  echo "valgrind not found; skipping valgrind pass"
fi

if command -v heaptrack >/dev/null 2>&1; then
  echo "heaptrack is available; optional profiles: see tests/memory/heap_profile.md"
else
  echo "heaptrack not found; skipping heaptrack pass"
fi
