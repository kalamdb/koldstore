#!/usr/bin/env bash
# Run flush failpoint crash-recovery E2E tests.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${1:-${KOLDSTORE_E2E_PGVERSION:-16}}"

export KOLDSTORE_E2E_PREPARE_ONLY=1
bash scripts/run-pg-e2e.sh "$PG_VERSION"
# shellcheck disable=SC1091
source "${KOLDSTORE_E2E_ENV_FILE:-$ROOT_DIR/.e2e-env}"

if ! cargo nextest --version >/dev/null 2>&1; then
  echo "error: cargo-nextest is required; install with: cargo install cargo-nextest --locked" >&2
  exit 1
fi

echo "running crash/failpoint recovery against PostgreSQL ${PG_VERSION}"
# Optional: KOLDSTORE_CRASH_FULL_MATRIX=1 or KOLDSTORE_CRASH_FAILPOINTS=a,b,c
cargo nextest run -p e2e -E 'test(crash::)' --test-threads "${KOLDSTORE_E2E_THREADS:-4}"
