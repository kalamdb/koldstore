#!/usr/bin/env bash
# Run postmaster immediate-restart crash recovery (serial; stops the cluster).
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

echo "running postmaster immediate-restart recovery against PostgreSQL ${PG_VERSION}"
export KOLDSTORE_CRASH_POSTMASTER_RESTART=1
# Must be serial: the test stops the shared pgrx postmaster.
cargo nextest run -p e2e -E 'test(crash::postmaster_restart::)' --test-threads 1
