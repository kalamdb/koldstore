#!/usr/bin/env bash
# Run deterministic multi-session isolation E2E schedules.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${1:-${KOLDSTORE_E2E_PGVERSION:-16}}"

export KOLDSTORE_E2E_PREPARE_ONLY=1
bash scripts/run-pg-e2e.sh "$PG_VERSION"

if ! cargo nextest --version >/dev/null 2>&1; then
  echo "error: cargo-nextest is required; install with: cargo install cargo-nextest --locked" >&2
  exit 1
fi

echo "running isolation schedules against PostgreSQL ${PG_VERSION}"
cargo nextest run -p e2e --test isolation_schedules --test-threads 1
