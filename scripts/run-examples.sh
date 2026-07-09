#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${KOLDSTORE_EXAMPLE_PGVERSION:-${KOLDSTORE_E2E_PGVERSION:-16}}"
PREPARE_ONLY="${KOLDSTORE_EXAMPLE_PREPARE_ONLY:-0}"
FILTER="${1:-}"

usage() {
  cat <<'EOF'
Run KoldStore real-world example scenarios (separate from the default E2E matrix).

Usage:
  scripts/run-examples.sh [scenario] [options]

Scenarios:
  chat_history     WhatsApp / Intercom-style chat history
  ai_memory        AI agent session history
  iot_telemetry    IoT fleet telemetry
  audit_events     Fintech immutable audit ledger
  game_events      Multiplayer game event history
  (omit)           Run all scenarios

Environment:
  KOLDSTORE_EXAMPLE_ROWS=50000      Total rows seeded per scenario
  KOLDSTORE_EXAMPLE_CLIENTS=8       Parallel PostgreSQL clients
  KOLDSTORE_EXAMPLE_SCOPES=50       Tenant/workspace/game scopes
  KOLDSTORE_EXAMPLE_TIMEOUT_SECS=600 Per-scenario wall-clock timeout (default 10m)
  KOLDSTORE_EXAMPLE_PGVERSION=16    PostgreSQL major version
  KOLDSTORE_EXAMPLE_PREPARE_ONLY=1  Prepare database only, skip tests

Examples:
  scripts/run-examples.sh
  scripts/run-examples.sh chat_history
  KOLDSTORE_EXAMPLE_ROWS=200000 scripts/run-examples.sh game_events
EOF
}

case "${FILTER}" in
  -h|--help|help)
    usage
    exit 0
    ;;
  chat_history|ai_memory|iot_telemetry|audit_events|game_events|"")
    ;;
  *)
    echo "unknown scenario: ${FILTER}" >&2
    usage >&2
    exit 1
    ;;
esac

echo "preparing pgrx PostgreSQL ${PG_VERSION} for KoldStore examples"
KOLDSTORE_E2E_PGVERSION="${PG_VERSION}" \
  KOLDSTORE_E2E_PREPARE_ONLY=1 \
  scripts/run-pg-e2e.sh "${PG_VERSION}"

if [[ "${PREPARE_ONLY}" == "1" || "${PREPARE_ONLY}" == "true" ]]; then
  echo "examples database is ready (prepare-only; skipping tests)"
  exit 0
fi

if ! cargo nextest --version >/dev/null 2>&1; then
  echo "error: cargo-nextest is required; install with: cargo install cargo-nextest --locked" >&2
  exit 1
fi

# Always stream live progress. nextest's success-output=immediate only dumps
# captured logs after a test finishes; --no-capture is required to see inserts /
# flush paths while a long scenario is still running.
NEXT_ARGS=(-p examples --no-capture)

if [[ -n "${FILTER}" ]]; then
  echo "running example scenario: ${FILTER}"
  cargo nextest run "${NEXT_ARGS[@]}" --test "${FILTER}"
else
  echo "running all KoldStore example scenarios"
  cargo nextest run "${NEXT_ARGS[@]}"
fi

echo "example scenarios passed"
