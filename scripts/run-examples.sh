#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${KOLDSTORE_EXAMPLE_PGVERSION:-${KOLDSTORE_E2E_PGVERSION:-16}}"
PREPARE_ONLY="${KOLDSTORE_EXAMPLE_PREPARE_ONLY:-0}"
MIRROR_CAPTURE_MODE="${KOLDSTORE_EXAMPLE_MIRROR_CAPTURE_MODE:-${KOLDSTORE_E2E_MIRROR_CAPTURE_MODE:-strict}}"
FILTER=""

usage() {
  cat <<'EOF'
Run KoldStore real-world example scenarios (separate from the default E2E matrix).

Usage:
  scripts/run-examples.sh [scenario] [--mode <strict|async>]

Scenarios:
  chat_history     WhatsApp / Intercom-style chat history
  ai_memory        AI agent session history
  iot_telemetry    IoT fleet telemetry
  audit_events     Fintech immutable audit ledger
  game_events      Multiplayer game event history
  (omit)           Run all scenarios

Options:
  --mode strict|async   Mirror capture mode (default: strict)

Environment:
  KOLDSTORE_EXAMPLE_ROWS=50000      Total rows seeded per scenario
  KOLDSTORE_EXAMPLE_CLIENTS=8       Parallel PostgreSQL clients
  KOLDSTORE_EXAMPLE_SCOPES=50       Tenant/workspace/game scopes
  KOLDSTORE_EXAMPLE_TIMEOUT_SECS=600 Per-scenario wall-clock timeout (default 10m)
  KOLDSTORE_EXAMPLE_PGVERSION=16    PostgreSQL major version
  KOLDSTORE_EXAMPLE_PREPARE_ONLY=1  Prepare database only, skip tests
  KOLDSTORE_EXAMPLE_MIRROR_CAPTURE_MODE / KOLDSTORE_E2E_MIRROR_CAPTURE_MODE

Examples:
  scripts/run-examples.sh
  scripts/run-examples.sh chat_history
  scripts/run-examples.sh --mode async
  scripts/run-examples.sh game_events --mode async
  KOLDSTORE_EXAMPLE_ROWS=200000 scripts/run-examples.sh game_events --mode strict
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help|help)
      usage
      exit 0
      ;;
    --mode)
      if [[ $# -lt 2 ]]; then
        echo "error: --mode requires strict or async" >&2
        usage >&2
        exit 2
      fi
      MIRROR_CAPTURE_MODE="$2"
      shift 2
      ;;
    --mode=*)
      MIRROR_CAPTURE_MODE="${1#*=}"
      shift
      ;;
    chat_history|ai_memory|iot_telemetry|audit_events|game_events)
      if [[ -n "${FILTER}" ]]; then
        echo "error: unexpected extra scenario '${1}' (already selected '${FILTER}')" >&2
        usage >&2
        exit 1
      fi
      FILTER="$1"
      shift
      ;;
    -*)
      echo "error: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
    *)
      echo "unknown scenario: ${1}" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${MIRROR_CAPTURE_MODE}" != "strict" && "${MIRROR_CAPTURE_MODE}" != "async" ]]; then
  echo "error: invalid --mode '${MIRROR_CAPTURE_MODE}'; expected strict or async" >&2
  exit 2
fi

echo "preparing pgrx PostgreSQL ${PG_VERSION} for KoldStore examples (--mode ${MIRROR_CAPTURE_MODE})"
E2E_ENV_FILE="${KOLDSTORE_E2E_ENV_FILE:-$ROOT_DIR/.e2e-env}"
KOLDSTORE_E2E_PGVERSION="${PG_VERSION}" \
  KOLDSTORE_E2E_PREPARE_ONLY=1 \
  KOLDSTORE_E2E_MIRROR_CAPTURE_MODE="${MIRROR_CAPTURE_MODE}" \
  scripts/run-pg-e2e.sh "${PG_VERSION}" --mode "${MIRROR_CAPTURE_MODE}"
# Prepare-only creates worker DBs and writes pool env; source it for nextest.
# shellcheck disable=SC1090
source "${E2E_ENV_FILE}"
# Ensure the example process sees the selected mode even if .e2e-env is stale.
export KOLDSTORE_E2E_MIRROR_CAPTURE_MODE="${MIRROR_CAPTURE_MODE}"

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
  echo "running example scenario: ${FILTER} (--mode ${MIRROR_CAPTURE_MODE})"
  cargo nextest run "${NEXT_ARGS[@]}" --test "${FILTER}"
else
  echo "running all KoldStore example scenarios (--mode ${MIRROR_CAPTURE_MODE})"
  cargo nextest run "${NEXT_ARGS[@]}"
fi

echo "example scenarios passed (--mode ${MIRROR_CAPTURE_MODE})"
