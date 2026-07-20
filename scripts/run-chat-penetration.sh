#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${KOLDSTORE_STRESS_PGVERSION:-${KOLDSTORE_E2E_PGVERSION:-16}}"
PREPARE_ONLY="${KOLDSTORE_STRESS_PREPARE_ONLY:-0}"
MIRROR_CAPTURE_MODE="${KOLDSTORE_STRESS_MIRROR_MODE:-${KOLDSTORE_E2E_MIRROR_CAPTURE_MODE:-strict}}"
PACKS="${KOLDSTORE_STRESS_PACKS:-chat}"

usage() {
  cat <<'EOF'
Run the KoldStore chat penetration stress soak (manual / CI).

Usage:
  scripts/run-chat-penetration.sh [--mode <strict|async>] [--packs <list>]

Options:
  --mode strict|async   Mirror capture mode (default: strict; async pack forces async)
  --packs list          Comma-separated packs (default: chat)
                        v1: chat,cold_dml,multi_table,joins,async

Environment (selected):
  KOLDSTORE_STRESS_MINUTES=5
  KOLDSTORE_STRESS_SOAK_SECONDS=…   Overrides minutes when set (local smoke)
  KOLDSTORE_STRESS_CLIENTS=24
  KOLDSTORE_STRESS_HISTORY_CLIENTS=8
  KOLDSTORE_STRESS_PACKS=chat
  KOLDSTORE_STRESS_PAYLOAD_BYTES=2048
  KOLDSTORE_STRESS_BYTEA_BYTES=2048
  KOLDSTORE_STRESS_LATENCY_MULTIPLIER=4
  KOLDSTORE_STRESS_PROGRESS_INTERVAL_SECS=5  Progress log interval during soak
  KOLDSTORE_STRESS_MAX_ROWS_PER_FILE=2000    Parquet rows/file (default 2000)
  KOLDSTORE_STRESS_WRITER_DELAY_MS=1         Writer sleep between ops (default 1)
  KOLDSTORE_STRESS_PGVERSION=16
  KOLDSTORE_STRESS_PREPARE_ONLY=1

Cold storage: <repo>/tmp/chat_penetration/ (cleared before each run)

Examples:
  scripts/run-chat-penetration.sh
  KOLDSTORE_STRESS_SOAK_SECONDS=30 scripts/run-chat-penetration.sh --packs chat,cold_dml
  scripts/run-chat-penetration.sh --packs chat,cold_dml,multi_table,joins --mode async
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
    --packs)
      if [[ $# -lt 2 ]]; then
        echo "error: --packs requires a comma-separated list" >&2
        usage >&2
        exit 2
      fi
      PACKS="$2"
      shift 2
      ;;
    --packs=*)
      PACKS="${1#*=}"
      shift
      ;;
    -*)
      echo "error: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
    *)
      echo "error: unexpected argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
done

# async pack implies async mirror mode for prepare + manage_table.
if [[ ",${PACKS}," == *",async,"* ]]; then
  MIRROR_CAPTURE_MODE="async"
fi

if [[ "${MIRROR_CAPTURE_MODE}" != "strict" && "${MIRROR_CAPTURE_MODE}" != "async" ]]; then
  echo "error: invalid --mode '${MIRROR_CAPTURE_MODE}'; expected strict or async" >&2
  exit 2
fi

export KOLDSTORE_STRESS_PACKS="${PACKS}"
export KOLDSTORE_STRESS_MIRROR_MODE="${MIRROR_CAPTURE_MODE}"
export KOLDSTORE_E2E_MIRROR_CAPTURE_MODE="${MIRROR_CAPTURE_MODE}"

echo "preparing pgrx PostgreSQL ${PG_VERSION} for chat penetration (--mode ${MIRROR_CAPTURE_MODE}, packs=${PACKS})"
E2E_ENV_FILE="${KOLDSTORE_E2E_ENV_FILE:-$ROOT_DIR/.e2e-env}"
KOLDSTORE_E2E_PGVERSION="${PG_VERSION}" \
  KOLDSTORE_E2E_PREPARE_ONLY=1 \
  KOLDSTORE_E2E_MIRROR_CAPTURE_MODE="${MIRROR_CAPTURE_MODE}" \
  scripts/run-pg-e2e.sh "${PG_VERSION}" --mode "${MIRROR_CAPTURE_MODE}"
# shellcheck disable=SC1090
source "${E2E_ENV_FILE}"
export KOLDSTORE_E2E_MIRROR_CAPTURE_MODE="${MIRROR_CAPTURE_MODE}"

if [[ "${PREPARE_ONLY}" == "1" || "${PREPARE_ONLY}" == "true" ]]; then
  echo "stress database is ready (prepare-only; skipping soak)"
  exit 0
fi

if ! cargo nextest --version >/dev/null 2>&1; then
  echo "error: cargo-nextest is required; install with: cargo install cargo-nextest --locked" >&2
  exit 1
fi

echo "running chat penetration soak (packs=${PACKS}, mode=${MIRROR_CAPTURE_MODE})"
cargo nextest run -p stress --test chat_penetration --no-capture

echo "chat penetration passed (packs=${PACKS}, mode=${MIRROR_CAPTURE_MODE})"
