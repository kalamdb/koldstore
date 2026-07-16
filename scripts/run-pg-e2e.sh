#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

usage() {
  cat <<'EOF'
Usage: scripts/run-pg-e2e.sh [PG_VERSION] [--mode <strict|async>]

Runs the complete E2E suite with one mirror capture mode. PG_VERSION defaults
to KOLDSTORE_E2E_PGVERSION or 16; mode defaults to
KOLDSTORE_E2E_MIRROR_CAPTURE_MODE or strict.
EOF
}

PG_VERSION="${KOLDSTORE_E2E_PGVERSION:-16}"
MIRROR_CAPTURE_MODE="${KOLDSTORE_E2E_MIRROR_CAPTURE_MODE:-strict}"
pg_version_seen=0
while [[ $# -gt 0 ]]; do
  case "$1" in
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
    -h|--help)
      usage
      exit 0
      ;;
    -* )
      echo "error: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
    *)
      if [[ "$pg_version_seen" -eq 1 ]]; then
        echo "error: unexpected positional argument '$1'" >&2
        usage >&2
        exit 2
      fi
      PG_VERSION="$1"
      pg_version_seen=1
      shift
      ;;
  esac
done

if [[ "$MIRROR_CAPTURE_MODE" != "strict" && "$MIRROR_CAPTURE_MODE" != "async" ]]; then
  echo "error: invalid --mode '$MIRROR_CAPTURE_MODE'; expected strict or async" >&2
  exit 2
fi

PREPARE_ONLY="${KOLDSTORE_E2E_PREPARE_ONLY:-0}"
PG_FEATURE="pg${PG_VERSION}"
PG_PORT="${KOLDSTORE_E2E_PGPORT:-288${PG_VERSION}}"
PG_HOST="${KOLDSTORE_E2E_PGHOST:-127.0.0.1}"
PG_USER="${KOLDSTORE_E2E_PGUSER:-$(whoami)}"
PG_DATABASE="${KOLDSTORE_E2E_PGDATABASE:-koldstore_pgrx_e2e}"
PG_PASSWORD="${KOLDSTORE_E2E_PGPASSWORD:-}"
PG_CONFIG="${PGRX_PG_CONFIG:-$(cargo pgrx info pg-config "$PG_VERSION")}"
PSQL="$(dirname "$PG_CONFIG")/psql"

echo "starting pgrx-managed PostgreSQL ${PG_VERSION}"
cargo pgrx start "$PG_FEATURE"

echo "installing koldstore into pgrx PostgreSQL ${PG_VERSION}"
INSTALL_ARGS=(
  -p pg_koldstore
  --no-default-features
  --features "$PG_FEATURE"
  --pg-config "$PG_CONFIG"
)
if [[ "${KOLDSTORE_PGRX_INSTALL_RELEASE:-}" == "1" || "${KOLDSTORE_PGRX_INSTALL_RELEASE:-}" == "true" ]]; then
  # release-pg: optimized + panic=unwind (plain --release uses panic=abort and
  # aborts on PostgreSQL ereport/longjmp from extension hooks).
  INSTALL_ARGS+=(--profile release-pg)
fi
if [[ "${KOLDSTORE_PGRX_INSTALL_SUDO:-}" == "1" || "${KOLDSTORE_PGRX_INSTALL_SUDO:-}" == "true" ]]; then
  INSTALL_ARGS+=(--sudo)
fi
cargo pgrx install "${INSTALL_ARGS[@]}"

if [[ "$MIRROR_CAPTURE_MODE" == "async" ]]; then
  echo "enabling logical WAL for async mirror tests"
  "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
    -c "ALTER SYSTEM SET wal_level = 'logical';"
fi

echo "restarting pgrx-managed PostgreSQL ${PG_VERSION} to load extension"
cargo pgrx stop "$PG_FEATURE"
cargo pgrx start "$PG_FEATURE"

echo "recreating local E2E database ${PG_DATABASE} on ${PG_HOST}:${PG_PORT}"
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = '${PG_DATABASE}' AND NOT active;"
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -c "DROP DATABASE IF EXISTS ${PG_DATABASE}" \
  -c "CREATE DATABASE ${PG_DATABASE}"
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 \
  -c "CREATE EXTENSION IF NOT EXISTS koldstore;"

server_version="$("$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -tAc "SHOW server_version")"
if [[ ! "${server_version}" =~ ^${PG_VERSION} ]]; then
  echo "error: expected PostgreSQL ${PG_VERSION} on ${PG_HOST}:${PG_PORT}, got '${server_version}'" >&2
  exit 1
fi
echo "verified pgrx PostgreSQL ${PG_VERSION} on ${PG_HOST}:${PG_PORT} (${server_version})"

export KOLDSTORE_E2E_PGVERSION="$PG_VERSION"
export KOLDSTORE_E2E_PGHOST="$PG_HOST"
export KOLDSTORE_E2E_PGPORT="$PG_PORT"
export KOLDSTORE_E2E_PGUSER="$PG_USER"
export KOLDSTORE_E2E_PGPASSWORD="$PG_PASSWORD"
export KOLDSTORE_E2E_PGDATABASE="$PG_DATABASE"
export KOLDSTORE_E2E_MIRROR_CAPTURE_MODE="$MIRROR_CAPTURE_MODE"
export KOLDSTORE_E2E_WAIT_FOR_STARTUP=1

if [[ "${PREPARE_ONLY}" == "1" || "${PREPARE_ONLY}" == "true" ]]; then
  echo "E2E PostgreSQL ${PG_VERSION} is ready (prepare-only; skipping cargo nextest)"
  exit 0
fi

echo "running pg-koldstore E2E tests in ${MIRROR_CAPTURE_MODE} mode against pgrx PostgreSQL ${PG_VERSION} on ${PG_HOST}:${PG_PORT}"
if [[ "${KOLDSTORE_MINIO:-}" == "1" || -n "${KOLDSTORE_MINIO_ENDPOINT:-}" ]]; then
  echo "MinIO-backed E2E enabled (KOLDSTORE_MINIO / KOLDSTORE_MINIO_ENDPOINT)"
else
  echo "MinIO-backed E2E skipped (set KOLDSTORE_MINIO=1 to enable flush_minio)"
fi

# Prefer cargo test on macOS: nextest --list spawns every integration binary and
# routinely stalls for a long time on unsigned debug deps under Gatekeeper.
use_nextest=0
if [[ "${KOLDSTORE_E2E_USE_NEXTEST:-}" == "1" || "${KOLDSTORE_E2E_USE_NEXTEST:-}" == "true" ]]; then
  use_nextest=1
elif [[ "$(uname -s)" != "Darwin" && "${KOLDSTORE_E2E_USE_CARGO_TEST:-}" != "1" ]]; then
  use_nextest=1
fi

if [[ "${use_nextest}" -eq 0 ]]; then
  echo "using cargo test for E2E (set KOLDSTORE_E2E_USE_NEXTEST=1 to force nextest)"
  CARGO_TEST_ARGS=(-p e2e --no-fail-fast -- --test-threads=1)
  if [[ "${KOLDSTORE_E2E_VERBOSE:-}" == "1" || "${KOLDSTORE_E2E_VERBOSE:-}" == "true" ]]; then
    echo "E2E verbose logging enabled (KOLDSTORE_E2E_VERBOSE); showing live test output"
    CARGO_TEST_ARGS+=(--nocapture)
  fi
  cargo test "${CARGO_TEST_ARGS[@]}"
  exit 0
fi

if ! cargo nextest --version >/dev/null 2>&1; then
  echo "error: cargo-nextest is required; install with: cargo install cargo-nextest --locked" >&2
  exit 1
fi

NEXT_ARGS=(-p e2e --test-threads 1)
if [[ "${KOLDSTORE_E2E_VERBOSE:-}" == "1" || "${KOLDSTORE_E2E_VERBOSE:-}" == "true" ]]; then
  echo "E2E verbose logging enabled (KOLDSTORE_E2E_VERBOSE); showing live test output"
  NEXT_ARGS+=(--no-capture)
fi

cargo nextest run "${NEXT_ARGS[@]}"
