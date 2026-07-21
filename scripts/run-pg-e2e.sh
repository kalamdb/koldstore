#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
# shellcheck source=lib/pgrx-lifecycle.sh
source "${ROOT_DIR}/scripts/lib/pgrx-lifecycle.sh"

usage() {
  cat <<'EOF'
Usage: scripts/run-pg-e2e.sh [PG_VERSION] [--mode <strict|async>]

Runs the complete E2E suite with one mirror capture mode. PG_VERSION defaults
to KOLDSTORE_E2E_PGVERSION or 16; mode defaults to
KOLDSTORE_E2E_MIRROR_CAPTURE_MODE or strict.

Parallelism: KOLDSTORE_E2E_THREADS (default 4) prepares that many worker
databases from a template so each concurrent test gets its own async slot.
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
THREADS="${KOLDSTORE_E2E_THREADS:-4}"
if ! [[ "$THREADS" =~ ^[1-9][0-9]*$ ]]; then
  echo "error: KOLDSTORE_E2E_THREADS must be a positive integer (got '$THREADS')" >&2
  exit 2
fi
# Keep SQL interpolation safe: only plain identifiers are accepted.
if ! [[ "$PG_DATABASE" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
  echo "error: KOLDSTORE_E2E_PGDATABASE must be a plain SQL identifier (got '$PG_DATABASE')" >&2
  exit 2
fi
TEMPLATE_DB="${PG_DATABASE}_template"
# Async needs one applier per worker DB plus headroom for the launcher.
WORKER_PROCESSES=$((THREADS + 8))
if [[ "$WORKER_PROCESSES" -lt 16 ]]; then
  WORKER_PROCESSES=16
fi

echo "starting pgrx-managed PostgreSQL ${PG_VERSION}"
pgrx_force_stop "${PG_VERSION}"
# Always enable logical WAL on every start. Async slots persist in the pgrx data
# directory across mode/side restarts; starting with wal_level < logical makes
# startup FATAL ("slot exists, but wal_level < logical") and looks like a crash.
PGRX_LOGICAL_CONF=(
  --postgresql-conf wal_level=logical
  --postgresql-conf "max_worker_processes=${WORKER_PROCESSES}"
  --postgresql-conf "max_replication_slots=${WORKER_PROCESSES}"
  --postgresql-conf "max_wal_senders=${WORKER_PROCESSES}"
)
# shared_preload requires the .so to exist; apply only after install.
PGRX_PRELOAD_CONF=(
  "${PGRX_LOGICAL_CONF[@]}"
  --postgresql-conf shared_preload_libraries=koldstore
)
pgrx_start_or_dump "${PG_VERSION}" "$PG_FEATURE" "${PGRX_LOGICAL_CONF[@]}"

if [[ "${KOLDSTORE_E2E_SKIP_INSTALL:-}" == "1" || "${KOLDSTORE_E2E_SKIP_INSTALL:-}" == "true" ]]; then
  echo "skipping cargo pgrx install (KOLDSTORE_E2E_SKIP_INSTALL=1; extension already installed)"
else
  echo "installing koldstore into pgrx PostgreSQL ${PG_VERSION}"
  INSTALL_ARGS=(
    -p pg_koldstore
    --no-default-features
    --features "$PG_FEATURE s3"
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
fi

echo "restarting pgrx-managed PostgreSQL ${PG_VERSION} with koldstore shared_preload"
pgrx_force_stop "${PG_VERSION}"
# Merge-scan hooks must exist in every backend. Slot cleanup before DROP DATABASE
# still runs below so the always-on async launcher does not race failpoint tests.
pgrx_start_or_dump "${PG_VERSION}" "$PG_FEATURE" "${PGRX_PRELOAD_CONF[@]}"

wait_for_postgres() {
  local attempts=30
  local delay=1
  local attempt
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    if "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 -c "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$delay"
  done
  echo "error: PostgreSQL ${PG_VERSION} on ${PG_HOST}:${PG_PORT} did not become ready" >&2
  return 1
}

wait_for_postgres

echo "recreating E2E template + ${THREADS} worker databases (prefix=${PG_DATABASE})"
# Drop worker DBs and any leftover shared/template DBs from prior runs.
# Async appliers may still hold logical slots after a restart; terminate them
# and drop slots before DROP DATABASE (active slots block DROP).
release_db_slots_and_backends() {
  local db="$1"
  local attempt
  # Dropping a database fails while any logical slot for it is *active*.
  # Terminate holders (appliers / walsenders), wait until inactive, then drop.
  for ((attempt = 1; attempt <= 20; attempt++)); do
    "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=0 \
      -c "SELECT pg_terminate_backend(pid)
          FROM pg_stat_activity
          WHERE datname = '${db}' AND pid <> pg_backend_pid();" \
      >/dev/null || true
    "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=0 \
      -c "SELECT pg_terminate_backend(pid)
          FROM pg_stat_activity
          WHERE backend_type LIKE 'koldstore%' AND pid <> pg_backend_pid();" \
      >/dev/null || true
    "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=0 \
      -c "SELECT pg_terminate_backend(active_pid)
          FROM pg_replication_slots
          WHERE (database = '${db}' OR slot_name LIKE 'koldstore_async_%')
            AND active_pid IS NOT NULL;" \
      >/dev/null || true
    # Wait until no slot for this DB is active, then drop them.
    if "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 -tAc \
      "SELECT NOT EXISTS (
         SELECT 1 FROM pg_replication_slots
         WHERE database = '${db}' AND active
       )" 2>/dev/null | grep -qx t; then
      "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=0 \
        -c "SELECT pg_drop_replication_slot(slot_name)
            FROM pg_replication_slots
            WHERE database = '${db}';" \
        >/dev/null || true
      return 0
    fi
    sleep 0.15
  done
  # Best-effort drop even if still marked active (next DROP may still fail).
  "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=0 \
    -c "SELECT pg_drop_replication_slot(slot_name)
        FROM pg_replication_slots
        WHERE database = '${db}' AND NOT active;" \
    >/dev/null || true
}

drop_database_retry() {
  local db="$1"
  local attempt
  for ((attempt = 1; attempt <= 15; attempt++)); do
    release_db_slots_and_backends "${db}"
    # WITH (FORCE) clears remaining backends; slots must already be inactive.
    if "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
      -c "DROP DATABASE IF EXISTS \"${db}\" WITH (FORCE)" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.3
  done
  echo "error: could not DROP DATABASE ${db} (slot or backend still active)" >&2
  "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -c \
    "SELECT slot_name, database, active, active_pid FROM pg_replication_slots;" >&2 || true
  "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -c \
    "SELECT pid, backend_type, datname, state FROM pg_stat_activity
     WHERE datname = '${db}' OR backend_type LIKE 'koldstore%';" >&2 || true
  return 1
}

# Drop every leftover worker DB for this prefix (not only 0..THREADS-1), so a
# lower THREADS count cannot leave wN databases from a prior parallel run.
while IFS= read -r leftover; do
  [[ -n "${leftover}" ]] || continue
  drop_database_retry "${leftover}"
done < <("$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 -tAc \
  "SELECT datname FROM pg_database
   WHERE datname = '${PG_DATABASE}'
      OR datname LIKE '${PG_DATABASE}\\_w%'
   ORDER BY datname")
# Template DBs cannot be dropped until IS_TEMPLATE is cleared.
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -c "UPDATE pg_database SET datistemplate = false WHERE datname = '${TEMPLATE_DB}'" \
  >/dev/null || true
drop_database_retry "${TEMPLATE_DB}"

"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -c "CREATE DATABASE \"${TEMPLATE_DB}\""
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$TEMPLATE_DB" -v ON_ERROR_STOP=1 \
  -c "CREATE EXTENSION IF NOT EXISTS koldstore;"
# Template must have no open connections before CREATE DATABASE ... TEMPLATE.
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${TEMPLATE_DB}' AND pid <> pg_backend_pid();" \
  >/dev/null || true
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -c "ALTER DATABASE \"${TEMPLATE_DB}\" WITH IS_TEMPLATE true"

for ((i = 0; i < THREADS; i++)); do
  worker_db="${PG_DATABASE}_w${i}"
  echo "  creating ${worker_db}"
  "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
    -c "CREATE DATABASE \"${worker_db}\" TEMPLATE \"${TEMPLATE_DB}\""
done

server_version="$("$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "${PG_DATABASE}_w0" -tAc "SHOW server_version")"
if [[ ! "${server_version}" =~ ^${PG_VERSION} ]]; then
  echo "error: expected PostgreSQL ${PG_VERSION} on ${PG_HOST}:${PG_PORT}, got '${server_version}'" >&2
  exit 1
fi
echo "verified pgrx PostgreSQL ${PG_VERSION} on ${PG_HOST}:${PG_PORT} (${server_version}); pool size=${THREADS}"

export KOLDSTORE_E2E_PGVERSION="$PG_VERSION"
export KOLDSTORE_E2E_PGHOST="$PG_HOST"
export KOLDSTORE_E2E_PGPORT="$PG_PORT"
export KOLDSTORE_E2E_PGUSER="$PG_USER"
export KOLDSTORE_E2E_PGPASSWORD="$PG_PASSWORD"
export KOLDSTORE_E2E_PGDATABASE="$PG_DATABASE"
export KOLDSTORE_E2E_MIRROR_CAPTURE_MODE="$MIRROR_CAPTURE_MODE"
export KOLDSTORE_E2E_WAIT_FOR_STARTUP=1
export KOLDSTORE_E2E_THREADS="$THREADS"
export KOLDSTORE_E2E_DB_POOL=1

# Persist for prepare-only callers (readiness scripts run nextest in the parent shell).
E2E_ENV_FILE="${KOLDSTORE_E2E_ENV_FILE:-$ROOT_DIR/.e2e-env}"
cat >"$E2E_ENV_FILE" <<EOF
export KOLDSTORE_E2E_PGVERSION='$PG_VERSION'
export KOLDSTORE_E2E_PGHOST='$PG_HOST'
export KOLDSTORE_E2E_PGPORT='$PG_PORT'
export KOLDSTORE_E2E_PGUSER='$PG_USER'
export KOLDSTORE_E2E_PGPASSWORD='$PG_PASSWORD'
export KOLDSTORE_E2E_PGDATABASE='$PG_DATABASE'
export KOLDSTORE_E2E_MIRROR_CAPTURE_MODE='$MIRROR_CAPTURE_MODE'
export KOLDSTORE_E2E_WAIT_FOR_STARTUP=1
export KOLDSTORE_E2E_THREADS='$THREADS'
export KOLDSTORE_E2E_DB_POOL=1
EOF

if [[ "${PREPARE_ONLY}" == "1" || "${PREPARE_ONLY}" == "true" ]]; then
  echo "E2E PostgreSQL ${PG_VERSION} is ready (prepare-only; env written to ${E2E_ENV_FILE})"
  exit 0
fi

echo "running pg-koldstore E2E tests in ${MIRROR_CAPTURE_MODE} mode against pgrx PostgreSQL ${PG_VERSION} on ${PG_HOST}:${PG_PORT} (threads=${THREADS})"
if [[ "${KOLDSTORE_MINIO:-}" == "1" || -n "${KOLDSTORE_MINIO_ENDPOINT:-}" ]]; then
  echo "MinIO-backed E2E enabled (KOLDSTORE_MINIO / KOLDSTORE_MINIO_ENDPOINT)"
else
  echo "MinIO-backed E2E skipped (set KOLDSTORE_MINIO=1 to enable flush_minio)"
fi

if ! cargo nextest --version >/dev/null 2>&1; then
  echo "error: cargo-nextest is required; install with: cargo install cargo-nextest --locked" >&2
  exit 1
fi

NEXT_ARGS=(-p e2e --test-threads "$THREADS")
if [[ "${KOLDSTORE_E2E_VERBOSE:-}" == "1" || "${KOLDSTORE_E2E_VERBOSE:-}" == "true" ]]; then
  echo "E2E verbose logging enabled (KOLDSTORE_E2E_VERBOSE); showing live test output"
  NEXT_ARGS+=(--no-capture)
fi

cargo nextest run "${NEXT_ARGS[@]}"
