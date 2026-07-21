#!/usr/bin/env bash
# Run pg_amcheck (when available) plus KoldStore catalog integrity queries.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${1:-${KOLDSTORE_E2E_PGVERSION:-16}}"
PG_CONFIG="${PGRX_PG_CONFIG:-$(cargo pgrx info pg-config "$PG_VERSION")}"
PSQL="$(dirname "$PG_CONFIG")/psql"
AMCHECK_BIN="$(dirname "$PG_CONFIG")/pg_amcheck"
E2E_ENV_FILE="${KOLDSTORE_E2E_ENV_FILE:-$ROOT_DIR/.e2e-env}"

if [[ "${KOLDSTORE_INTEGRITY_PREPARE:-1}" == "1" ]]; then
  export KOLDSTORE_E2E_PREPARE_ONLY=1
  bash scripts/run-pg-e2e.sh "$PG_VERSION"
fi

# Prefer env written by prepare-only (worker pool). Fall back to process env.
if [[ -f "$E2E_ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$E2E_ENV_FILE"
fi

PG_PORT="${KOLDSTORE_E2E_PGPORT:-288${PG_VERSION}}"
PG_HOST="${KOLDSTORE_E2E_PGHOST:-127.0.0.1}"
PG_DATABASE_PREFIX="${KOLDSTORE_E2E_PGDATABASE:-koldstore_pgrx_e2e}"
# run-pg-e2e.sh drops the shared prefix DB and creates ${prefix}_wN workers.
if [[ "${KOLDSTORE_E2E_DB_POOL:-0}" == "1" || "${KOLDSTORE_E2E_DB_POOL:-}" == "true" ]]; then
  PG_DATABASE="${PG_DATABASE_PREFIX}_w0"
else
  PG_DATABASE="$PG_DATABASE_PREFIX"
fi

wait_for_postgres() {
  local attempts="${1:-45}"
  local i
  for ((i = 1; i <= attempts; i++)); do
    if "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
      -c "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "error: PostgreSQL at ${PG_HOST}:${PG_PORT} did not become ready" >&2
  return 1
}

run_integrity_sql() {
  "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 <<'SQL'
-- Missing cold segment objects referenced by active catalog rows.
SELECT 'duplicate_active_segments' AS check_name, count(*)::bigint AS bad
FROM (
  SELECT table_oid, scope_key, object_path, count(*) AS c
  FROM koldstore.cold_segments
  WHERE status = 'active'
  GROUP BY 1, 2, 3
  HAVING count(*) > 1
) d;

SELECT 'invalid_running_jobs' AS check_name, count(*)::bigint AS bad
FROM koldstore.jobs
WHERE status = 'running'
  AND updated_at < now() - interval '1 day';

SELECT 'manifest_without_segments' AS check_name, count(*)::bigint AS bad
FROM koldstore.manifest m
WHERE m.sync_state = 'in_sync'
  AND m.segment_count > 0
  AND NOT EXISTS (
    SELECT 1 FROM koldstore.cold_segments cs
    WHERE cs.table_oid = m.table_oid
      AND cs.scope_key IS NOT DISTINCT FROM m.scope_key
      AND cs.status = 'active'
  );

SELECT 'orphan_error_jobs_without_trace' AS check_name, count(*)::bigint AS bad
FROM koldstore.jobs
WHERE status = 'error' AND coalesce(error_trace, '') = '';
SQL
}

echo "waiting for PostgreSQL ${PG_HOST}:${PG_PORT} before integrity checks"
wait_for_postgres 45

echo "running KoldStore integrity SQL against ${PG_HOST}:${PG_PORT}/${PG_DATABASE}"
# Prior SQLsmith steps can abort a backend (cassert) and force crash recovery;
# retry briefly while the postmaster finishes reinit.
integrity_ok=0
for attempt in 1 2 3 4 5; do
  if run_integrity_sql; then
    integrity_ok=1
    break
  fi
  echo "integrity SQL attempt ${attempt} failed; waiting for postmaster recovery"
  wait_for_postgres 45 || true
  sleep 2
done
if [[ "$integrity_ok" -ne 1 ]]; then
  echo "error: integrity SQL failed after retries" >&2
  exit 1
fi

if [[ -x "$AMCHECK_BIN" ]]; then
  echo "ensuring amcheck extension (skip when unavailable, e.g. bare pgrx install)"
  if ! "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 \
    -c "CREATE EXTENSION IF NOT EXISTS amcheck;" >/dev/null 2>&1; then
    echo "amcheck extension not available; skipping heap/index amcheck"
  else
    echo "running pg_amcheck"
    amcheck_out="$("$AMCHECK_BIN" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" 2>&1)" || {
      amcheck_rc=$?
      echo "$amcheck_out"
      if echo "$amcheck_out" | grep -Eqi 'amcheck is not installed|no relations to check'; then
        echo "pg_amcheck skipped (extension/relations unavailable)"
      else
        echo "error: pg_amcheck reported problems (exit ${amcheck_rc})" >&2
        exit 1
      fi
    }
    if [[ -n "${amcheck_out:-}" ]]; then
      echo "$amcheck_out"
    fi
  fi
else
  echo "pg_amcheck not found at ${AMCHECK_BIN}; skipping heap/index amcheck"
fi

echo "integrity checks completed for PostgreSQL ${PG_VERSION}"
