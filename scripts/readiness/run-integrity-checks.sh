#!/usr/bin/env bash
# Run pg_amcheck (when available) plus KoldStore catalog integrity queries.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${1:-${KOLDSTORE_E2E_PGVERSION:-16}}"
PG_PORT="${KOLDSTORE_E2E_PGPORT:-288${PG_VERSION}}"
PG_HOST="${KOLDSTORE_E2E_PGHOST:-127.0.0.1}"
PG_DATABASE="${KOLDSTORE_E2E_PGDATABASE:-koldstore_pgrx_e2e}"
PG_CONFIG="${PGRX_PG_CONFIG:-$(cargo pgrx info pg-config "$PG_VERSION")}"
PSQL="$(dirname "$PG_CONFIG")/psql"
AMCHECK_BIN="$(dirname "$PG_CONFIG")/pg_amcheck"

if [[ "${KOLDSTORE_INTEGRITY_PREPARE:-1}" == "1" ]]; then
  export KOLDSTORE_E2E_PREPARE_ONLY=1
  bash scripts/run-pg-e2e.sh "$PG_VERSION"
fi

echo "running KoldStore integrity SQL against ${PG_HOST}:${PG_PORT}/${PG_DATABASE}"
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

if [[ -x "$AMCHECK_BIN" ]]; then
  echo "running pg_amcheck"
  "$AMCHECK_BIN" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" || {
    echo "error: pg_amcheck reported problems" >&2
    exit 1
  }
else
  echo "pg_amcheck not found at ${AMCHECK_BIN}; skipping heap/index amcheck"
fi

echo "integrity checks completed for PostgreSQL ${PG_VERSION}"
