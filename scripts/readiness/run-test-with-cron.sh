#!/usr/bin/env bash
# Manual smoke test: schedule koldstore.flush_table via pg_cron and wait for it to flush.
#
# Not part of the default E2E/CI loop. Run when you want to verify the README
# pg_cron recipe against a local pgrx PostgreSQL.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${KOLDSTORE_E2E_PGVERSION:-16}"
PG_FEATURE="pg${PG_VERSION}"
PG_PORT="${KOLDSTORE_E2E_PGPORT:-288${PG_VERSION}}"
PG_HOST="${KOLDSTORE_E2E_PGHOST:-127.0.0.1}"
PG_USER="${KOLDSTORE_E2E_PGUSER:-$(whoami)}"
PG_DATABASE="${KOLDSTORE_E2E_PGDATABASE:-koldstore_pgrx_e2e}"
PG_PASSWORD="${KOLDSTORE_E2E_PGPASSWORD:-}"
WAIT_SECONDS="${KOLDSTORE_CRON_WAIT_SECONDS:-150}"
PG_CRON_REF="${KOLDSTORE_PG_CRON_REF:-v1.6.5}"
PG_CRON_SRC="${KOLDSTORE_PG_CRON_SRC:-${ROOT_DIR}/target/pg_cron-src}"
SKIP_PREPARE="${KOLDSTORE_CRON_SKIP_PREPARE:-0}"
CRON_JOB_NAME="koldstore-flush-cron-smoke"

usage() {
  cat <<'EOF'
Schedule periodic KoldStore flush with pg_cron and assert it works.

Usage:
  scripts/readiness/run-test-with-cron.sh [options]

Options:
  --pg-version N     PostgreSQL major version (default: 16)
  --wait-seconds N   Max seconds to wait for the cron tick (default: 150)
  --skip-prepare     Reuse the current pgrx DB / extension install
  -h, --help         Show this help text

Environment:
  KOLDSTORE_E2E_PGVERSION / KOLDSTORE_E2E_PGPORT / KOLDSTORE_E2E_PGHOST
  KOLDSTORE_E2E_PGUSER / KOLDSTORE_E2E_PGDATABASE
  KOLDSTORE_CRON_WAIT_SECONDS
  KOLDSTORE_PG_CRON_REF          git ref for citusdata/pg_cron (default: v1.6.5)
  KOLDSTORE_CRON_SKIP_PREPARE=1  same as --skip-prepare

What it does:
  1. Prepares pgrx PostgreSQL + installs koldstore (unless --skip-prepare)
  2. Builds/installs pg_cron into that PostgreSQL when missing
  3. Enables shared_preload_libraries=pg_cron and cron.database_name
  4. Creates a managed table, schedules cron.schedule('* * * * *', flush_table)
  5. Waits for a completed flush job + cold segments + hot prune
  6. Unschedules the cron job

Note: pg_cron's finest schedule is one minute, so this test usually takes 1–2 minutes.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pg-version)
      PG_VERSION="${2:?missing value for --pg-version}"
      PG_FEATURE="pg${PG_VERSION}"
      PG_PORT="${KOLDSTORE_E2E_PGPORT:-288${PG_VERSION}}"
      shift 2
      ;;
    --wait-seconds)
      WAIT_SECONDS="${2:?missing value for --wait-seconds}"
      shift 2
      ;;
    --skip-prepare)
      SKIP_PREPARE=1
      shift
      ;;
    -h|--help|help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

PG_CONFIG="${PGRX_PG_CONFIG:-$(cargo pgrx info pg-config "${PG_VERSION}")}"
PSQL="$(dirname "${PG_CONFIG}")/psql"
LIBDIR="$("${PG_CONFIG}" --pkglibdir)"
SHAREDIR="$("${PG_CONFIG}" --sharedir)"

psql_args=(-h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -v ON_ERROR_STOP=1)
if [[ -n "${PG_PASSWORD}" ]]; then
  export PGPASSWORD="${PG_PASSWORD}"
fi

psql_db() {
  "${PSQL}" "${psql_args[@]}" -d "${PG_DATABASE}" "$@"
}

psql_postgres() {
  "${PSQL}" "${psql_args[@]}" -d postgres "$@"
}

pg_cron_lib_present() {
  [[ -f "${LIBDIR}/pg_cron.so" || -f "${LIBDIR}/pg_cron.dylib" ]]
}

# pgrx-install PGXS is sometimes incomplete (missing Makefile.global). Link the
# matching source-tree makefiles so out-of-tree extensions can build.
ensure_pgxs_makefiles() {
  local pgxs_src version_root
  pgxs_src="${LIBDIR}/pgxs/src"
  version_root="$(cd "${LIBDIR}/../../.." && pwd)"
  mkdir -p "${pgxs_src}"
  if [[ ! -f "${pgxs_src}/Makefile.global" && -f "${version_root}/src/Makefile.global" ]]; then
    echo "repairing incomplete PGXS under ${pgxs_src}"
    ln -sf "${version_root}/src/Makefile.global" "${pgxs_src}/Makefile.global"
    ln -sf "${version_root}/src/Makefile.shlib" "${pgxs_src}/Makefile.shlib"
    if [[ -f "${version_root}/src/Makefile.port" ]]; then
      ln -sf "${version_root}/src/Makefile.port" "${pgxs_src}/Makefile.port"
    fi
  fi
  if [[ ! -f "${pgxs_src}/Makefile.global" ]]; then
    echo "error: PGXS Makefile.global missing at ${pgxs_src}/Makefile.global" >&2
    echo "       pgrx source tree also missing at ${version_root}/src" >&2
    exit 1
  fi
}

ensure_pg_cron_installed() {
  if pg_cron_lib_present && [[ -f "${SHAREDIR}/extension/pg_cron.control" ]]; then
    echo "pg_cron already installed under ${LIBDIR}"
    return 0
  fi

  ensure_pgxs_makefiles

  echo "building pg_cron ${PG_CRON_REF} against ${PG_CONFIG}"
  if [[ ! -d "${PG_CRON_SRC}/.git" ]]; then
    rm -rf "${PG_CRON_SRC}"
    git clone --depth 1 --branch "${PG_CRON_REF}" \
      https://github.com/citusdata/pg_cron.git "${PG_CRON_SRC}"
  else
    git -C "${PG_CRON_SRC}" fetch --depth 1 origin "${PG_CRON_REF}"
    git -C "${PG_CRON_SRC}" checkout -q FETCH_HEAD
  fi

  make -C "${PG_CRON_SRC}" clean >/dev/null 2>&1 || true
  make -C "${PG_CRON_SRC}" PG_CONFIG="${PG_CONFIG}"
  make -C "${PG_CRON_SRC}" PG_CONFIG="${PG_CONFIG}" install

  if ! pg_cron_lib_present; then
    echo "error: pg_cron library missing after install in ${LIBDIR}" >&2
    exit 1
  fi
}

data_directory() {
  psql_postgres -tAc "SHOW data_directory" | tr -d '[:space:]'
}

configure_pg_cron_preload() {
  local data_dir conf preload_line
  data_dir="$(data_directory)"
  conf="${data_dir}/postgresql.conf"
  if [[ ! -f "${conf}" ]]; then
    echo "error: postgresql.conf not found at ${conf}" >&2
    exit 1
  fi

  echo "configuring pg_cron in ${conf}"
  # Drop previous managed lines, then append a clean block.
  local tmp
  tmp="$(mktemp)"
  grep -v -E \
    '^[[:space:]]*#?[[:space:]]*shared_preload_libraries[[:space:]]*=' "${conf}" \
    | grep -v -E '^[[:space:]]*#?[[:space:]]*cron\.database_name[[:space:]]*=' \
    | grep -v -E '^# koldstore pg_cron smoke' \
    > "${tmp}"

  preload_line="shared_preload_libraries = 'pg_cron,koldstore'"
  # Preserve any existing non-empty preload list and ensure both libs are present.
  local current
  current="$(psql_postgres -tAc "SHOW shared_preload_libraries" | tr -d '[:space:]' || true)"
  if [[ -n "${current}" ]]; then
    local merged="${current}"
    if [[ ",${merged}," != *",pg_cron,"* && "${merged}" != "pg_cron" ]]; then
      merged="${merged},pg_cron"
    fi
    if [[ ",${merged}," != *",koldstore,"* && "${merged}" != "koldstore" ]]; then
      merged="${merged},koldstore"
    fi
    preload_line="shared_preload_libraries = '${merged}'"
  fi

  {
    cat "${tmp}"
    echo
    echo "# koldstore pg_cron smoke (managed by scripts/readiness/run-test-with-cron.sh)"
    echo "${preload_line}"
    echo "cron.database_name = '${PG_DATABASE}'"
  } > "${conf}"
  rm -f "${tmp}"

  echo "restarting PostgreSQL ${PG_VERSION} to load pg_cron"
  cargo pgrx stop "${PG_FEATURE}"
  cargo pgrx start "${PG_FEATURE}"

  # Wait until accepting connections again.
  for _ in $(seq 1 60); do
    if psql_postgres -tAc "SELECT 1" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  if ! psql_postgres -tAc "SELECT 1" >/dev/null 2>&1; then
    echo "error: PostgreSQL did not come back after enabling pg_cron" >&2
    exit 1
  fi

  local loaded
  loaded="$(psql_postgres -tAc "SHOW shared_preload_libraries" | tr -d '[:space:]')"
  if [[ ",${loaded}," != *",pg_cron,"* && "${loaded}" != "pg_cron" ]]; then
    echo "error: shared_preload_libraries is '${loaded}', expected pg_cron" >&2
    exit 1
  fi
  if [[ ",${loaded}," != *",koldstore,"* && "${loaded}" != "koldstore" ]]; then
    echo "error: shared_preload_libraries is '${loaded}', expected koldstore for merge-scan" >&2
    exit 1
  fi
}

cleanup_cron_job() {
  psql_db -c "SELECT cron.unschedule(jobid) FROM cron.job WHERE jobname = '${CRON_JOB_NAME}';" \
    >/dev/null 2>&1 || true
}

assert_eq() {
  local label="$1"
  local expected="$2"
  local actual="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "error: ${label}: expected '${expected}', got '${actual}'" >&2
    exit 1
  fi
  echo "ok: ${label} = ${actual}"
}

assert_ge() {
  local label="$1"
  local minimum="$2"
  local actual="$3"
  if ! [[ "${actual}" =~ ^[0-9]+$ ]] || (( actual < minimum )); then
    echo "error: ${label}: expected >= ${minimum}, got '${actual}'" >&2
    exit 1
  fi
  echo "ok: ${label} = ${actual}"
}

if [[ "${SKIP_PREPARE}" != "1" && "${SKIP_PREPARE}" != "true" ]]; then
  echo "preparing pgrx PostgreSQL ${PG_VERSION} + koldstore"
  KOLDSTORE_E2E_PGVERSION="${PG_VERSION}" \
    KOLDSTORE_E2E_PGPORT="${PG_PORT}" \
    KOLDSTORE_E2E_PGHOST="${PG_HOST}" \
    KOLDSTORE_E2E_PGUSER="${PG_USER}" \
    KOLDSTORE_E2E_PGDATABASE="${PG_DATABASE}" \
    KOLDSTORE_E2E_PREPARE_ONLY=1 \
    scripts/run-pg-e2e.sh "${PG_VERSION}"
else
  echo "skipping prepare; expecting koldstore on ${PG_HOST}:${PG_PORT}/${PG_DATABASE}"
  cargo pgrx start "${PG_FEATURE}" >/dev/null
fi

ensure_pg_cron_installed
configure_pg_cron_preload

STORAGE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/koldstore-cron-XXXXXX")"
STORAGE_NAME="cron_smoke_fs_$$"
trap 'cleanup_cron_job; rm -rf "${STORAGE_ROOT}"' EXIT

echo "creating cron flush fixture in ${PG_DATABASE}"
psql_db <<SQL
CREATE EXTENSION IF NOT EXISTS koldstore;
CREATE EXTENSION IF NOT EXISTS pg_cron;

DROP SCHEMA IF EXISTS cron_smoke CASCADE;
DROP TABLE IF EXISTS koldstore.cron_items__cl CASCADE;
CREATE SCHEMA cron_smoke;

SELECT koldstore.register_storage(
  '${STORAGE_NAME}',
  'filesystem',
  '${STORAGE_ROOT}',
  '{}'::jsonb,
  '{}'::jsonb
);

CREATE TABLE cron_smoke.cron_items (
  id bigint PRIMARY KEY,
  title text NOT NULL,
  qty integer NOT NULL
);

INSERT INTO cron_smoke.cron_items (id, title, qty)
SELECT gs, 'item-' || gs, gs
FROM generate_series(1, 25) AS gs;

SELECT koldstore.manage_table(
  table_name     => 'cron_smoke.cron_items'::regclass,
  storage        => '${STORAGE_NAME}',
  hot_row_limit  => 10,
  min_flush_rows => 1,
  migration_order_by => 'id'
);

-- Clear any prior schedule with the same name, then schedule every minute.
SELECT cron.unschedule(jobid) FROM cron.job WHERE jobname = '${CRON_JOB_NAME}';

SELECT cron.schedule(
  '${CRON_JOB_NAME}',
  '* * * * *',
  \$\$SELECT koldstore.flush_table(table_name => 'cron_smoke.cron_items'::regclass)\$\$
);
SQL

echo "scheduled '${CRON_JOB_NAME}' (* * * * *); waiting up to ${WAIT_SECONDS}s for pg_cron to flush"
deadline=$((SECONDS + WAIT_SECONDS))
flushed=0
cold_segments=0
hot_rows=25
while (( SECONDS < deadline )); do
  flushed="$(psql_db -tAc "
    SELECT COALESCE(SUM(rows_flushed), 0)
    FROM koldstore.jobs j
    JOIN pg_class c ON c.oid = j.table_oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'cron_smoke'
      AND c.relname = 'cron_items'
      AND j.job_type = 'flush'
      AND j.status = 'completed'
  " | tr -d '[:space:]')"
  cold_segments="$(psql_db -tAc "
    SELECT count(*)
    FROM koldstore.cold_segments s
    JOIN pg_class c ON c.oid = s.table_oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'cron_smoke'
      AND c.relname = 'cron_items'
      AND s.status = 'active'
  " | tr -d '[:space:]')"
  # ONLY still routes through KoldMergeScan; use describe_table for true heap count.
  hot_rows="$(psql_db -tAc "
    SELECT (koldstore.describe_table(table_name => 'cron_smoke.cron_items'::regclass)::jsonb->>'hot_rows')
  " | tr -d '[:space:]')"

  if [[ "${flushed}" =~ ^[0-9]+$ ]] && (( flushed >= 15 )) \
    && [[ "${cold_segments}" =~ ^[0-9]+$ ]] && (( cold_segments >= 1 )) \
    && [[ "${hot_rows}" =~ ^[0-9]+$ ]] && (( hot_rows <= 10 )); then
    break
  fi
  sleep 2
done

echo "==> final counters: rows_flushed=${flushed} cold_segments=${cold_segments} hot_rows=${hot_rows}"

run_status="$(psql_db -tAc "
  SELECT status
  FROM cron.job_run_details
  WHERE jobid = (SELECT jobid FROM cron.job WHERE jobname = '${CRON_JOB_NAME}' LIMIT 1)
  ORDER BY start_time DESC NULLS LAST
  LIMIT 1
" | tr -d '[:space:]')"
if [[ -n "${run_status}" ]]; then
  echo "ok: latest cron.job_run_details.status = ${run_status}"
fi

assert_ge "completed flush rows" "15" "${flushed}"
assert_ge "active cold segments" "1" "${cold_segments}"
if ! [[ "${hot_rows}" =~ ^[0-9]+$ ]] || (( hot_rows > 10 )); then
  echo "error: expected hot heap <= 10 after cron flush, got ${hot_rows}" >&2
  exit 1
fi
echo "ok: hot rows after cron flush = ${hot_rows}"

desc_hot="$(psql_db -tAc "SELECT (koldstore.describe_table(table_name => 'cron_smoke.cron_items'::regclass)::jsonb->>'hot_rows')" | tr -d '[:space:]')"
cold_rows="$(psql_db -tAc "SELECT (koldstore.describe_table(table_name => 'cron_smoke.cron_items'::regclass)::jsonb->>'cold_row_count')" | tr -d '[:space:]')"
assert_eq "describe_table hot_rows" "10" "${desc_hot}"
assert_eq "describe_table cold_row_count" "15" "${cold_rows}"

# Spot-check that a cold PK is still visible through normal SQL (merge scan).
cold_title="$(psql_db -tAc "SELECT title FROM cron_smoke.cron_items WHERE id = 1" | tr -d '[:space:]')"
assert_eq "cold row visible via SELECT" "item-1" "${cold_title}"

manifest_count="$(psql_db -tAc "
  SELECT count(*)
  FROM koldstore.manifest m
  JOIN pg_class c ON c.oid = m.table_oid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'cron_smoke'
    AND c.relname = 'cron_items'
    AND m.sync_state = 'in_sync'
" | tr -d '[:space:]')"
assert_ge "in-sync manifests" "1" "${manifest_count}"

cleanup_cron_job
trap - EXIT
rm -rf "${STORAGE_ROOT}"

cat <<EOF

pg_cron flush smoke test passed.

Re-run:
  scripts/readiness/run-test-with-cron.sh
  scripts/readiness/run-test-with-cron.sh --pg-version ${PG_VERSION}
  scripts/readiness/run-test-with-cron.sh --skip-prepare
EOF
