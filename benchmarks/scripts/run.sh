#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_PATH="$ROOT_DIR/benchmarks/scripts/run.sh"
SQL_DIR="$ROOT_DIR/benchmarks/sql"
PGBENCH_DIR="$ROOT_DIR/benchmarks/pgbench"
RESULTS_DIR="$ROOT_DIR/benchmarks/results"

psql_in_mode() {
  # Extension modes preload koldstore so planner hooks register before SELECT.
  # Baseline must not preload: DROP EXTENSION removes catalog tables and the
  # hook panics if the library is still injected into the session.
  #
  # Hot-only keeps merge scan OFF: all rows are still in the heap, so queries
  # must use normal Index/Seq Scan. Otherwise KoldMergeScan (cost 0) always
  # wins and the "hot-only" numbers measure merge-scan overhead, not mirror
  # overhead — which previously showed ~99% slower TPS vs baseline.
  local pgoptions="-c search_path=${BENCH_SCHEMA},public,koldstore"
  if [[ "${MODE:-}" != baseline && "${MODE:-}" != "" ]]; then
    pgoptions="-c session_preload_libraries=koldstore ${pgoptions}"
  fi
  if [[ "${MODE:-}" == extension-hot ]]; then
    pgoptions="${pgoptions} -c koldstore.enable_merge_scan=off"
  fi
  PGOPTIONS="$pgoptions" \
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 "$@"
}

# Modes that flush to cold storage before measuring.
flushed_mode() {
  [[ "$MODE" == extension-hot-cold || "$MODE" == extension-cold-only ]]
}

# cold-only is archive/read: DML is N/A; queries still run against cold Parquet.
cold_archive_mode() {
  [[ "$MODE" == extension-cold-only ]]
}

reset_extension_state() {
  echo "[setup] resetting extension-owned state for ${MODE}"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
DROP EXTENSION IF EXISTS koldstore CASCADE;
DROP SCHEMA IF EXISTS koldstore CASCADE;
SQL
  rm -rf "$KOLDSTORE_BENCH_STORAGE_PATH"
  mkdir -p "$KOLDSTORE_BENCH_STORAGE_PATH"
}

setup_mode() {
  reset_extension_state
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -v schema="$BENCH_SCHEMA" <<'SQL'
DROP SCHEMA IF EXISTS :"schema" CASCADE;
CREATE SCHEMA :"schema";
SQL

  if [[ "$MODE" == baseline ]]; then
    psql_in_mode -f "$SQL_DIR/00_schema_baseline.sql"
  else
    psql_in_mode -f "$SQL_DIR/01_schema_extension.sql"
  fi

  psql_in_mode -v BENCH_ROWS="$BENCH_ROWS" -f "$SQL_DIR/02_seed_100k.sql"
  psql_in_mode -f "$SQL_DIR/03_indexes.sql"

  if [[ "$MODE" != baseline ]]; then
    local hot_row_limit=""
    local min_flush_rows="${BENCH_MIN_FLUSH_ROWS:-1}"
    # Floor is koldstore.min_max_rows_per_file (default 1000).
    local max_rows_per_file="${BENCH_MAX_ROWS_PER_FILE:-10000}"
    if (( max_rows_per_file < 1000 )); then
      max_rows_per_file=1000
    fi
    case "$MODE" in
      extension-hot-cold)
        hot_row_limit="${BENCH_HOT_LIMIT}"
        ;;
      extension-cold-only)
        # Keep one hot sentinel row so the planner still prefers KoldMergeScan.
        # A fully empty heap often falls back to Seq Scan and hides cold rows.
        hot_row_limit=1
        ;;
      extension-hot)
        # Managed + mirrored, but no flush in this mode.
        hot_row_limit=""
        ;;
    esac
    psql_in_mode \
      -v KOLDSTORE_BENCH_STORAGE_PATH="$KOLDSTORE_BENCH_STORAGE_PATH" \
      -v KOLDSTORE_BENCH_COMPRESSION="$KOLDSTORE_BENCH_COMPRESSION" \
      -v HOT_ROW_LIMIT="$hot_row_limit" \
      -v MIN_FLUSH_ROWS="$min_flush_rows" \
      -v MAX_ROWS_PER_FILE="$max_rows_per_file" \
      -f "$SQL_DIR/04_extension_setup.sql"
    wait_for_manage_jobs
  fi

  if flushed_mode; then
    flush_and_verify
    verify_post_flush_row_shape
  fi

  compact_hot_table
}

wait_for_manage_jobs() {
  echo "[flush] waiting for manage_table background jobs to finish..."
  local waited=0
  while true; do
    local pending
    pending=$(psql_in_mode -t -A -c \
      "SELECT count(*) FROM koldstore.jobs WHERE table_oid = 'bench_events'::regclass::oid AND status IN ('pending','running');" \
      2>/dev/null | tr -d '[:space:]' || echo "0")
    pending="${pending:-0}"
    if [[ "$pending" == "0" ]]; then
      echo "[flush] all manage_table jobs finished (waited ${waited}s)"
      return
    fi
    if (( waited >= 120 )); then
      echo "ERROR: manage_table jobs did not finish after ${waited}s ($pending still pending/running)" >&2
      exit 1
    fi
    sleep 2
    (( waited += 2 )) || true
    echo "[flush] ${pending} job(s) still running... (${waited}s)"
  done
}

flush_and_verify() {
  echo "[flush] flushing bench_events to cold storage..."
  local job_id flush_rows flush_status flush_error
  if ! job_id=$(psql_in_mode -t -A -c "SELECT koldstore.flush_table('bench_events'::regclass);"); then
    echo "ERROR: flush_table failed" >&2
    exit 1
  fi
  job_id="$(echo "$job_id" | tr -d '[:space:]')"
  if [[ -z "$job_id" ]]; then
    echo "ERROR: flush_table returned no job id" >&2
    exit 1
  fi

  local job_row
  if ! job_row=$(psql_in_mode -t -A -c \
    "SELECT COALESCE(rows_flushed,0)::text || E'\t' || COALESCE(status,'') || E'\t' || COALESCE(error_trace,'') FROM koldstore.jobs WHERE id = '${job_id}'::uuid;"); then
    echo "ERROR: could not read flush job ${job_id}" >&2
    exit 1
  fi
  flush_rows="$(echo "$job_row" | awk -F'\t' '{print $1}' | tr -d '[:space:]')"
  flush_status="$(echo "$job_row" | awk -F'\t' '{print $2}')"
  flush_error="$(echo "$job_row" | awk -F'\t' '{print $3}')"
  echo "[flush] job=${job_id} rows_flushed=${flush_rows:-0} status=${flush_status:-unknown}"

  if [[ -z "$flush_rows" || "$flush_rows" == "0" ]]; then
    echo "ERROR: flush_table flushed 0 rows - cold storage is empty" >&2
    if [[ -n "$flush_error" ]]; then
      echo "  flush error: $flush_error" >&2
    fi
    echo "  Hint: check manage_table backfill, hot_row_limit, mirror rows, and supported column types." >&2
    psql_in_mode -c "SELECT id, status, rows_flushed, left(COALESCE(error_trace,''), 200) FROM koldstore.jobs ORDER BY 1 DESC LIMIT 5;" >&2 || true
    psql_in_mode -c "SELECT count(*) AS mirror_rows FROM koldstore.bench_events__cl;" >&2 || true
    exit 1
  fi

  local manifest_file
  manifest_file=$(
    python3 - "$KOLDSTORE_BENCH_STORAGE_PATH" <<'PY'
import pathlib
import sys

for path in pathlib.Path(sys.argv[1]).rglob("manifest.json"):
    print(path)
    break
PY
  )
  if [[ -z "$manifest_file" ]]; then
    echo "ERROR: flush_table returned $flush_rows rows but no manifest.json found in $KOLDSTORE_BENCH_STORAGE_PATH" >&2
    exit 1
  fi

  local committed
  committed=$(python3 - "$manifest_file" <<'PY'
import json
import sys

try:
    with open(sys.argv[1], encoding="utf-8") as f:
        payload = json.load(f)
    print(len([s for s in payload.get("segments", []) if s.get("status") == "committed"]))
except Exception:
    print(0)
PY
  )
  if [[ "$committed" == "0" ]]; then
    echo "ERROR: manifest.json exists but has no committed segments - flush may have partially failed" >&2
    echo "  manifest: $manifest_file" >&2
    exit 1
  fi

  echo "[flush] VERIFIED: $manifest_file ($committed committed segment(s), $flush_rows rows)"
}

verify_post_flush_row_shape() {
  # flush_table writes Parquet; hot-heap prune is not always applied by the
  # current flush path, so the harness shapes the heap for accurate size and
  # query-mode measurements (merge scan still serves cold rows).
  local hot_before cold_segments keep_n
  hot_before=$(psql_in_mode -t -A <<'SQL' | tail -n 1 | tr -d '[:space:]'
SET koldstore.enable_merge_scan = off;
SELECT count(*) FROM ONLY bench_events;
SQL
  )
  cold_segments=$(psql_in_mode -t -A -c \
    "SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = 'bench_events'::regclass::oid AND status = 'active';" \
    | tr -d '[:space:]')
  hot_before="${hot_before:-0}"
  cold_segments="${cold_segments:-0}"

  if [[ "$cold_segments" == "0" ]]; then
    cold_segments=$(
      python3 -c "import pathlib,sys; root=pathlib.Path(sys.argv[1]); print(sum(1 for p in root.rglob('*.parquet')))" \
        "$KOLDSTORE_BENCH_STORAGE_PATH"
    )
  fi
  cold_segments="${cold_segments:-0}"
  if [[ "$cold_segments" == "0" ]]; then
    echo "ERROR: expected cold segments after flush for ${MODE}, found 0" >&2
    exit 1
  fi

  if [[ "$MODE" == extension-hot-cold ]]; then
    keep_n="${BENCH_HOT_LIMIT}"
    echo "[prune] shaping hot+cold heap to newest ${keep_n} rows (hot_before=${hot_before})"
  else
    keep_n=1
    echo "[prune] shaping cold-only archive heap to 1 sentinel hot row (hot_before=${hot_before})"
  fi

  if (( hot_before > keep_n )); then
    # Disable merge scan so heap deletes see physical hot rows only.
    psql_in_mode <<SQL
BEGIN;
SET LOCAL koldstore.enable_merge_scan = off;
SET LOCAL session_replication_role = replica;
CREATE TEMP TABLE bench_keep_ctids ON COMMIT DROP AS
SELECT ctid AS keep_ctid
FROM ONLY bench_events
ORDER BY created_at DESC, id DESC
LIMIT ${keep_n};
DELETE FROM ONLY bench_events b
WHERE NOT EXISTS (SELECT 1 FROM bench_keep_ctids k WHERE k.keep_ctid = b.ctid);
TRUNCATE TABLE koldstore.bench_events__cl;
ANALYZE bench_events;
COMMIT;
SQL
  else
    echo "[prune] heap already at target size (${hot_before}); truncating mirror only"
    psql_in_mode <<'SQL'
BEGIN;
SET LOCAL koldstore.enable_merge_scan = off;
SET LOCAL session_replication_role = replica;
TRUNCATE TABLE koldstore.bench_events__cl;
ANALYZE bench_events;
COMMIT;
SQL
  fi
  local hot_after visible
  hot_after=$(psql_in_mode -t -A <<'SQL' | tail -n 1 | tr -d '[:space:]'
SET koldstore.enable_merge_scan = off;
SELECT count(*) FROM ONLY bench_events;
SQL
  )
  visible=$(psql_in_mode -t -A <<'SQL' | tail -n 1 | tr -d '[:space:]'
SET koldstore.enable_merge_scan = on;
SELECT count(*) FROM bench_events;
SQL
  )
  hot_after="${hot_after:-0}"
  visible="${visible:-0}"

  if [[ "$MODE" == extension-hot-cold ]]; then
    if [[ "$hot_after" == "0" ]]; then
      echo "ERROR: hot+cold mode expected retained hot rows, found 0" >&2
      exit 1
    fi
    if (( visible <= hot_after )); then
      echo "ERROR: hot+cold visible (${visible}) should exceed hot heap (${hot_after}) when cold segments exist" >&2
      psql_in_mode -c "EXPLAIN SELECT count(*) FROM bench_events;" >&2 || true
      exit 1
    fi
    echo "[flush] hot+cold shape: hot_rows=${hot_after} visible=${visible} cold_segments=${cold_segments}"
  else
    if (( hot_after > 1 )); then
      echo "ERROR: cold-only mode expected at most 1 hot sentinel row, found ${hot_after}" >&2
      exit 1
    fi
    if (( visible <= hot_after )); then
      echo "ERROR: cold-only visible (${visible}) should exceed hot sentinel (${hot_after})" >&2
      psql_in_mode -c "EXPLAIN SELECT count(*) FROM bench_events;" >&2 || true
      exit 1
    fi
    echo "[flush] cold-only shape: hot_rows=${hot_after} visible=${visible} cold_segments=${cold_segments}"
  fi
}

compact_hot_table() {
  if [[ "${KOLDSTORE_BENCH_COMPACT_AFTER_SETUP:-1}" == "0" ]]; then
    echo "[compact] skipped for ${MODE} (KOLDSTORE_BENCH_COMPACT_AFTER_SETUP=0)"
    return
  fi

  echo "[compact] compacting ${MODE}/bench_events before size snapshot and workloads"
  psql_in_mode -c "VACUUM (FULL, ANALYZE) bench_events;"
  psql_in_mode -c "REINDEX TABLE bench_events;"
}

write_skipped_result() {
  local name="$1"
  local script="$2"
  local reason="$3"
  cat >"$RAW_DIR/${name}.json" <<JSON
{
  "mode": "$MODE",
  "benchmark": "$name",
  "script": "$script",
  "status": "skipped",
  "reason": "$reason"
}
JSON
}

write_na_result() {
  local name="$1"
  local script="$2"
  local reason="$3"
  cat >"$RAW_DIR/${name}.json" <<JSON
{
  "mode": "$MODE",
  "benchmark": "$name",
  "script": "$script",
  "status": "n/a",
  "reason": "$reason"
}
JSON
}

write_failed_result() {
  local name="$1"
  local script="$2"
  local reason="$3"
  local out_path="${4:-}"
  local err_path="${5:-}"
  cat >"$RAW_DIR/${name}.json" <<JSON
{
  "mode": "$MODE",
  "benchmark": "$name",
  "script": "$script",
  "status": "failed",
  "reason": "$reason",
  "stdout": "$out_path",
  "stderr": "$err_path"
}
JSON
}

write_plan() {
  local name="$1"
  local plan_path="$PLAN_DIR/${name}.json"
  case "$name" in
    single_hot_query)
      psql_in_mode -X -At -o "$plan_path" <<'SQL'
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT *
FROM bench_events
WHERE user_id = md5('user-42')::uuid
  AND created_at >= now() - interval '7 days'
ORDER BY created_at DESC
LIMIT 50;
SQL
      ;;
    batch_hot_query)
      psql_in_mode -X -At -o "$plan_path" <<'SQL'
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT *
FROM bench_events
WHERE user_id = md5('user-42')::uuid
  AND conversation_id = md5('conversation-2542')::uuid
  AND created_at >= now() - interval '7 days'
ORDER BY created_at DESC
LIMIT 200;
SQL
      ;;
    hot_cold_query)
      psql_in_mode -X -At -o "$plan_path" <<'SQL'
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT *
FROM bench_events
WHERE user_id = md5('user-42')::uuid
  AND created_at >= now() - interval '180 days'
ORDER BY created_at DESC
LIMIT 500;
SQL
      ;;
    cold_only_query)
      psql_in_mode -X -At -o "$plan_path" <<'SQL'
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT *
FROM bench_events
WHERE user_id = md5('user-42')::uuid
  AND created_at < now() - interval '90 days'
ORDER BY created_at DESC
LIMIT 500;
SQL
      ;;
    cold_miss_query)
      psql_in_mode -X -At -o "$plan_path" <<'SQL'
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT *
FROM bench_events
WHERE user_id = md5('user-42')::uuid
  AND created_at < now() - interval '5 years'
ORDER BY created_at DESC
LIMIT 100;
SQL
      ;;
    single_insert)
      psql_in_mode -X -At -o "$plan_path" <<'SQL'
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
INSERT INTO bench_events (tenant_id, user_id, conversation_id, event_type, status, priority, score, amount, is_active, is_deleted, payload, metadata, tags, binary_hash, created_at, updated_at)
VALUES (md5('tenant-1')::uuid, md5('user-42')::uuid, md5('conversation-42')::uuid, 'message_created', 'queued', 1, 1.0, 1.0, true, false, '{}'::jsonb, '{}'::jsonb, 'pgbench', md5('plan'), now(), now());
ROLLBACK;
SQL
      ;;
    batch_insert_*)
      local batch_size="${name##*_}"
      psql_in_mode -X -At -o "$plan_path" -v batch_size="$batch_size" <<'SQL'
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
INSERT INTO bench_events (tenant_id, user_id, conversation_id, event_type, status, priority, score, amount, is_active, is_deleted, payload, metadata, tags, binary_hash, created_at, updated_at)
SELECT md5('tenant-1')::uuid, md5('user-' || s::text)::uuid, md5('conversation-' || s::text)::uuid, 'message_created', 'queued', 1, 1.0, 1.0, true, false, '{}'::jsonb, '{}'::jsonb, 'pgbench', md5('plan-' || s::text), now(), now()
FROM generate_series(1, :batch_size) AS s;
ROLLBACK;
SQL
      ;;
    single_update)
      psql_in_mode -X -At -o "$plan_path" <<'SQL'
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
UPDATE bench_events SET status = 'updated', updated_at = now() WHERE id = 42;
ROLLBACK;
SQL
      ;;
    batch_update)
      psql_in_mode -X -At -o "$plan_path" <<'SQL'
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
UPDATE bench_events
SET status = 'batch_updated', updated_at = now()
WHERE user_id = md5('user-42')::uuid
  AND created_at >= now() - interval '7 days';
ROLLBACK;
SQL
      ;;
    single_delete)
      psql_in_mode -X -At -o "$plan_path" <<'SQL'
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
DELETE FROM bench_events WHERE id = 42;
ROLLBACK;
SQL
      ;;
    batch_delete)
      psql_in_mode -X -At -o "$plan_path" <<'SQL'
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
DELETE FROM bench_events
WHERE user_id = md5('user-42')::uuid
  AND created_at < now() - interval '30 days';
ROLLBACK;
SQL
      ;;
    mixed_20_clients)
      cat >"$plan_path" <<'JSON'
{
  "note": "mixed_20_clients combines SELECT/INSERT/UPDATE/DELETE branches; see individual workload plans for representative branch plans."
}
JSON
      ;;
  esac
}

run_benchmark() {
  local name="$1"
  local script="$2"
  local clients="$3"
  local jobs="$4"
  local seconds="$5"
  local batch_size="${6:-}"
  local is_dml="${7:-0}"
  echo "running ${MODE}/${name}"

  if [[ "$MODE" == extension-cold-only && "$is_dml" == 1 ]]; then
    echo "n/a ${MODE}/${name}: DML not applicable in cold-only archive mode"
    write_na_result "$name" "$script" "DML not applicable in cold-only archive mode"
    return
  fi

  write_plan "$name"
  "$ROOT_DIR/benchmarks/scripts/collect_system_stats.sh" "$MODE" "before-$name" "$RAW_DIR/${name}.system.before.json"

  local temp_script="$RAW_DIR/${name}.sql"
  if [[ -n "$batch_size" ]]; then
    printf "\\set batch_size %s\n" "$batch_size" >"$temp_script"
    printf "\\set max_id %s\n" "$BENCH_ROWS" >>"$temp_script"
    cat "$PGBENCH_DIR/$script" >>"$temp_script"
  else
    {
      printf "\\set max_id %s\n" "$BENCH_ROWS"
      cat "$PGBENCH_DIR/$script"
    } >"$temp_script"
  fi

  local log_prefix="$RAW_DIR/${name}"
  local out_path="$RAW_DIR/${name}.out"
  local err_path="$RAW_DIR/${name}.err"
  local pgoptions="-c search_path=${BENCH_SCHEMA},public,koldstore"
  if [[ "$MODE" != baseline ]]; then
    pgoptions="-c session_preload_libraries=koldstore ${pgoptions}"
  fi
  if [[ "$MODE" == extension-hot ]]; then
    # Measure mirror/trigger overhead on the normal PG plan, not KoldMergeScan.
    pgoptions="${pgoptions} -c koldstore.enable_merge_scan=off"
  fi
  if ! PGOPTIONS="$pgoptions" \
    pgbench \
      -n \
      -M prepared \
      -c "$clients" \
      -j "$jobs" \
      -T "$seconds" \
      -l \
      --log-prefix "$log_prefix" \
      --random-seed 1 \
      -f "$temp_script" \
      "$DATABASE_URL" \
      >"$out_path" \
      2>"$err_path"; then
    "$ROOT_DIR/benchmarks/scripts/collect_system_stats.sh" "$MODE" "after-$name" "$RAW_DIR/${name}.system.after.json"
    write_failed_result "$name" "$script" "pgbench failed; see stderr_path" "$out_path" "$err_path"
    echo "failed ${MODE}/${name}; see $err_path" >&2
    return
  fi

  "$ROOT_DIR/benchmarks/scripts/collect_system_stats.sh" "$MODE" "after-$name" "$RAW_DIR/${name}.system.after.json"

  cat >"$RAW_DIR/${name}.json" <<JSON
{
  "mode": "$MODE",
  "benchmark": "$name",
  "script": "$script",
  "status": "completed",
  "clients": $clients,
  "jobs": $jobs,
  "seconds": $seconds,
  "batch_size": ${batch_size:-null},
  "stdout": "$out_path",
  "stderr": "$err_path",
  "log_prefix": "$log_prefix",
  "plan": "$PLAN_DIR/${name}.json",
  "system_before": "$RAW_DIR/${name}.system.before.json",
  "system_after": "$RAW_DIR/${name}.system.after.json"
}
JSON
}

ensure_postgres_ready() {
  local label="${1:-benchmark}"
  local attempts=0
  while (( attempts < 30 )); do
    if psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -X -c "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    if (( attempts == 0 )); then
      echo "[recover] PostgreSQL not ready before ${label}; waiting/restarting..."
    fi
    if [[ "${KOLDSTORE_BENCH_START_PGRX:-1}" != "0" ]]; then
      cargo pgrx start "pg${KOLDSTORE_BENCH_PGVERSION:-16}" >/dev/null 2>&1 || true
    fi
    sleep 2
    attempts=$((attempts + 1))
  done
  echo "ERROR: PostgreSQL still not ready after ${attempts} attempts before ${label}" >&2
  return 1
}

run_mode() {
  MODE="${1:?usage: run.sh __run-mode <baseline|extension-hot|extension-hot-cold|extension-cold-only>}"

  case "$MODE" in
    baseline) BENCH_SCHEMA="benchmark_baseline" ;;
    extension-hot) BENCH_SCHEMA="benchmark_extension_hot" ;;
    extension-hot-cold) BENCH_SCHEMA="benchmark_extension_hot_cold" ;;
    extension-cold-only) BENCH_SCHEMA="benchmark_extension_cold_only" ;;
    *) echo "unknown benchmark mode: $MODE" >&2; exit 64 ;;
  esac

  : "${DATABASE_URL:=postgres}"
  : "${BENCH_ROWS:=25000}"
  : "${BENCH_HOT_LIMIT:=$(( BENCH_ROWS / 5 ))}"
  : "${BENCH_CLIENTS:=2}"
  : "${BENCH_JOBS:=2}"
  : "${BENCH_SECONDS:=5}"
  : "${BENCH_MIXED_SECONDS:=15}"
  : "${BENCH_MIXED_CLIENTS:=4}"
  : "${BENCH_MIXED_JOBS:=2}"
  : "${KOLDSTORE_BENCH_COMPRESSION:=zstd}"

  if (( BENCH_HOT_LIMIT < 1 )); then
    BENCH_HOT_LIMIT=1
  fi
  if (( BENCH_HOT_LIMIT >= BENCH_ROWS )); then
    BENCH_HOT_LIMIT=$(( BENCH_ROWS > 1 ? BENCH_ROWS - 1 : 1 ))
  fi
  export BENCH_HOT_LIMIT

  RAW_DIR="$RESULTS_DIR/raw/$MODE"
  PLAN_DIR="$RESULTS_DIR/plans/$MODE"
  KOLDSTORE_BENCH_STORAGE_PATH="$RESULTS_DIR/cold-storage/$MODE"

  export BENCH_SCHEMA
  export KOLDSTORE_BENCH_STORAGE_PATH

  mkdir -p "$RAW_DIR" "$PLAN_DIR" "$KOLDSTORE_BENCH_STORAGE_PATH"

  ensure_postgres_ready "$MODE" || return 1
  setup_mode
  "$ROOT_DIR/benchmarks/scripts/collect_db_stats.sh" "$MODE" "$RAW_DIR/db.before.json"

  # Query workloads: meaningful on every mode (baseline/hot heap, hot+cold merge, cold archive).
  run_benchmark "single_hot_query" "query_hot_single.sql" "$BENCH_CLIENTS" "$BENCH_JOBS" "$BENCH_SECONDS" "" 0
  run_benchmark "batch_hot_query" "query_hot_batch.sql" "$BENCH_CLIENTS" "$BENCH_JOBS" "$BENCH_SECONDS" "" 0
  run_benchmark "hot_cold_query" "query_hot_cold_single.sql" "$BENCH_CLIENTS" "$BENCH_JOBS" "$BENCH_SECONDS" "" 0
  run_benchmark "cold_only_query" "query_cold_only_single.sql" "$BENCH_CLIENTS" "$BENCH_JOBS" "$BENCH_SECONDS" "" 0
  run_benchmark "cold_miss_query" "query_cold_miss.sql" "$BENCH_CLIENTS" "$BENCH_JOBS" "$BENCH_SECONDS" "" 0

  if cold_archive_mode; then
    echo "[bench] ${MODE}: skipping DML (archive/read-only tier)"
    for dml_name in single_insert batch_insert_100 batch_insert_500 batch_insert_1000 \
      single_update batch_update single_delete batch_delete mixed_20_clients; do
      case "$dml_name" in
        batch_insert_*) write_na_result "$dml_name" "insert_batch.sql" "DML not applicable in cold-only archive mode" ;;
        single_insert) write_na_result "$dml_name" "insert_single.sql" "DML not applicable in cold-only archive mode" ;;
        single_update) write_na_result "$dml_name" "update_single.sql" "DML not applicable in cold-only archive mode" ;;
        batch_update) write_na_result "$dml_name" "update_batch.sql" "DML not applicable in cold-only archive mode" ;;
        single_delete) write_na_result "$dml_name" "delete_single.sql" "DML not applicable in cold-only archive mode" ;;
        batch_delete) write_na_result "$dml_name" "delete_batch.sql" "DML not applicable in cold-only archive mode" ;;
        mixed_20_clients) write_na_result "$dml_name" "mixed_20_clients.sql" "DML not applicable in cold-only archive mode" ;;
      esac
    done
  else
    run_benchmark "single_insert" "insert_single.sql" "$BENCH_CLIENTS" "$BENCH_JOBS" "$BENCH_SECONDS" "" 1
    run_benchmark "batch_insert_100" "insert_batch.sql" "$BENCH_CLIENTS" "$BENCH_JOBS" "$BENCH_SECONDS" "100" 1
    run_benchmark "batch_insert_500" "insert_batch.sql" "$BENCH_CLIENTS" "$BENCH_JOBS" "$BENCH_SECONDS" "500" 1
    run_benchmark "batch_insert_1000" "insert_batch.sql" "$BENCH_CLIENTS" "$BENCH_JOBS" "$BENCH_SECONDS" "1000" 1
    run_benchmark "single_update" "update_single.sql" "$BENCH_CLIENTS" "$BENCH_JOBS" "$BENCH_SECONDS" "" 1
    run_benchmark "batch_update" "update_batch.sql" "$BENCH_CLIENTS" "$BENCH_JOBS" "$BENCH_SECONDS" "" 1
    run_benchmark "single_delete" "delete_single.sql" "$BENCH_CLIENTS" "$BENCH_JOBS" "$BENCH_SECONDS" "" 1
    run_benchmark "batch_delete" "delete_batch.sql" "$BENCH_CLIENTS" "$BENCH_JOBS" "$BENCH_SECONDS" "" 1
    if [[ "$MODE" != baseline && "${BENCH_SKIP_MIXED:-1}" != "0" ]]; then
      # Concurrent merge-scan + mirror DML currently aborts the backend (Abort trap).
      # Baseline still runs mixed; enable extension mixed with BENCH_SKIP_MIXED=0.
      write_skipped_result "mixed_20_clients" "mixed_20_clients.sql" \
        "skipped on extension modes: concurrent merge-scan abort (set BENCH_SKIP_MIXED=0 to force)"
    else
      run_benchmark "mixed_20_clients" "mixed_20_clients.sql" "$BENCH_MIXED_CLIENTS" "$BENCH_MIXED_JOBS" "$BENCH_MIXED_SECONDS" "" 1
    fi
  fi

  "$ROOT_DIR/benchmarks/scripts/collect_db_stats.sh" "$MODE" "$RAW_DIR/db.after.json"
}

if [[ "${1:-}" == "__run-mode" ]]; then
  shift
  run_mode "$@"
  exit 0
fi

BENCH_PROFILE="full"
if [[ "${1:-}" == "--mini" || "${1:-}" == "mini" ]]; then
  BENCH_PROFILE="mini"
  shift
  export BENCH_ROWS="${BENCH_ROWS:-5000}"
  export BENCH_HOT_LIMIT="${BENCH_HOT_LIMIT:-1000}"
  export BENCH_SECONDS="${BENCH_SECONDS:-1}"
  export BENCH_MIXED_SECONDS="${BENCH_MIXED_SECONDS:-3}"
  export BENCH_CLIENTS="${BENCH_CLIENTS:-2}"
  export BENCH_JOBS="${BENCH_JOBS:-2}"
  export BENCH_MIXED_CLIENTS="${BENCH_MIXED_CLIENTS:-2}"
  export BENCH_MIXED_JOBS="${BENCH_MIXED_JOBS:-2}"
  export KOLDSTORE_BENCH_SKIP_CRITERION="${KOLDSTORE_BENCH_SKIP_CRITERION:-1}"
fi

if [[ $# -gt 0 ]]; then
  cat >&2 <<'EOF'
usage: benchmarks/scripts/run.sh [--mini]

  --mini  run a small, fast harness/debug benchmark and still generate reports
EOF
  exit 64
fi

cd "$ROOT_DIR"

for tool in cargo python3; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "required tool not found on PATH: $tool" >&2
    exit 69
  fi
done

if [[ "${KOLDSTORE_BENCH_START_PGRX:-1}" != "0" ]]; then
  PG_VERSION="${KOLDSTORE_BENCH_PGVERSION:-16}"
  PG_FEATURE="pg${PG_VERSION}"
  PG_HOST="${KOLDSTORE_BENCH_PGHOST:-127.0.0.1}"
  PG_PORT="${KOLDSTORE_BENCH_PGPORT:-288${PG_VERSION}}"
  PG_USER="${KOLDSTORE_BENCH_PGUSER:-$(whoami)}"
  PG_DATABASE="${KOLDSTORE_BENCH_PGDATABASE:-koldstore_pgrx_bench}"
  PG_CONFIG="${PGRX_PG_CONFIG:-$(cargo pgrx info pg-config "$PG_VERSION")}"
  PSQL="$(dirname "$PG_CONFIG")/psql"

  export PATH="$(dirname "$PG_CONFIG"):$PATH"

  echo "starting pgrx-managed PostgreSQL ${PG_VERSION}"
  cargo pgrx start "$PG_FEATURE"

  echo "installing pg_koldstore into pgrx PostgreSQL ${PG_VERSION}"
  INSTALL_ARGS=(
    -p pg_koldstore
    --no-default-features
    --features "$PG_FEATURE"
    --pg-config "$PG_CONFIG"
  )
  # release-pg: optimized + panic=unwind (plain --release aborts on ereport/longjmp).
  if [[ "${KOLDSTORE_PGRX_INSTALL_RELEASE:-1}" == "1" || "${KOLDSTORE_PGRX_INSTALL_RELEASE:-1}" == "true" ]]; then
    INSTALL_ARGS+=(--profile release-pg)
  fi
  if [[ "${KOLDSTORE_PGRX_INSTALL_SUDO:-}" == "1" || "${KOLDSTORE_PGRX_INSTALL_SUDO:-}" == "true" ]]; then
    INSTALL_ARGS+=(--sudo)
  fi
  cargo pgrx install "${INSTALL_ARGS[@]}"

  # Ensure the server is up after install (another harness may have stopped it).
  cargo pgrx start "$PG_FEATURE" >/dev/null

  echo "recreating benchmark database ${PG_DATABASE} on ${PG_HOST}:${PG_PORT}"
  # WITH (FORCE) clears leftover sessions (e.g. pg_cron) that block DROP.
  "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
    -c "DROP DATABASE IF EXISTS ${PG_DATABASE} WITH (FORCE)" \
    -c "CREATE DATABASE ${PG_DATABASE}"

  # pgrx Postgres builds may lack LZ4 toast support; force pglz so merge-scan
  # heap reads of jsonb/text toast don't fail with "invalid compression method id 3".
  "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 \
    -c "ALTER DATABASE ${PG_DATABASE} SET default_toast_compression = 'pglz'"

  export DATABASE_URL="${DATABASE_URL:-host=${PG_HOST} port=${PG_PORT} user=${PG_USER} dbname=${PG_DATABASE}}"
else
  if [[ -z "${DATABASE_URL:-}" ]]; then
    cat >&2 <<'EOF'
DATABASE_URL is required when KOLDSTORE_BENCH_START_PGRX=0.

Example:

  export DATABASE_URL="host=127.0.0.1 port=28816 user=$USER dbname=postgres"
  KOLDSTORE_BENCH_START_PGRX=0 ./benchmarks/scripts/run.sh
EOF
    exit 64
  fi
  PSQL="$(command -v psql)"
fi

if [[ ! -x "$PSQL" ]]; then
  echo "required psql executable not found: $PSQL" >&2
  exit 69
fi

if ! command -v pgbench >/dev/null 2>&1; then
  echo "required tool not found on PATH: pgbench" >&2
  exit 69
fi

echo "checking PostgreSQL connection and koldstore extension"
"$PSQL" "$DATABASE_URL" -v ON_ERROR_STOP=1 -X <<'SQL'
CREATE EXTENSION IF NOT EXISTS koldstore;
SELECT koldstore_version();
SHOW koldstore.enable_merge_scan;
SQL

export BENCH_ROWS="${BENCH_ROWS:-25000}"
if [[ -z "${BENCH_HOT_LIMIT:-}" ]]; then
  BENCH_HOT_LIMIT=$(( BENCH_ROWS / 5 ))
fi
if (( BENCH_HOT_LIMIT < 1 )); then
  BENCH_HOT_LIMIT=1
fi
if (( BENCH_HOT_LIMIT >= BENCH_ROWS )); then
  BENCH_HOT_LIMIT=$(( BENCH_ROWS > 1 ? BENCH_ROWS - 1 : 1 ))
fi
export BENCH_HOT_LIMIT
export BENCH_SECONDS="${BENCH_SECONDS:-5}"
export BENCH_MIXED_SECONDS="${BENCH_MIXED_SECONDS:-15}"
export BENCH_CLIENTS="${BENCH_CLIENTS:-2}"
export BENCH_JOBS="${BENCH_JOBS:-2}"
export BENCH_MIXED_CLIENTS="${BENCH_MIXED_CLIENTS:-4}"
export BENCH_MIXED_JOBS="${BENCH_MIXED_JOBS:-2}"
export KOLDSTORE_BENCH_COMPRESSION="${KOLDSTORE_BENCH_COMPRESSION:-zstd}"
export KOLDSTORE_BENCH_SKIP_CRITERION="${KOLDSTORE_BENCH_SKIP_CRITERION:-1}"

echo "running pgKalam benchmark suite"
echo "BENCH_PROFILE=$BENCH_PROFILE"
echo "DATABASE_URL=$DATABASE_URL"
echo "BENCH_ROWS=$BENCH_ROWS BENCH_HOT_LIMIT=$BENCH_HOT_LIMIT BENCH_SECONDS=$BENCH_SECONDS BENCH_MIXED_SECONDS=$BENCH_MIXED_SECONDS"
echo "BENCH_CLIENTS=$BENCH_CLIENTS BENCH_JOBS=$BENCH_JOBS BENCH_MIXED_CLIENTS=$BENCH_MIXED_CLIENTS BENCH_MIXED_JOBS=$BENCH_MIXED_JOBS"

mkdir -p "$RESULTS_DIR"
if [[ "${KOLDSTORE_BENCH_CLEAN_RESULTS:-1}" != "0" ]]; then
  echo "clearing previous raw benchmark results"
  rm -rf \
    "$RESULTS_DIR/raw" \
    "$RESULTS_DIR/plans" \
    "$RESULTS_DIR/cold-storage" \
    "$RESULTS_DIR/summary.json" \
    "$RESULTS_DIR/report.md" \
    "$RESULTS_DIR/report.html"
fi

overall_status=0

run_step() {
  local label="$1"
  shift
  echo "$label"
  if ! "$@"; then
    echo "warning: ${label} failed; generating report from partial data" >&2
    overall_status=1
  fi
}

if [[ "${KOLDSTORE_BENCH_SKIP_CRITERION:-0}" == "1" ]]; then
  echo "skipping Criterion extension-operation benchmarks"
else
  run_step "running Criterion extension-operation benchmarks" cargo bench -p pg-koldstore-benchmarks
fi
run_step "running pgbench baseline" "$SCRIPT_PATH" __run-mode baseline
run_step "running pgbench extension hot-only" "$SCRIPT_PATH" __run-mode extension-hot
if [[ "${BENCH_SKIP_FLUSHED_MODES:-0}" != "1" ]]; then
  run_step "running pgbench extension hot+cold" "$SCRIPT_PATH" __run-mode extension-hot-cold
  run_step "running pgbench extension cold-only" "$SCRIPT_PATH" __run-mode extension-cold-only
else
  echo "skipping flushed modes (BENCH_SKIP_FLUSHED_MODES=1)"
fi

echo "generating benchmark reports"
if ! "$ROOT_DIR/benchmarks/scripts/generate_report.py" --results-dir "$RESULTS_DIR"; then
  echo "error: report generation failed" >&2
  exit 1
fi

echo "benchmark reports written under $RESULTS_DIR"
echo "latest HTML report: $RESULTS_DIR/report.html"
timestamped_html="$(
  python3 - "$RESULTS_DIR/summary.json" <<'PY'
import json
import sys
from pathlib import Path

summary_path = Path(sys.argv[1])
if summary_path.exists():
    report = json.loads(summary_path.read_text(encoding="utf-8")).get("html_report")
    if report:
        print(summary_path.parent / report)
PY
)"
if [[ -n "$timestamped_html" ]]; then
  echo "timestamped HTML report: $timestamped_html"
fi

exit "$overall_status"
