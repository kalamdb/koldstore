#!/usr/bin/env bash
set -euo pipefail

MODE="${1:?usage: collect_db_stats.sh <mode> <output-json>}"
OUTPUT_JSON="${2:?usage: collect_db_stats.sh <mode> <output-json>}"

: "${DATABASE_URL:=postgres}"
: "${BENCH_SCHEMA:=${MODE//-/_}}"
: "${KOLDSTORE_BENCH_STORAGE_PATH:=}"

mkdir -p "$(dirname "$OUTPUT_JSON")"

cold_storage_size_bytes=0
if [[ -n "$KOLDSTORE_BENCH_STORAGE_PATH" && -d "$KOLDSTORE_BENCH_STORAGE_PATH" ]]; then
  cold_storage_size_bytes="$(du -sk "$KOLDSTORE_BENCH_STORAGE_PATH" | awk '{ print $1 * 1024 }')"
fi

PGOPTIONS="-c search_path=${BENCH_SCHEMA},public,koldstore" \
psql "$DATABASE_URL" \
  -v ON_ERROR_STOP=1 \
  -v MODE="$MODE" \
  -v COLD_STORAGE_SIZE_BYTES="$cold_storage_size_bytes" \
  -X -At -o "$OUTPUT_JSON" <<'SQL'
-- Collect only the metrics that are meaningful for cross-mode comparison:
--   heap_size_bytes     = main heap fork size from pg_relation_size
--   table_size_bytes    = full table size from pg_table_size (heap + toast + vm/fsm, no indexes)
--   index_size_bytes    = total index size from pg_indexes_size
--   extension_metadata_size_bytes = koldstore catalog tables
--   cold_storage_size_bytes = parquet files on disk (from shell du above)
-- database_size_bytes is intentionally omitted; it includes WAL, temp tables,
-- pg_catalog, and unrelated schemas, making it misleading for storage comparison.
WITH relation_sizes AS (
  SELECT
    COALESCE(pg_relation_size(to_regclass('bench_events')), 0) AS heap_size_bytes,
    COALESCE(pg_table_size(to_regclass('bench_events')), 0)   AS table_size_bytes,
    COALESCE(pg_indexes_size(to_regclass('bench_events')), 0)  AS index_size_bytes
),
tuple_stats AS (
  SELECT
    COALESCE(n_live_tup, 0) AS live_tuples_estimate,
    COALESCE(n_dead_tup, 0) AS dead_tuples_estimate
  FROM pg_stat_user_tables
  WHERE relid = to_regclass('bench_events')
),
extension_sizes AS (
  SELECT COALESCE(SUM(pg_total_relation_size(format('%I.%I', schemaname, tablename)::regclass)), 0)
         AS extension_metadata_size_bytes
  FROM pg_tables
  WHERE schemaname = 'koldstore'
)
SELECT jsonb_pretty(jsonb_build_object(
  'mode',                          :'MODE',
  'collected_at',                  now(),
  'heap_size_bytes',               relation_sizes.heap_size_bytes,
  'table_size_bytes',              relation_sizes.table_size_bytes,
  'index_size_bytes',              relation_sizes.index_size_bytes,
  'live_tuples_estimate',          COALESCE(tuple_stats.live_tuples_estimate, 0),
  'dead_tuples_estimate',          COALESCE(tuple_stats.dead_tuples_estimate, 0),
  'extension_metadata_size_bytes', extension_sizes.extension_metadata_size_bytes,
  'cold_storage_size_bytes',       :'COLD_STORAGE_SIZE_BYTES'::bigint
))
FROM relation_sizes
CROSS JOIN extension_sizes
LEFT JOIN tuple_stats ON true;
SQL
