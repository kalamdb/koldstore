CREATE EXTENSION IF NOT EXISTS koldstore WITH SCHEMA public;

SELECT koldstore.register_storage(
  name         => 'bench-local',
  storage_type => 'filesystem',
  base_path    => :'KOLDSTORE_BENCH_STORAGE_PATH',
  credentials  => '{}'::jsonb,
  config       => '{}'::jsonb
);

-- HOT_ROW_LIMIT empty => NULL (no retention policy; flush_table archives all mirror rows).
-- MIN_FLUSH_ROWS / MAX_ROWS_PER_FILE are required non-null args (defaults apply only when omitted).
SELECT *
FROM koldstore.manage_table(
  table_name        => 'bench_events'::regclass,
  storage           => 'bench-local',
  hot_row_limit     => NULLIF(TRIM(:'HOT_ROW_LIMIT'), '')::bigint,
  min_flush_rows    => TRIM(:'MIN_FLUSH_ROWS')::bigint,
  max_rows_per_file => TRIM(:'MAX_ROWS_PER_FILE')::bigint,
  migration_order_by => 'created_at',
  compression       => :'KOLDSTORE_BENCH_COMPRESSION'
);

-- Benchmark note:
--   hot+cold modes set HOT_ROW_LIMIT and call flush_table so excess rows move to
--   Parquet while the newest hot_row_limit rows stay in the heap.
--   cold-only modes leave HOT_ROW_LIMIT empty so flush_table archives everything.
