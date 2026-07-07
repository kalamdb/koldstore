CREATE EXTENSION IF NOT EXISTS koldstore WITH SCHEMA public;

SELECT koldstore.register_storage(
  'bench-local',
  'filesystem',
  :'KOLDSTORE_BENCH_STORAGE_PATH',
  '{}'::jsonb,
  '{}'::jsonb
);

SELECT *
FROM koldstore.manage_table(
  table_name     => 'bench_events'::regclass,
  storage        => 'bench-local',
  hot_row_limit  => NULL,
  order_column   => 'created_at',
  compression    => :'KOLDSTORE_BENCH_COMPRESSION'
);

-- Benchmark note:
--   The harness may prune flushed hot rows after manifest verification when it
--   is collecting storage-only snapshots for cold modes. That prune is owned by
--   the benchmark runner, not by this migration step.
