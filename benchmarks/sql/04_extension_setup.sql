CREATE EXTENSION IF NOT EXISTS koldstore WITH SCHEMA public;

SELECT koldstore.register_storage(
  'bench-local',
  'filesystem',
  :'KOLDSTORE_BENCH_STORAGE_PATH',
  '{}'::jsonb,
  '{}'::jsonb
);

SELECT *
FROM koldstore.migrate_table(
  'bench_events'::regclass,
  'shared',
  'bench-local',
  NULL,
  NULL,
  'created_at',
  :'KOLDSTORE_BENCH_COMPRESSION'
);

-- Benchmark note:
--   The harness may prune flushed hot rows after manifest verification when it
--   is collecting storage-only snapshots for cold modes. That prune is owned by
--   the benchmark runner, not by this migration step.
