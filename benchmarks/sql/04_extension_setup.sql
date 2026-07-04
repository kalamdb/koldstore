CREATE EXTENSION IF NOT EXISTS koldstore;

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
  'created_at'
);

-- Note on current cold-storage behavior:
--   flush_table writes a parquet segment (containing _seq values) and
--   manifest.json to cold storage, and records the segment in the koldstore
--   catalog.  The hot heap is NOT pruned automatically; rows remain in the
--   heap after flush.
--
--   The benchmark runs all 5 query types (hot-range, wide-range, cold-range,
--   cold-miss) in every mode.  The TPS difference across modes reflects the
--   overhead of the extension's catalog checks, not parquet-served reads.
--
--   When the extension gains heap pruning or SQL cold-read APIs, remove this
--   note and update benchmarks/scripts/run.sh accordingly.
