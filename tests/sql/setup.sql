-- Shared fixture for KoldStore SQL regression cases.
-- Invoked once by scripts/run-sql-regression.sh before each case file.

DROP SCHEMA IF EXISTS sqlreg CASCADE;
CREATE SCHEMA sqlreg;

SELECT koldstore.register_storage(
  'sqlreg_fs',
  'filesystem',
  :'STORAGE_ROOT',
  '{}'::jsonb,
  '{}'::jsonb
);

SET koldstore.min_max_rows_per_file = 1;
