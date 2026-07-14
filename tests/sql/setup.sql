-- Shared fixture for KoldStore SQL regression cases.
-- Invoked once by scripts/run-sql-regression.sh before each case file.
-- Must run in the same psql session as the case (session GUCs do not carry
-- across separate psql invocations).

SET client_min_messages TO WARNING;

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
SET client_min_messages TO NOTICE;
