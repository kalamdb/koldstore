-- Shared fixture for KoldStore SQL regression cases.
-- Invoked once by scripts/run-sql-regression.sh before each case file.
-- Must run in the same psql session as the case (session GUCs do not carry
-- across separate psql invocations).

SET client_min_messages TO WARNING;

DROP SCHEMA IF EXISTS sqlreg CASCADE;
CREATE SCHEMA sqlreg;

-- Catalog storage rows survive DROP SCHEMA; only register when missing so
-- later cases on the shared regression DB do not hit "already exists".
SELECT koldstore.register_storage(
  'sqlreg_fs',
  'filesystem',
  :'STORAGE_ROOT',
  '{}'::jsonb,
  '{}'::jsonb
)
WHERE NOT EXISTS (
  SELECT 1 FROM koldstore.storage WHERE name = 'sqlreg_fs'
);

SET koldstore.min_max_rows_per_file = 1;
SET client_min_messages TO NOTICE;
