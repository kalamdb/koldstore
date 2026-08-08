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
-- SQL cases assert post-flush SELECTs in the same session; Nested/inline runs
-- flush to completion before returning (queue mode would race the executor).
SET koldstore.flush_execution = 'inline';

-- Fail-fast apply lock: retry like e2e `flush_table_job_id` so background
-- Thin wrapper: returns the flush job UUID (null when not due). Retries when
-- mirror apply briefly holding the lock does not flake ON_ERROR_STOP cases.
-- Default force=true so policy no-ops (empty / under hot_row_limit / undersized
-- excess) still return a job UUID for stable expected outputs.
CREATE OR REPLACE FUNCTION sqlreg.flush_table(rel regclass, force boolean DEFAULT true)
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
  attempt integer := 0;
  err_text text;
  result jsonb;
BEGIN
  LOOP
    attempt := attempt + 1;
    BEGIN
      result := koldstore.flush_table(rel, force);
      IF result ? 'error' AND result->>'error' IS NOT NULL AND NOT COALESCE((result->>'ok')::boolean, false) THEN
        RAISE WARNING 'koldstore flush_table: %', result->>'error';
      END IF;
      RETURN NULLIF(result->>'job_id', '')::uuid;
    EXCEPTION
      WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS err_text = MESSAGE_TEXT;
        IF attempt >= 40
           OR (
             position('apply lock' IN err_text) = 0
             AND position('retry shortly' IN err_text) = 0
             AND position('flush unavailable' IN err_text) = 0
             AND position('flush already in progress' IN err_text) = 0
           )
        THEN
          RAISE;
        END IF;
        PERFORM pg_sleep(0.05 * attempt);
    END;
  END LOOP;
END;
$$;

SET client_min_messages TO NOTICE;
