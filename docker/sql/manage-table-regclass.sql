-- Regression smoke test for direct management of a populated, qualified table.
-- This must be the first managed table in a fresh database so it also exercises
-- logical-capture provisioning before the source relation is opened.

\set ON_ERROR_STOP on

SELECT koldstore.register_storage(
  name         => 'regclass-smoke',
  storage_type => 'filesystem',
  base_path    => '/tmp/koldstore-regclass-smoke',
  credentials  => '{}'::jsonb,
  config       => '{}'::jsonb
);

CREATE SCHEMA app;
CREATE TABLE app.messages (
  id bigint PRIMARY KEY,
  body text NOT NULL
);
INSERT INTO app.messages VALUES (1, 'existing row');

SET search_path = pg_catalog;
DO $invalid$
BEGIN
  PERFORM koldstore.manage_table(
    table_name         => 'app.messages',
    storage            => 'regclass-smoke',
    hot_row_limit      => 1000,
    min_flush_rows     => 1,
    max_rows_per_file  => 500,
    migration_order_by => 'id'
  );
  RAISE EXCEPTION 'invalid max_rows_per_file unexpectedly succeeded';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLERRM NOT LIKE 'migrate table failed: max_rows_per_file must be at least 1000%' THEN
      RAISE;
    END IF;
END
$invalid$;

DO $capture$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_catalog.pg_replication_slots
    WHERE slot_name LIKE 'koldstore_async_%'
  ) THEN
    RAISE EXCEPTION 'invalid management request provisioned a logical slot';
  END IF;
END
$capture$;

SELECT koldstore.manage_table(
  table_name         => 'app.messages',
  storage            => 'regclass-smoke',
  hot_row_limit      => 1000,
  min_flush_rows     => 1,
  max_rows_per_file  => 1000,
  migration_order_by => 'id'
);
RESET search_path;

DO $smoke$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM koldstore.schemas
    WHERE table_oid = 'app.messages'::regclass
      AND active
  ) THEN
    RAISE EXCEPTION 'app.messages was not managed';
  END IF;

  IF (SELECT count(*) FROM koldstore.messages__cl) <> 1 THEN
    RAISE EXCEPTION 'existing row was not initialized in the mirror';
  END IF;
END
$smoke$;
