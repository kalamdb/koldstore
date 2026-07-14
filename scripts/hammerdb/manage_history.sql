-- Selective KoldStore manage for HammerDB TPROC-C.
-- Only HISTORY is managed: it is append-heavy. Leave customer/orders/stock hot.
--
-- HammerDB HISTORY has no primary key and uses numeric(p,s)/varchar(n).
-- Add a surrogate PK and widen types to KoldStore-supported catalog forms.

\set ON_ERROR_STOP on

DO $$
BEGIN
  IF to_regclass('public.history') IS NULL AND to_regclass('tpcc.history') IS NULL THEN
    RAISE EXCEPTION 'HISTORY table not found after HammerDB build';
  END IF;
END $$;

ALTER TABLE IF EXISTS public.history
  ADD COLUMN IF NOT EXISTS ks_id bigserial;
-- Prefer float8 over numeric: flush currently fails reading numeric Datums as text.
-- (Also applied in prepare_history_pk.sql for fair multi-arm compares.)
ALTER TABLE IF EXISTS public.history
  ALTER COLUMN h_amount TYPE double precision USING h_amount::float8;
ALTER TABLE IF EXISTS public.history
  ALTER COLUMN h_data TYPE text USING h_data::text;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.history'::regclass AND contype = 'p'
  ) THEN
    ALTER TABLE public.history ADD PRIMARY KEY (ks_id);
  END IF;
END $$;

SELECT koldstore.register_storage(
  'hammerdb_fs',
  'filesystem',
  :'STORAGE_ROOT',
  '{}'::jsonb,
  '{}'::jsonb
);

SELECT koldstore.manage_table(
  table_name => COALESCE(to_regclass('public.history'), to_regclass('tpcc.history')),
  storage => 'hammerdb_fs',
  hot_row_limit => 1000,
  min_flush_rows => 100,
  max_rows_per_file => 5000
);
