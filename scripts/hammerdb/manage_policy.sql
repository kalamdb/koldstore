-- Deep / selective KoldStore manage policy for HammerDB TPROC-C.
-- Variables (psql -v):
--   STORAGE_ROOT  filesystem root for register_storage
--   MANAGE_SET    history | append | broad
--
-- Always manages HISTORY (required). Best-effort manages order_line (append/broad)
-- and orders (broad). Skips are NOTICE'd, not silent.
\set ON_ERROR_STOP on

DO $$
BEGIN
  IF to_regclass('public.history') IS NULL THEN
    RAISE EXCEPTION 'HISTORY table not found after HammerDB build';
  END IF;
END $$;

-- --- HISTORY (required) ----------------------------------------------------
ALTER TABLE public.history
  ADD COLUMN IF NOT EXISTS ks_id bigserial;
ALTER TABLE public.history
  ALTER COLUMN h_amount TYPE double precision USING h_amount::float8;
ALTER TABLE public.history
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
  table_name => 'public.history'::regclass,
  storage => 'hammerdb_fs',
  hot_row_limit => 1000,
  min_flush_rows => 100,
  max_rows_per_file => 5000
);

-- --- Best-effort helpers ---------------------------------------------------
CREATE OR REPLACE FUNCTION pg_temp.try_manage_rel(rel regclass, label text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
  err text;
BEGIN
  IF rel IS NULL THEN
    RAISE NOTICE 'manage skip %: relation missing', label;
    RETURN false;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conrelid = rel AND contype = 'p'
  ) THEN
    RAISE NOTICE 'manage skip %: no primary key', label;
    RETURN false;
  END IF;
  BEGIN
    PERFORM koldstore.manage_table(
      table_name => rel,
      storage => 'hammerdb_fs',
      hot_row_limit => 5000,
      min_flush_rows => 200,
      max_rows_per_file => 10000
    );
    RAISE NOTICE 'managed % (%)', label, rel;
    RETURN true;
  EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS err = MESSAGE_TEXT;
    RAISE NOTICE 'manage skip %: %', label, err;
    RETURN false;
  END;
END;
$$;

DO $$
DECLARE
  manage_set text := lower(trim(:'MANAGE_SET'));
  ok boolean;
BEGIN
  IF manage_set NOT IN ('history', 'append', 'broad') THEN
    RAISE EXCEPTION 'unknown MANAGE_SET=% (history|append|broad)', manage_set;
  END IF;

  IF manage_set IN ('append', 'broad') THEN
    -- Widen common numeric/varchar columns when present so flush can read them.
    IF to_regclass('public.order_line') IS NOT NULL THEN
      BEGIN
        ALTER TABLE public.order_line
          ALTER COLUMN ol_amount TYPE double precision USING ol_amount::float8;
      EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'order_line ol_amount widen skipped: %', SQLERRM;
      END;
      BEGIN
        ALTER TABLE public.order_line
          ALTER COLUMN ol_dist_info TYPE text USING ol_dist_info::text;
      EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'order_line ol_dist_info widen skipped: %', SQLERRM;
      END;
    END IF;
    ok := pg_temp.try_manage_rel(to_regclass('public.order_line'), 'order_line');
    IF NOT ok THEN
      RAISE NOTICE 'append/broad continuing without order_line manage';
    END IF;
  END IF;

  IF manage_set = 'broad' THEN
    IF to_regclass('public.orders') IS NOT NULL THEN
      BEGIN
        -- HammerDB may name the table orders; leave types alone unless needed.
        NULL;
      END;
    END IF;
    ok := pg_temp.try_manage_rel(to_regclass('public.orders'), 'orders');
    IF NOT ok THEN
      RAISE NOTICE 'broad continuing without orders manage';
    END IF;
  END IF;
END $$;

-- Contract: HISTORY must be actively managed.
DO $$
DECLARE
  n int;
BEGIN
  SELECT count(*) INTO n
  FROM koldstore.schemas
  WHERE table_oid = 'public.history'::regclass AND active;
  IF n <> 1 THEN
    RAISE EXCEPTION 'expected public.history actively managed; active=%', n;
  END IF;
END $$;
