-- Shared HISTORY surrogate PK so all compare arms can probe the same key.
-- Safe to re-run; does not manage the table.
\set ON_ERROR_STOP on

ALTER TABLE public.history
  ADD COLUMN IF NOT EXISTS ks_id bigserial;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.history'::regclass AND contype = 'p'
  ) THEN
    ALTER TABLE public.history ADD PRIMARY KEY (ks_id);
  END IF;
END $$;

-- Widen types early so flush works later without a mid-flight rewrite surprise.
ALTER TABLE public.history
  ALTER COLUMN h_amount TYPE double precision USING h_amount::float8;
ALTER TABLE public.history
  ALTER COLUMN h_data TYPE text USING h_data::text;
