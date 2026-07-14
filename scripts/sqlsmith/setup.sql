-- SQLsmith target fixture for KoldStore fuzz runs.
-- Creates a managed table with mixed hot+cold data across supported scalar types.

DROP SCHEMA IF EXISTS sqlsmith_ks CASCADE;
CREATE SCHEMA sqlsmith_ks;

SELECT koldstore.register_storage(
  'sqlsmith_fs',
  'filesystem',
  :'STORAGE_ROOT',
  '{}'::jsonb,
  '{}'::jsonb
);

SET koldstore.min_max_rows_per_file = 1;

CREATE TABLE sqlsmith_ks.fuzz_rows (
  id bigint PRIMARY KEY,
  flag boolean,
  qty integer,
  amount numeric(12, 2),
  label text,
  created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO sqlsmith_ks.fuzz_rows (id, flag, qty, amount, label)
SELECT
  gs,
  (gs % 2 = 0),
  (gs % 100)::integer,
  (gs * 1.25)::numeric(12, 2),
  CASE WHEN gs % 7 = 0 THEN NULL ELSE 'lbl-' || gs::text END
FROM generate_series(1, 200) AS gs;

SELECT koldstore.manage_table(
  table_name => 'sqlsmith_ks.fuzz_rows'::regclass,
  storage => 'sqlsmith_fs',
  hot_row_limit => 40,
  min_flush_rows => 1,
  max_rows_per_file => 50
);

SELECT koldstore.flush_table('sqlsmith_ks.fuzz_rows'::regclass);

INSERT INTO sqlsmith_ks.fuzz_rows (id, flag, qty, amount, label)
SELECT
  gs,
  true,
  1,
  0.5,
  'hot-' || gs::text
FROM generate_series(1000, 1020) AS gs;
