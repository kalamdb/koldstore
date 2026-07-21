-- Dual-table fixture for differential SQLsmith compare.
-- Plain baseline + managed twin. Flush state is applied by the runner.

DROP SCHEMA IF EXISTS diff_ks CASCADE;
CREATE SCHEMA diff_ks;

SELECT koldstore.register_storage(
  'diff_fs',
  'filesystem',
  :'STORAGE_ROOT',
  '{}'::jsonb,
  '{}'::jsonb
);

SET koldstore.min_max_rows_per_file = 1;

CREATE TABLE diff_ks.baseline (
  id bigint PRIMARY KEY,
  flag boolean,
  qty integer,
  amount numeric(12, 2),
  label text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE diff_ks.managed (
  id bigint PRIMARY KEY,
  flag boolean,
  qty integer,
  amount numeric(12, 2),
  label text,
  created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO diff_ks.baseline (id, flag, qty, amount, label)
SELECT
  gs,
  (gs % 2 = 0),
  (gs % 100)::integer,
  (gs * 1.25)::numeric(12, 2),
  CASE WHEN gs % 7 = 0 THEN NULL ELSE 'lbl-' || gs::text END
FROM generate_series(1, 120) AS gs;

INSERT INTO diff_ks.managed SELECT * FROM diff_ks.baseline;

SELECT koldstore.manage_table(
  table_name => 'diff_ks.managed'::regclass,
  storage => 'diff_fs',
  hot_row_limit => 30,
  min_flush_rows => 1,
  max_rows_per_file => 40,
  migration_order_by => 'id',
  auto_flush => false
);
