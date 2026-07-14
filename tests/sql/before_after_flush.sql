-- before-flush SELECT results must equal after-flush SELECT results.

CREATE TABLE sqlreg.before_after (
  id bigint PRIMARY KEY,
  body text NOT NULL,
  qty integer NOT NULL
);

INSERT INTO sqlreg.before_after (id, body, qty)
SELECT gs, 'row-' || gs::text, (gs % 7)::integer
FROM generate_series(1, 20) AS gs;

SELECT koldstore.manage_table(
  table_name => 'sqlreg.before_after'::regclass,
  storage => 'sqlreg_fs',
  hot_row_limit => 5,
  min_flush_rows => 1,
  max_rows_per_file => 10,
  migration_order_by => 'id'
);

CREATE TEMP TABLE before_flush AS
SELECT id, body, qty FROM sqlreg.before_after ORDER BY id;

SELECT koldstore.flush_table('sqlreg.before_after'::regclass);

CREATE TEMP TABLE after_flush AS
SELECT id, body, qty FROM sqlreg.before_after ORDER BY id;

SELECT 'missing_after' AS diff, b.*
FROM before_flush b
WHERE NOT EXISTS (
  SELECT 1 FROM after_flush a
  WHERE a.id = b.id AND a.body = b.body AND a.qty = b.qty
)
UNION ALL
SELECT 'extra_after' AS diff, a.*
FROM after_flush a
WHERE NOT EXISTS (
  SELECT 1 FROM before_flush b
  WHERE a.id = b.id AND a.body = b.body AND a.qty = b.qty
)
ORDER BY 1, 2;

SELECT count(*) AS row_count FROM sqlreg.before_after;
