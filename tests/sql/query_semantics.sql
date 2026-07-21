-- Query semantics: empty/one-row edges, before==after flush, mixed hot+cold SELECT.

-- Empty managed table
CREATE TABLE sqlreg.empty_t (
  id bigint PRIMARY KEY,
  body text NOT NULL
);

SELECT koldstore.manage_table(
  table_name => 'sqlreg.empty_t'::regclass,
  storage => 'sqlreg_fs',
  hot_row_limit => 10,
  min_flush_rows => 1,
  max_rows_per_file => 10,
  migration_order_by => 'id'
);

SELECT count(*) AS empty_count FROM sqlreg.empty_t;
SELECT koldstore.flush_table('sqlreg.empty_t'::regclass);
SELECT count(*) AS empty_after_flush FROM sqlreg.empty_t;

-- Single-row flush/select
CREATE TABLE sqlreg.one_row (
  id bigint PRIMARY KEY,
  body text NOT NULL
);

INSERT INTO sqlreg.one_row VALUES (1, 'only');

SELECT koldstore.manage_table(
  table_name => 'sqlreg.one_row'::regclass,
  storage => 'sqlreg_fs',
  hot_row_limit => 1,
  min_flush_rows => 1,
  max_rows_per_file => 1,
  migration_order_by => 'id'
);

SELECT koldstore.flush_table('sqlreg.one_row'::regclass);
SELECT id, body FROM sqlreg.one_row;

-- Before-flush SELECT must equal after-flush SELECT
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

-- Mixed hot+cold SELECT after a partial flush
CREATE TABLE sqlreg.hot_cold (
  id bigint PRIMARY KEY,
  body text NOT NULL
);

INSERT INTO sqlreg.hot_cold (id, body)
SELECT gs, 'v' || gs::text
FROM generate_series(1, 12) AS gs;

SELECT koldstore.manage_table(
  table_name => 'sqlreg.hot_cold'::regclass,
  storage => 'sqlreg_fs',
  hot_row_limit => 4,
  min_flush_rows => 1,
  max_rows_per_file => 8,
  migration_order_by => 'id'
);

SELECT koldstore.flush_table('sqlreg.hot_cold'::regclass);

INSERT INTO sqlreg.hot_cold (id, body) VALUES (100, 'hot-new');

SELECT id, body FROM sqlreg.hot_cold WHERE id IN (1, 6, 100) ORDER BY id;
SELECT count(*) AS total FROM sqlreg.hot_cold;
SELECT id, body FROM sqlreg.hot_cold ORDER BY id LIMIT 3;
