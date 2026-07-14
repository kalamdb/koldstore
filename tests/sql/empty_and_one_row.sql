-- Empty managed table and single-row flush/select edge cases.

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
