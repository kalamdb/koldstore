-- Mixed hot+cold SELECT after a partial flush.

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
