-- Managed table joins a plain heap table (stable row counts).

CREATE TABLE sqlreg.join_accounts (
  account_id bigint PRIMARY KEY,
  name text NOT NULL
);

CREATE TABLE sqlreg.join_items (
  id bigint PRIMARY KEY,
  account_id bigint NOT NULL,
  title text NOT NULL
);

INSERT INTO sqlreg.join_accounts (account_id, name) VALUES
  (1, 'a'), (2, 'b'), (3, 'c');

INSERT INTO sqlreg.join_items (id, account_id, title)
SELECT gs, ((gs - 1) % 3) + 1, 't' || gs::text
FROM generate_series(1, 12) AS gs;

SELECT koldstore.manage_table(
  table_name => 'sqlreg.join_items'::regclass,
  storage => 'sqlreg_fs',
  hot_row_limit => 4,
  min_flush_rows => 1,
  max_rows_per_file => 8,
  migration_order_by => 'id',
  auto_flush => false
);

SELECT sqlreg.flush_table('sqlreg.join_items'::regclass);

SELECT count(*)::bigint AS inner_join_count
FROM sqlreg.join_items i
INNER JOIN sqlreg.join_accounts a ON a.account_id = i.account_id;

SELECT count(*)::bigint AS left_join_count
FROM sqlreg.join_items i
LEFT JOIN sqlreg.join_accounts a ON a.account_id = i.account_id;

SELECT i.id, a.name
FROM sqlreg.join_items i
INNER JOIN sqlreg.join_accounts a ON a.account_id = i.account_id
WHERE i.id IN (1, 6, 12)
ORDER BY i.id;
