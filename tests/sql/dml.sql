-- Hot DML: insert / update / delete / reinsert after delete.

CREATE TABLE sqlreg.dml_t (
  id bigint PRIMARY KEY,
  body text NOT NULL,
  qty integer NOT NULL
);

SELECT koldstore.manage_table(
  table_name => 'sqlreg.dml_t'::regclass,
  storage => 'sqlreg_fs',
  hot_row_limit => 100,
  min_flush_rows => 1,
  max_rows_per_file => 50,
  migration_order_by => 'id'
);

INSERT INTO sqlreg.dml_t (id, body, qty) VALUES
  (1, 'one', 1),
  (2, 'two', 2),
  (3, 'three', 3);

UPDATE sqlreg.dml_t SET body = body || '-u', qty = qty + 10 WHERE id = 2;
DELETE FROM sqlreg.dml_t WHERE id = 3;
INSERT INTO sqlreg.dml_t (id, body, qty) VALUES (3, 'reinsert', 9);

SELECT id, body, qty FROM sqlreg.dml_t ORDER BY id;
SELECT count(*)::bigint AS row_count FROM sqlreg.dml_t;
