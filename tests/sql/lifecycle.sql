-- Manage / describe / flush / unmanage smoke (stable catalog assertions).

CREATE TABLE sqlreg.lifecycle_t (
  id bigint PRIMARY KEY,
  body text NOT NULL
);

INSERT INTO sqlreg.lifecycle_t (id, body) VALUES (1, 'a'), (2, 'b');

SELECT koldstore.manage_table(
  table_name => 'sqlreg.lifecycle_t'::regclass,
  storage => 'sqlreg_fs',
  hot_row_limit => 10,
  min_flush_rows => 1,
  max_rows_per_file => 10,
  migration_order_by => 'id'
);

SELECT count(*)::bigint AS active_schemas
FROM koldstore.schemas
WHERE table_oid = 'sqlreg.lifecycle_t'::regclass::oid AND active;

SELECT (koldstore.describe_table('sqlreg.lifecycle_t'::regclass)
         ? 'storage_binding') AS has_storage_binding;

SELECT koldstore.flush_table('sqlreg.lifecycle_t'::regclass);

SELECT id, body FROM sqlreg.lifecycle_t ORDER BY id;

SELECT koldstore.unmanage_table('sqlreg.lifecycle_t'::regclass);

SELECT count(*)::bigint AS active_after_unmanage
FROM koldstore.schemas
WHERE table_oid = 'sqlreg.lifecycle_t'::regclass::oid AND active;

SELECT id, body FROM sqlreg.lifecycle_t ORDER BY id;
