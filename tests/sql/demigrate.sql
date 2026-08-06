-- Unmanage after flush retains visible rows; cold catalog is cleared.

CREATE TABLE sqlreg.demigrate_t (
  id bigint PRIMARY KEY,
  body text NOT NULL
);

INSERT INTO sqlreg.demigrate_t (id, body)
SELECT gs, 'v' || gs::text
FROM generate_series(1, 10) AS gs;

SELECT koldstore.manage_table(
  table_name => 'sqlreg.demigrate_t'::regclass,
  storage => 'sqlreg_fs',
  hot_row_limit => 2,
  min_flush_rows => 1,
  max_rows_per_file => 8,
  migration_order_by => 'id',
  auto_flush => false
);

SELECT sqlreg.flush_table('sqlreg.demigrate_t'::regclass);

SELECT count(*)::bigint AS cold_segments_before
FROM koldstore.cold_segments
WHERE table_oid = 'sqlreg.demigrate_t'::regclass::oid
  AND status = 'active';

SELECT koldstore.unmanage_table(
  'sqlreg.demigrate_t'::regclass,
  true,
  true
);

SELECT count(*)::bigint AS active_schemas_after
FROM koldstore.schemas
WHERE table_oid = 'sqlreg.demigrate_t'::regclass::oid AND active;

SELECT count(*)::bigint AS visible_rows FROM sqlreg.demigrate_t;
SELECT id, body FROM sqlreg.demigrate_t ORDER BY id LIMIT 3;
