SELECT koldstore.manage_table(
  table_name        => 'iot.telemetry',
  storage           => 'local-dev',
  hot_row_limit     => 2000000,
  min_flush_rows    => 100000,
  max_rows_per_file => 25000,
  table_type        => 'user',
  scope_column      => 'tenant_id',
  migration_order_by => 'ts'
);
