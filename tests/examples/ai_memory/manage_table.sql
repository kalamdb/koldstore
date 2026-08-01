SELECT koldstore.manage_table(
  table_name        => 'ai.session_events',
  storage           => 'local-dev',
  hot_row_limit     => 500000,
  min_flush_rows    => 50000,
  max_rows_per_file => 10000,
  table_type        => 'user',
  scope_column      => 'workspace_id',
  migration_order_by => 'created_at'
);
