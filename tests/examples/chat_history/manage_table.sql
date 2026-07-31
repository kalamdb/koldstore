SELECT koldstore.manage_table(
  table_name        => 'chat.messages',
  storage           => 'local-dev',
  hot_row_limit     => 10000,
  min_flush_rows    => 1000,
  max_rows_per_file => 500,
  table_type        => 'user',
  scope_column      => 'tenant_id',
  migration_order_by => 'created_at'
);

-- Example tests scale these limits down while keeping the same semantics.
