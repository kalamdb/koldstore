SELECT koldstore.manage_table(
  table_name        => 'audit.account_events',
  storage           => 'local-dev',
  hot_row_limit     => 1000000,
  min_flush_rows    => 100000,
  max_rows_per_file => 50000,
  table_type        => 'user',
  scope_column      => 'tenant_id',
  order_column      => 'created_at'
);

-- Safe for immutable audit/event history. Do not use KoldStore v1 for mutable balances.
