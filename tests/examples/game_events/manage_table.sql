SELECT koldstore.manage_table(
  table_name        => 'game.player_events',
  storage           => 'local-dev',
  hot_row_limit     => 5000000,
  min_flush_rows    => 250000,
  max_rows_per_file => 50000,
  table_type        => 'user',
  scope_column      => 'game_id',
  order_column      => 'created_at'
);
