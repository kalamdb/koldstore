Tasks:
- Add cold policy change and syntax change: (When is a v2 not for now, but its on the roadmap):

Example new syntax:
ALTER TABLE messages SET (
  koldstore_enabled = true,
  koldstore_storage = 'cold_s3',

  koldstore_order_column = 'created_at',
  koldstore_move_after = '90 days',
  koldstore_move_when = 'status = ''completed''',

  koldstore_hot_row_limit = 100000,
  koldstore_min_flush_rows = 1000,
  koldstore_max_rows_per_file = 10000
);


- Check the bg worker which does the WAL reading, we can make it also flush cold rows or make this as a shared resource for both of these workers

