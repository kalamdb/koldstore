-- Visible README demo: run with psql -e -f so SQL echoes but \echo / \! stay hidden.
\set ON_ERROR_STOP on
\pset pager off
\pset border 2
\pset linestyle unicode
\timing off

\echo ''
\echo '━━━ 1. Baseline: a normal PostgreSQL table with 1,000,000 rows ━'
\! sleep 0.5

SELECT count(*) AS rows_before_flush
FROM app.messages;

\! sleep 2

\echo ''
\echo '━━━ 2. PostgreSQL disk usage before flush ━━━━━━━━━━━━━━━━━━━━'
\! sleep 0.5

SELECT
  pg_size_pretty(pg_relation_size('app.messages')) AS heap,
  pg_size_pretty(pg_indexes_size('app.messages')) AS indexes,
  pg_size_pretty(pg_total_relation_size('app.messages')) AS total;

\! sleep 2

\echo ''
\echo '━━━ 3. Put the existing table under KoldStore management ━━━━━'
\! sleep 0.5

SELECT koldstore.manage_table(
  table_name         => 'app.messages'::regclass,
  storage            => 'local-dev',
  hot_row_limit      => 100000,
  min_flush_rows     => 1,
  max_rows_per_file  => 250000,
  migration_order_by => 'id'
);

\echo ''
\echo '━━━ 4. Flush the oldest rows to cold Parquet storage (Keep 100k in PostgreSQL) ━━━━━━━━━'
\! sleep 0.5

SELECT koldstore.flush_table(
  table_name => 'app.messages'::regclass
);

\echo ''
\echo '━━━ 5. Hot rows stay in PostgreSQL; older rows move cold ━━━━━'
\! sleep 0.5

SELECT
  (koldstore.describe_table(table_name => 'app.messages'::regclass)->>'hot_rows')::int AS hot_rows,
  (koldstore.describe_table(table_name => 'app.messages'::regclass)->>'cold_row_count')::int AS cold_rows;

\! sleep 4

\echo ''
\echo '━━━ 6. Reclaim heap space and compare disk usage ━━━━━━━━━━━━━'
\! sleep 0.1

VACUUM (FULL) app.messages;

SELECT
  pg_size_pretty(pg_relation_size('app.messages')) AS heap_after,
  round(100.0 * (1 - pg_relation_size('app.messages')::numeric / NULLIF(b.heap_bytes::numeric, 0)),1) || '% smaller' AS heap_saved,
  pg_size_pretty(pg_indexes_size('app.messages')) AS indexes_after,
  round(100.0 * (1 - pg_indexes_size('app.messages')::numeric / NULLIF(b.indexes_bytes::numeric, 0)),1) || '% smaller' AS indexes_saved,
  pg_size_pretty(pg_total_relation_size('app.messages')) AS total_after,
  round(100.0 * (1 - pg_total_relation_size('app.messages')::numeric / NULLIF(b.total_bytes::numeric, 0)),1) || '% smaller' AS total_saved
FROM app.demo_baseline AS b;

\! sleep 6

\echo ''
\echo '━━━ 7. The application still queries the original table (From both Hot & Cold storage) ━━━━━'
\! sleep 0.5

SELECT count(*) AS rows_visible
FROM app.messages;

\! sleep 3

\echo ''
\echo '━━━ 8. Cold reads use KoldMergeScan ━━━━━━━━━━━━━━━━━━━━━━━━━━'
\! sleep 0.5

EXPLAIN (COSTS OFF)
SELECT *
FROM app.messages
WHERE id = 7;

\! sleep 5

\echo ''
\echo '━━━ 9. Cold files written by KoldStore ━━━━━━━━━━━━━━━━━━━━━━━'
\! sleep 0.5

\! du -sh /tmp/koldstore-demo/app/messages
\! echo
\! ls -lh /tmp/koldstore-demo/app/messages/

\! sleep 4

\echo ''
\echo '✓ Same PostgreSQL table. Smaller hot storage. Full history.'
\echo ''

\! sleep 2
