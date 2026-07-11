-- pg-koldstore demo: conversations + messages flushed to MinIO.
--
-- Prerequisites:
--   docker/run.sh                 # starts Postgres (koldstore preloaded) + MinIO
--
-- Run from the host:
--   psql "postgres://postgres:postgres@127.0.0.1:5432/koldstore" -f docker/sql/example.sql
--
-- Or from inside the Postgres container:
--   docker compose -f docker/docker-compose.yml exec -T postgres \
--     psql -U postgres -d koldstore -f - < docker/sql/example.sql
--
-- Notes:
--   * MinIO is reached at http://minio:9000 from inside the compose network.
--   * hot_row_limit => 1000 keeps at most 1000 mirror rows hot before flushing oldest rows by seq.
--   * force => true queues a flush job immediately (scaffold queues jobs today).

\set ON_ERROR_STOP on

\echo '==> extension version'
SELECT koldstore_version();

\echo '==> register MinIO storage (compose service: minio)'
SELECT koldstore.register_storage(
  name         => 'local-minio',
  storage_type => 's3',
  base_path    => 's3://koldstore-test/',
  credentials  => '{"access_key_id":"minioadmin","secret_access_key":"minioadmin"}'::jsonb,
  config       => '{"endpoint":"http://minio:9000","region":"us-east-1","path_style":true}'::jsonb
);

\echo '==> recreate demo schema'
DROP SCHEMA IF EXISTS demo CASCADE;
CREATE SCHEMA demo;

CREATE TABLE demo.conversations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  title text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE demo.messages (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_id uuid NOT NULL REFERENCES demo.conversations (id),
  author text NOT NULL,
  body text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX messages_conversation_id_idx ON demo.messages (conversation_id);

\echo '==> manage tables into koldstore (flush after 1000 hot rows)'
SELECT koldstore.manage_table(
  table_name     => 'demo.conversations',
  storage        => 'local-minio',
  hot_row_limit  => 1000,
  min_flush_rows => 1
);

SELECT koldstore.manage_table(
  table_name     => 'demo.messages',
  storage        => 'local-minio',
  hot_row_limit  => 1000,
  min_flush_rows => 1
);

\echo '==> seed conversations'
INSERT INTO demo.conversations (id, title)
SELECT
  ('00000000-0000-4000-8000-' || lpad(g::text, 12, '0'))::uuid,
  'conversation-' || g
FROM generate_series(1, 100) AS g;

\echo '==> seed 10_000 messages'
INSERT INTO demo.messages (conversation_id, author, body, created_at)
SELECT
  ('00000000-0000-4000-8000-' || lpad(((g % 100) + 1)::text, 12, '0'))::uuid,
  'user-' || ((g % 50) + 1),
  'message body ' || g,
  now() - ((10000 - g) || ' seconds')::interval
FROM generate_series(1, 10000) AS g;

\echo '==> regular update/delete metadata changes'
UPDATE demo.messages
SET body = body || ' (edited)'
WHERE body = 'message body 1';

INSERT INTO demo.messages (conversation_id, author, body)
VALUES (
  '00000000-0000-4000-8000-000000000001'::uuid,
  'delete-test',
  'this row will be deleted'
);

DELETE FROM demo.messages
WHERE author = 'delete-test';

\echo '==> hot row counts before flush'
SELECT 'conversations' AS table_name, count(*) AS hot_rows FROM demo.conversations
UNION ALL
SELECT 'messages', count(*) FROM demo.messages;

\echo '==> clean-schema sanity check'
SELECT
  table_schema,
  table_name,
  count(*) FILTER (WHERE column_name IN ('_seq', '_commit_seq', '_deleted', '_user_id')) AS internal_columns
FROM information_schema.columns
WHERE table_schema = 'demo'
  AND table_name IN ('conversations', 'messages')
GROUP BY table_schema, table_name
ORDER BY table_name;

SELECT
  to_regclass('koldstore.conversations__cl') IS NOT NULL AS conversations_mirror_exists,
  to_regclass('koldstore.messages__cl') IS NOT NULL AS messages_mirror_exists,
  to_regclass('koldstore.row_events') IS NOT NULL AS global_row_events_exists;

\echo '==> queue flush jobs (force)'
SELECT koldstore.flush_table(table_name => 'demo.conversations') AS conversations_flush_job;
SELECT koldstore.flush_table(table_name => 'demo.messages') AS messages_flush_job;

\echo '==> table status'
SELECT 'demo.conversations' AS table_name, koldstore.describe_table(table_name => 'demo.conversations');
SELECT 'demo.messages' AS table_name, koldstore.describe_table(table_name => 'demo.messages');

\echo '==> queued flush jobs'
SELECT
  j.id,
  c.relname AS table_name,
  j.scope_key,
  j.job_type,
  j.status,
  j.created_at
FROM system.jobs j
JOIN pg_class c ON c.oid = j.table_oid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'demo'
ORDER BY j.created_at;

\echo '==> manifest state'
SELECT
  c.relname AS table_name,
  m.scope_key,
  m.manifest_path,
  m.sync_state,
  m.segment_count
FROM koldstore.manifest m
JOIN pg_class c ON c.oid = m.table_oid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'demo'
ORDER BY c.relname;

\echo '==> flush completion check'
SELECT
  'completed' AS current_demo_state,
  'A completed flush has completed jobs, manifest sync_state = in_sync, segment_count > 0, active segments, and manifest.json in MinIO.' AS how_to_verify;

\echo '==> recent mirror-backed changes include normal insert/update/delete'
SELECT op, deleted, count(*) AS events
FROM koldstore.changes_since('demo.messages', 0, 20000)
GROUP BY op, deleted
ORDER BY op, deleted;

\echo '==> sample queries'
SELECT id, title, created_at
FROM demo.conversations
ORDER BY created_at
LIMIT 5;

SELECT m.id, c.title, m.author, m.body, m.created_at
FROM demo.messages m
JOIN demo.conversations c ON c.id = m.conversation_id
ORDER BY m.created_at DESC
LIMIT 5;

SELECT
  c.title,
  count(*) AS message_count
FROM demo.messages m
JOIN demo.conversations c ON c.id = m.conversation_id
GROUP BY c.title
ORDER BY message_count DESC, c.title
LIMIT 10;

\echo '==> cold storage validation scaffold'
SELECT koldstore.validate_cold_storage('demo.messages');

\echo '==> demo complete'
