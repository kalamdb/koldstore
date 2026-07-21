-- Hidden VHS setup: prepare demo data without recording it.
-- Visible tape: baseline → manage → flush → hot/cold → sizes → count → EXPLAIN → Parquet ls.

CREATE EXTENSION IF NOT EXISTS koldstore;

-- Demo uses max_rows_per_file => 10000; default floor is 1000.
ALTER DATABASE koldstoredb SET koldstore.min_max_rows_per_file = 10000;

-- Keep the visible recording free of trigger-create NOTICE spam.
ALTER DATABASE koldstoredb SET client_min_messages TO warning;

SELECT koldstore.register_storage(
  name         => 'local-dev',
  storage_type => 'filesystem',
  base_path    => '/koldstore/data',
  credentials  => '{}'::jsonb,
  config       => '{}'::jsonb
);

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE app.messages (
  id bigint PRIMARY KEY,
  account_id bigint NOT NULL,
  title text NOT NULL,
  body text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO app.messages (id, account_id, title, body)
SELECT
  gs,
  gs % 3,
  'message-' || lpad(gs::text, 6, '0'),
  'hello from row ' || gs
FROM generate_series(1, 1000000) AS gs;

-- Capture baseline sizes here so the visible demo never shows raw byte queries.
CREATE TABLE app.demo_baseline AS
SELECT
  pg_relation_size('app.messages'::regclass) AS heap_bytes,
  pg_indexes_size('app.messages'::regclass) AS indexes_bytes,
  pg_total_relation_size('app.messages'::regclass) AS total_bytes;
