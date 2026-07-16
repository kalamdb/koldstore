-- Wide events table used by tests/storage/pg_vs_koldstore.rs.
-- Placeholders: {{schema}}, {{table}}
--
-- ~50 application columns so heap vs Parquet size differences are obvious at
-- larger row counts. Keep indexes limited to hot-path lookup columns.

CREATE TABLE {{schema}}.{{table}} (
  id              bigint PRIMARY KEY,
  account_id      bigint NOT NULL,
  tenant_id       text NOT NULL,
  event_type      text NOT NULL,
  status          text NOT NULL,
  priority        integer NOT NULL,
  score           double precision NOT NULL,
  amount_cents    bigint NOT NULL,
  quantity        integer NOT NULL,
  is_active       boolean NOT NULL,
  is_deleted      boolean NOT NULL,
  region          text NOT NULL,
  country         text NOT NULL,
  city            text NOT NULL,
  channel         text NOT NULL,
  source          text NOT NULL,
  campaign        text NOT NULL,
  device          text NOT NULL,
  os_name         text NOT NULL,
  app_version     text NOT NULL,
  session_id      text NOT NULL,
  request_id      text NOT NULL,
  trace_id        text NOT NULL,
  user_agent      text NOT NULL,
  ip_address      text NOT NULL,
  referrer        text NOT NULL,
  path            text NOT NULL,
  method          text NOT NULL,
  response_code   integer NOT NULL,
  latency_ms      integer NOT NULL,
  payload_bytes   integer NOT NULL,
  error_code      text,
  error_message   text,
  tag_a           text NOT NULL,
  tag_b           text NOT NULL,
  tag_c           text NOT NULL,
  tag_d           text NOT NULL,
  tag_e           text NOT NULL,
  metric_1        double precision NOT NULL,
  metric_2        double precision NOT NULL,
  metric_3        double precision NOT NULL,
  metric_4        double precision NOT NULL,
  metric_5        double precision NOT NULL,
  flag_1          boolean NOT NULL,
  flag_2          boolean NOT NULL,
  flag_3          boolean NOT NULL,
  flag_4          boolean NOT NULL,
  flag_5          boolean NOT NULL,
  note_1          text NOT NULL,
  note_2          text NOT NULL,
  note_3          text NOT NULL,
  note_4          text NOT NULL,
  note_5          text NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX {{table}}_account_idx ON {{schema}}.{{table}} (account_id);
CREATE INDEX {{table}}_tenant_created_idx ON {{schema}}.{{table}} (tenant_id, created_at);
CREATE INDEX {{table}}_event_type_idx ON {{schema}}.{{table}} (event_type);

-- Keep the timed comparison deterministic at 10M rows. Long async catch-up can
-- otherwise give autoanalyze enough time to scan one side while the other is
-- being measured; explicit ANALYZE/VACUUM phases below remain part of the test.
ALTER TABLE {{schema}}.{{table}} SET (autovacuum_enabled = false);
