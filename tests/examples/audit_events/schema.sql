CREATE TABLE audit.account_events (
  tenant_id text NOT NULL,
  account_id text NOT NULL,
  id bigint PRIMARY KEY,
  actor_id text NOT NULL,
  event_type text NOT NULL,
  before_state jsonb NOT NULL,
  after_state jsonb NOT NULL,
  ip text,
  created_at timestamptz NOT NULL
);
