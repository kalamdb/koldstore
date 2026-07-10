DROP TABLE IF EXISTS bench_events CASCADE;

CREATE TABLE bench_events (
  id BIGSERIAL PRIMARY KEY,
  tenant_id UUID NOT NULL,
  user_id UUID NOT NULL,
  conversation_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  status TEXT NOT NULL,
  priority INT NOT NULL,
  score DOUBLE PRECISION NOT NULL,
  amount DOUBLE PRECISION,
  is_active BOOLEAN NOT NULL,
  is_deleted BOOLEAN NOT NULL DEFAULT false,
  payload JSONB,
  metadata JSONB,
  tags TEXT,
  binary_hash TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
