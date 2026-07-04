CREATE INDEX idx_bench_events_user_created
ON bench_events (user_id, created_at DESC);

CREATE INDEX idx_bench_events_tenant_created
ON bench_events (tenant_id, created_at DESC);

CREATE INDEX idx_bench_events_conversation_created
ON bench_events (conversation_id, created_at DESC);

CREATE INDEX idx_bench_events_event_type
ON bench_events (event_type);

CREATE INDEX idx_bench_events_payload_gin
ON bench_events USING GIN (payload);

ANALYZE bench_events;
