TRUNCATE TABLE bench_events RESTART IDENTITY;

SELECT setseed(0.42);

INSERT INTO bench_events (
  tenant_id,
  user_id,
  conversation_id,
  event_type,
  status,
  priority,
  score,
  amount,
  is_active,
  is_deleted,
  payload,
  metadata,
  tags,
  binary_hash,
  created_at,
  updated_at
)
SELECT
  md5('tenant-' || (g % 25)::text)::uuid,
  md5('user-' || (g % 2500)::text)::uuid,
  md5('conversation-' || (g % 10000)::text)::uuid,
  (ARRAY['message_created', 'message_read', 'message_edited', 'reaction_added', 'attachment_uploaded'])[1 + (g % 5)],
  (ARRAY['queued', 'processing', 'completed', 'failed'])[1 + (g % 4)],
  (g % 10)::int,
  round((random() * 1000)::numeric, 4)::double precision,
  round((random() * 100000)::numeric, 4),
  g % 11 <> 0,
  false,
  jsonb_build_object(
    'event_id', g,
    'message_length', 20 + (g % 400),
    'channel', (ARRAY['web', 'ios', 'android', 'api'])[1 + (g % 4)],
    'attributes', jsonb_build_object('retry', g % 7 = 0, 'shard', g % 16)
  ),
  jsonb_build_object(
    'seed', 'deterministic-100k',
    'tenant_bucket', g % 25,
    'user_bucket', g % 2500
  ),
  ARRAY[
    'tenant-' || (g % 25)::text,
    'priority-' || (g % 10)::text,
    (ARRAY['chat', 'storage', 'billing', 'search'])[1 + (g % 4)]
  ]::text,
  md5('binary-hash-' || g::text),
  now() - ((g % 365)::text || ' days')::interval - ((g % 86400)::text || ' seconds')::interval,
  now() - ((g % 30)::text || ' days')::interval
FROM generate_series(1, :BENCH_ROWS) AS g;

ANALYZE bench_events;
