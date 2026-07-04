\set tenant_idx random(0, 24)
\set user_idx random(0, 2499)
\set conversation_idx random(0, 9999)
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
VALUES (
  md5('tenant-' || (:tenant_idx)::text)::uuid,
  md5('user-' || (:user_idx)::text)::uuid,
  md5('conversation-' || (:conversation_idx)::text)::uuid,
  'message_created',
  'queued',
  (:user_idx % 10),
  (:user_idx)::double precision / 10.0,
  (:user_idx)::numeric,
  true,
  false,
  jsonb_build_object('source', 'pgbench', 'user_idx', (:user_idx)::int),
  jsonb_build_object('benchmark', 'insert_single'),
  ARRAY['pgbench', 'single-insert'],
  decode(md5('insert-single-' || (:user_idx)::text), 'hex'),
  now(),
  now()
);
