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
SELECT
  md5('tenant-' || (:tenant_idx)::text)::uuid,
  md5('user-' || ((:user_idx + s) % 2500)::text)::uuid,
  md5('conversation-' || ((:conversation_idx + s) % 10000)::text)::uuid,
  'message_created',
  'queued',
  ((:user_idx + s) % 10)::int,
  ((:user_idx + s)::double precision / 10.0),
  (:user_idx + s)::numeric,
  true,
  false,
  jsonb_build_object('source', 'pgbench', 'batch_row', s),
  jsonb_build_object('benchmark', 'insert_batch', 'batch_size', (:batch_size)::int),
  'pgbench,batch-insert',
  md5('insert-batch-' || (:user_idx + s)::text),
  now(),
  now()
FROM generate_series(1, :batch_size) AS s;
