\set op random(1, 100)
\set id random(1, :max_id)
\set user_idx random(0, 2499)
\set conversation_idx random(0, 9999)
\if :op <= 60
SELECT *
FROM bench_events
WHERE user_id = md5('user-' || (:user_idx)::text)::uuid
  AND created_at >= now() - interval '7 days'
ORDER BY created_at DESC
LIMIT 50;
\elif :op <= 80
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
  md5('tenant-' || (:user_idx % 25)::text)::uuid,
  md5('user-' || (:user_idx)::text)::uuid,
  md5('conversation-' || (:conversation_idx)::text)::uuid,
  'message_created',
  'queued',
  (:user_idx % 10),
  (:user_idx)::double precision / 10.0,
  (:user_idx)::numeric,
  true,
  false,
  jsonb_build_object('source', 'pgbench', 'workload', 'mixed'),
  jsonb_build_object('benchmark', 'mixed_20_clients'),
  'pgbench,mixed',
  md5('mixed-' || (:user_idx)::text),
  now(),
  now()
);
\elif :op <= 90
UPDATE bench_events
SET status = 'mixed_updated',
    updated_at = now()
WHERE id = :id;
\else
DELETE FROM bench_events
WHERE id = :id;
\endif
