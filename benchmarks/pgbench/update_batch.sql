\set user_idx random(0, 2499)
UPDATE bench_events
SET status = 'batch_updated',
    updated_at = now()
WHERE user_id = md5('user-' || (:user_idx)::text)::uuid
  AND created_at >= now() - interval '7 days';
