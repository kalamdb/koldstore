\set user_idx random(0, 2499)
DELETE FROM bench_events
WHERE user_id = md5('user-' || (:user_idx)::text)::uuid
  AND created_at < now() - interval '30 days';
