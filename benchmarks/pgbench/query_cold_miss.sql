\set user_idx random(0, 2499)
SELECT *
FROM bench_events
WHERE user_id = md5('user-' || (:user_idx)::text)::uuid
  AND created_at < now() - interval '5 years'
ORDER BY created_at DESC
LIMIT 100;
