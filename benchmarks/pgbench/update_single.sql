\set id random(1, :max_id)
UPDATE bench_events
SET status = 'updated',
    updated_at = now()
WHERE id = :id;
