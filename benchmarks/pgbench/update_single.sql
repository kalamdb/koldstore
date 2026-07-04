\set id random(1, 100000)
UPDATE bench_events
SET status = 'updated',
    updated_at = now()
WHERE id = :id;
