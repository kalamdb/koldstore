\set id random(1, :max_id)
DELETE FROM bench_events
WHERE id = :id;
