\set id random(1, 100000)
DELETE FROM bench_events
WHERE id = :id;
