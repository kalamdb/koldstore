-- Release-image load test: manage table, 100k insert, flush, more inserts,
-- hot+cold queries, and full changes_since drain.
\set ON_ERROR_STOP on

\echo '==> version / preload'
SELECT koldstore_version();
SELECT koldstore.preload_status();
SHOW shared_preload_libraries;
SHOW wal_level;

\echo '==> register filesystem storage'
SELECT koldstore.register_storage(
  name         => 'release-fs',
  storage_type => 'filesystem',
  base_path    => '/koldstore-data/cold/',
  credentials  => '{}'::jsonb,
  config       => '{}'::jsonb
);

\echo '==> create + manage table (hot_row_limit=10000)'
DROP TABLE IF EXISTS loadtest CASCADE;
CREATE TABLE loadtest (
  id bigint PRIMARY KEY,
  body text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE loadtest SET (
  koldstore_enabled = true,
  koldstore_storage = 'release-fs',
  koldstore_hot_row_limit = 10000,
  koldstore_min_flush_rows = 1,
  koldstore_max_rows_per_file = 10000
);

\echo '==> insert wave 1: 100000 rows'
INSERT INTO loadtest (id, body)
SELECT g, 'wave1-' || g
FROM generate_series(1, 100000) AS g;

\echo '==> query after wave 1 (pre-flush)'
SELECT count(*) AS wave1_count FROM loadtest;
SELECT id, body FROM loadtest WHERE id = 1;
SELECT id, body FROM loadtest WHERE id = 100000;
SELECT jsonb_pretty(koldstore.table_status(table_name => 'loadtest'::regclass)) AS status_pre_flush;

\echo '==> force flush so oldest rows go cold'
SELECT jsonb_pretty(koldstore.flush_table(
  table_name => 'loadtest'::regclass,
  force => true
)) AS flush_result;

\echo '==> wait briefly for async mirror / job settle'
SELECT pg_sleep(2);

\echo '==> status after flush (expect cold segments + pruned hot)'
SELECT jsonb_pretty(koldstore.table_status(table_name => 'loadtest'::regclass)) AS status_post_flush;
SELECT
  (koldstore.table_status(table_name => 'loadtest'::regclass)->>'hot_rows')::bigint AS hot_rows,
  (koldstore.table_status(table_name => 'loadtest'::regclass)->>'cold_row_count')::bigint AS cold_rows;
SELECT count(*) AS active_cold_segments
FROM koldstore.cold_segments
WHERE table_oid = 'loadtest'::regclass AND status = 'active';

\echo '==> query after flush (merge hot+cold)'
SELECT count(*) AS after_flush_count FROM loadtest;
EXPLAIN (COSTS OFF) SELECT count(*) FROM loadtest;
SELECT id, body FROM loadtest WHERE id = 1;          -- should be cold
SELECT id, body FROM loadtest WHERE id = 100000;     -- may still be hot
EXPLAIN (COSTS OFF) SELECT * FROM loadtest WHERE id = 1;
EXPLAIN (COSTS OFF) SELECT * FROM loadtest WHERE id = 100000;

\echo '==> insert wave 2: 25000 more rows'
INSERT INTO loadtest (id, body)
SELECT g, 'wave2-' || g
FROM generate_series(100001, 125000) AS g;

\echo '==> query after wave 2'
SELECT count(*) AS after_wave2_count FROM loadtest;
SELECT id, body FROM loadtest WHERE id = 1;
SELECT id, body FROM loadtest WHERE id = 50000;
SELECT id, body FROM loadtest WHERE id = 125000;
SELECT jsonb_pretty(koldstore.table_status(table_name => 'loadtest'::regclass)) AS status_wave2;

\echo '==> optional second flush to keep hot bounded'
SELECT jsonb_pretty(koldstore.flush_table(
  table_name => 'loadtest'::regclass,
  force => true
)) AS flush2_result;
SELECT pg_sleep(2);
SELECT
  (koldstore.table_status(table_name => 'loadtest'::regclass)->>'hot_rows')::bigint AS hot_rows,
  (koldstore.table_status(table_name => 'loadtest'::regclass)->>'cold_row_count')::bigint AS cold_rows,
  (SELECT count(*) FROM loadtest) AS logical_rows;

\echo '==> changes_since full drain (scan all retained changes)'
CREATE TEMP TABLE cs_drain (
  seq bigint,
  op text,
  pk jsonb,
  deleted boolean,
  source text
);

DO $$
DECLARE
  cur bigint := 0;
  batch_count int;
  total int := 0;
  batch_limit int := 10000;
BEGIN
  LOOP
    INSERT INTO cs_drain (seq, op, pk, deleted, source)
    SELECT seq, op, pk, deleted, source
    FROM koldstore.changes_since(
      table_name => 'loadtest'::regclass,
      since_seq  => cur,
      limit_rows => batch_limit
    );
    GET DIAGNOSTICS batch_count = ROW_COUNT;
    EXIT WHEN batch_count = 0;
    total := total + batch_count;
    SELECT max(seq) INTO cur FROM cs_drain;
    RAISE NOTICE 'changes_since page: +% rows (cursor=%, total=%)', batch_count, cur, total;
  END LOOP;
  RAISE NOTICE 'changes_since drain complete: % rows', total;
END
$$;

SELECT count(*) AS changes_since_rows FROM cs_drain;
SELECT count(DISTINCT (pk->>'id')) AS distinct_pks FROM cs_drain;
SELECT min(seq) AS min_seq, max(seq) AS max_seq FROM cs_drain;
SELECT source, count(*) FROM cs_drain GROUP BY source ORDER BY source;
SELECT op, count(*) FROM cs_drain GROUP BY op ORDER BY op;

\echo '==> final assertions'
DO $$
DECLARE
  logical_rows bigint;
  cs_rows bigint;
  hot_rows bigint;
  cold_rows bigint;
  segments bigint;
BEGIN
  SELECT count(*) INTO logical_rows FROM loadtest;
  SELECT count(*) INTO cs_rows FROM cs_drain;
  SELECT (koldstore.table_status(table_name => 'loadtest'::regclass)->>'hot_rows')::bigint
    INTO hot_rows;
  SELECT (koldstore.table_status(table_name => 'loadtest'::regclass)->>'cold_row_count')::bigint
    INTO cold_rows;
  SELECT count(*) INTO segments
  FROM koldstore.cold_segments
  WHERE table_oid = 'loadtest'::regclass AND status = 'active';

  IF logical_rows <> 125000 THEN
    RAISE EXCEPTION 'expected 125000 logical rows, got %', logical_rows;
  END IF;
  IF segments < 1 THEN
    RAISE EXCEPTION 'expected active cold segments, got %', segments;
  END IF;
  IF cold_rows IS NULL OR cold_rows < 1 THEN
    RAISE EXCEPTION 'expected cold_row_count > 0, got %', cold_rows;
  END IF;
  IF hot_rows IS NULL OR hot_rows < 1 THEN
    RAISE EXCEPTION 'expected hot_rows > 0, got %', hot_rows;
  END IF;
  IF cs_rows < 125000 THEN
    RAISE EXCEPTION 'changes_since drained %, expected at least 125000', cs_rows;
  END IF;

  RAISE NOTICE 'PASS logical=% hot=% cold=% segments=% changes_since=%',
    logical_rows, hot_rows, cold_rows, segments, cs_rows;
END
$$;

\echo '==> release hot/cold load test complete'
