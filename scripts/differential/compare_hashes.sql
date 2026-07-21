-- Deterministic dual-hash checks after SQLsmith fuzz (baseline vs managed).

SELECT 'pk_eq' AS check_name,
  md5(coalesce(string_agg(row_to_json(t)::text, E'\n' ORDER BY id), '')) AS baseline_hash,
  (
    SELECT md5(coalesce(string_agg(row_to_json(t)::text, E'\n' ORDER BY id), ''))
    FROM (
      SELECT id, flag, qty, amount, label FROM diff_ks.managed ORDER BY id
    ) t
  ) AS managed_hash
FROM (
  SELECT id, flag, qty, amount, label FROM diff_ks.baseline ORDER BY id
) t
HAVING md5(coalesce(string_agg(row_to_json(t)::text, E'\n' ORDER BY id), ''))
    = (
      SELECT md5(coalesce(string_agg(row_to_json(u)::text, E'\n' ORDER BY id), ''))
      FROM (
        SELECT id, flag, qty, amount, label FROM diff_ks.managed ORDER BY id
      ) u
    );

DO $$
DECLARE
  baseline_hash text;
  managed_hash text;
BEGIN
  SELECT md5(coalesce(string_agg(row_to_json(t)::text, E'\n' ORDER BY id), ''))
  INTO baseline_hash
  FROM (SELECT id, flag, qty, amount, label FROM diff_ks.baseline ORDER BY id) t;

  SELECT md5(coalesce(string_agg(row_to_json(t)::text, E'\n' ORDER BY id), ''))
  INTO managed_hash
  FROM (SELECT id, flag, qty, amount, label FROM diff_ks.managed ORDER BY id) t;

  IF baseline_hash IS DISTINCT FROM managed_hash THEN
    RAISE EXCEPTION 'differential hash mismatch baseline=% managed=%',
      baseline_hash, managed_hash;
  END IF;

  RAISE NOTICE 'differential_hash_ok';
END $$;
