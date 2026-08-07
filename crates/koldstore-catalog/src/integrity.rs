//! Catalog integrity check SQL plans (pg-free).
//!
//! Cheap, structured checks for `koldstore.verify_table_integrity`. Deeper
//! object/Parquet validation stays deferred to process-harness work.
//!
//! Segment-id lists in check details are capped at [`SEGMENT_ID_SAMPLE_LIMIT`]
//! with `truncated: true` when more rows match, so backends with many segments
//! do not build unbounded JSON.

use koldstore_common::{SqlParamType, SqlResult, SqlStatement};

/// Max segment ids embedded in each integrity check detail payload.
pub const SEGMENT_ID_SAMPLE_LIMIT: i64 = 32;

/// Plans a JSON integrity report for one managed table.
///
/// `$1` = table oid, `$2` = pending-segment TTL seconds.
///
/// Returns one text jsonb with shape:
/// `{ "table_oid", "ok", "checks": [ { "name", "ok", "detail", ... } ] }`.
///
/// v1 checks:
/// - exactly one active managed schema
/// - at most one active (`pending`/`running`) flush job
/// - no `active` cold segments missing checksum or path
/// - pending segments older than TTL are flagged (not auto-fixed)
/// - row-group array cardinality matches `row_group_count` for active segments
/// - no duplicate active `(writer_job_id, pass_id, segment_ordinal)`
/// - active segments have positive `byte_size`
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_verify_table_integrity() -> SqlResult<SqlStatement> {
    let limit = SEGMENT_ID_SAMPLE_LIMIT;
    SqlStatement::read_with_params(
        "verify table integrity",
        &format!(
            r#"
WITH params AS (
    SELECT $1::oid AS table_oid, $2::bigint AS pending_ttl_seconds
),
active_schemas AS (
    SELECT count(*)::bigint AS n
    FROM koldstore.schemas s, params p
    WHERE s.table_oid = p.table_oid
      AND s.active
),
active_flush_jobs AS (
    SELECT count(*)::bigint AS n
    FROM koldstore.jobs j, params p
    WHERE j.table_oid = p.table_oid
      AND j.scope_key = ''
      AND j.job_type = 'flush'
      AND j.status IN ('pending', 'running')
),
active_missing_identity AS (
    SELECT count(*)::bigint AS n,
           COALESCE(
               (
                   SELECT jsonb_agg(sample.segment_id ORDER BY sample.segment_id)
                   FROM (
                       SELECT cs.segment_id::text AS segment_id
                       FROM koldstore.cold_segments cs, params p
                       WHERE cs.table_oid = p.table_oid
                         AND cs.status = 'active'
                         AND (
                               cs.path IS NULL
                            OR btrim(cs.path) = ''
                            OR cs.checksum IS NULL
                            OR btrim(cs.checksum) = ''
                         )
                       ORDER BY cs.segment_id
                       LIMIT {limit}
                   ) sample
               ),
               '[]'::jsonb
           ) AS segment_ids
    FROM koldstore.cold_segments cs, params p
    WHERE cs.table_oid = p.table_oid
      AND cs.status = 'active'
      AND (
            cs.path IS NULL
         OR btrim(cs.path) = ''
         OR cs.checksum IS NULL
         OR btrim(cs.checksum) = ''
      )
),
stale_pending AS (
    SELECT count(*)::bigint AS n,
           COALESCE(
               (
                   SELECT jsonb_agg(sample.segment_id ORDER BY sample.ord)
                   FROM (
                       SELECT cs.segment_id::text AS segment_id,
                              row_number() OVER (
                                  ORDER BY cs.created_at, cs.segment_id
                              ) AS ord
                       FROM koldstore.cold_segments cs, params p
                       WHERE cs.table_oid = p.table_oid
                         AND cs.status = 'pending'
                         AND cs.created_at < now()
                             - (p.pending_ttl_seconds * interval '1 second')
                       ORDER BY cs.created_at, cs.segment_id
                       LIMIT {limit}
                   ) sample
               ),
               '[]'::jsonb
           ) AS segment_ids
    FROM koldstore.cold_segments cs, params p
    WHERE cs.table_oid = p.table_oid
      AND cs.status = 'pending'
      AND cs.created_at < now() - (p.pending_ttl_seconds * interval '1 second')
),
row_group_cardinality AS (
    SELECT count(*)::bigint AS n,
           COALESCE(
               (
                   SELECT jsonb_agg(sample.segment_id ORDER BY sample.segment_id)
                   FROM (
                       SELECT cs.segment_id::text AS segment_id
                       FROM koldstore.cold_segments cs, params p
                       WHERE cs.table_oid = p.table_oid
                         AND cs.status = 'active'
                         AND (
                               cardinality(cs.row_group_row_counts)
                                   IS DISTINCT FROM cs.row_group_count
                            OR cardinality(cs.row_group_min_seqs)
                                   IS DISTINCT FROM cs.row_group_count
                            OR cardinality(cs.row_group_max_seqs)
                                   IS DISTINCT FROM cs.row_group_count
                         )
                       ORDER BY cs.segment_id
                       LIMIT {limit}
                   ) sample
               ),
               '[]'::jsonb
           ) AS segment_ids
    FROM koldstore.cold_segments cs, params p
    WHERE cs.table_oid = p.table_oid
      AND cs.status = 'active'
      AND (
            cardinality(cs.row_group_row_counts) IS DISTINCT FROM cs.row_group_count
         OR cardinality(cs.row_group_min_seqs) IS DISTINCT FROM cs.row_group_count
         OR cardinality(cs.row_group_max_seqs) IS DISTINCT FROM cs.row_group_count
      )
),
duplicate_active_pass AS (
    SELECT count(*)::bigint AS n,
           COALESCE(
               (
                   SELECT jsonb_agg(sample.pass_key ORDER BY sample.pass_key)
                   FROM (
                       SELECT format(
                                  '%s/%s/%s',
                                  d.writer_job_id,
                                  d.pass_id,
                                  d.segment_ordinal
                              ) AS pass_key
                       FROM (
                           SELECT cs.writer_job_id,
                                  cs.pass_id,
                                  cs.segment_ordinal
                           FROM koldstore.cold_segments cs, params p
                           WHERE cs.table_oid = p.table_oid
                             AND cs.status = 'active'
                             AND cs.writer_job_id IS NOT NULL
                             AND cs.pass_id IS NOT NULL
                             AND cs.segment_ordinal IS NOT NULL
                           GROUP BY cs.writer_job_id, cs.pass_id, cs.segment_ordinal
                           HAVING count(*) > 1
                       ) d
                       ORDER BY d.writer_job_id, d.pass_id, d.segment_ordinal
                       LIMIT {limit}
                   ) sample
               ),
               '[]'::jsonb
           ) AS pass_keys
    FROM (
        SELECT cs.writer_job_id, cs.pass_id, cs.segment_ordinal
        FROM koldstore.cold_segments cs, params p
        WHERE cs.table_oid = p.table_oid
          AND cs.status = 'active'
          AND cs.writer_job_id IS NOT NULL
          AND cs.pass_id IS NOT NULL
          AND cs.segment_ordinal IS NOT NULL
        GROUP BY cs.writer_job_id, cs.pass_id, cs.segment_ordinal
        HAVING count(*) > 1
    ) dups
),
active_bad_byte_size AS (
    SELECT count(*)::bigint AS n,
           COALESCE(
               (
                   SELECT jsonb_agg(sample.segment_id ORDER BY sample.segment_id)
                   FROM (
                       SELECT cs.segment_id::text AS segment_id
                       FROM koldstore.cold_segments cs, params p
                       WHERE cs.table_oid = p.table_oid
                         AND cs.status = 'active'
                         AND (cs.byte_size IS NULL OR cs.byte_size <= 0)
                       ORDER BY cs.segment_id
                       LIMIT {limit}
                   ) sample
               ),
               '[]'::jsonb
           ) AS segment_ids
    FROM koldstore.cold_segments cs, params p
    WHERE cs.table_oid = p.table_oid
      AND cs.status = 'active'
      AND (cs.byte_size IS NULL OR cs.byte_size <= 0)
),
checks AS (
    SELECT jsonb_build_array(
        jsonb_build_object(
            'name', 'one_active_managed_schema',
            'ok', (SELECT n = 1 FROM active_schemas),
            'detail', jsonb_build_object(
                'active_schema_count', (SELECT n FROM active_schemas)
            )
        ),
        jsonb_build_object(
            'name', 'at_most_one_active_flush_job',
            'ok', (SELECT n <= 1 FROM active_flush_jobs),
            'detail', jsonb_build_object(
                'active_flush_job_count', (SELECT n FROM active_flush_jobs)
            )
        ),
        jsonb_build_object(
            'name', 'active_segments_have_checksum_and_path',
            'ok', (SELECT n = 0 FROM active_missing_identity),
            'detail', jsonb_build_object(
                'missing_count', (SELECT n FROM active_missing_identity),
                'segment_ids', (SELECT segment_ids FROM active_missing_identity),
                'truncated', (SELECT n > {limit} FROM active_missing_identity)
            )
        ),
        jsonb_build_object(
            'name', 'no_stale_pending_segments',
            'ok', (SELECT n = 0 FROM stale_pending),
            'detail', jsonb_build_object(
                'stale_count', (SELECT n FROM stale_pending),
                'segment_ids', (SELECT segment_ids FROM stale_pending),
                'truncated', (SELECT n > {limit} FROM stale_pending),
                'pending_ttl_seconds', (SELECT pending_ttl_seconds FROM params),
                'auto_fixed', false
            )
        ),
        jsonb_build_object(
            'name', 'active_segment_row_group_cardinality',
            'ok', (SELECT n = 0 FROM row_group_cardinality),
            'detail', jsonb_build_object(
                'mismatch_count', (SELECT n FROM row_group_cardinality),
                'segment_ids', (SELECT segment_ids FROM row_group_cardinality),
                'truncated', (SELECT n > {limit} FROM row_group_cardinality)
            )
        ),
        jsonb_build_object(
            'name', 'no_duplicate_active_pass',
            'ok', (SELECT n = 0 FROM duplicate_active_pass),
            'detail', jsonb_build_object(
                'duplicate_count', (SELECT n FROM duplicate_active_pass),
                'pass_keys', (SELECT pass_keys FROM duplicate_active_pass),
                'truncated', (SELECT n > {limit} FROM duplicate_active_pass)
            )
        ),
        jsonb_build_object(
            'name', 'active_segment_byte_size_positive',
            'ok', (SELECT n = 0 FROM active_bad_byte_size),
            'detail', jsonb_build_object(
                'bad_count', (SELECT n FROM active_bad_byte_size),
                'segment_ids', (SELECT segment_ids FROM active_bad_byte_size),
                'truncated', (SELECT n > {limit} FROM active_bad_byte_size)
            )
        )
    ) AS arr
)
SELECT jsonb_build_object(
    'table_oid', (SELECT table_oid FROM params),
    'ok', (
        SELECT bool_and((check_row->>'ok')::boolean)
        FROM jsonb_array_elements((SELECT arr FROM checks)) AS check_row
    ),
    'checks', (SELECT arr FROM checks)
)::text
"#
        ),
        [SqlParamType::Oid, SqlParamType::BigInt],
    )
}

#[cfg(test)]
mod tests {
    use super::{plan_verify_table_integrity, SEGMENT_ID_SAMPLE_LIMIT};

    #[test]
    fn verify_table_integrity_plan_covers_v1_checks() {
        let statement = plan_verify_table_integrity().unwrap();
        assert!(statement.sql.contains("one_active_managed_schema"));
        assert!(statement.sql.contains("at_most_one_active_flush_job"));
        assert!(statement
            .sql
            .contains("active_segments_have_checksum_and_path"));
        assert!(statement.sql.contains("no_stale_pending_segments"));
        assert!(statement
            .sql
            .contains("active_segment_row_group_cardinality"));
        assert!(statement.sql.contains("no_duplicate_active_pass"));
        assert!(statement.sql.contains("active_segment_byte_size_positive"));
        assert!(statement.sql.contains("'auto_fixed', false"));
        assert!(statement.sql.contains("'truncated'"));
        assert!(statement
            .sql
            .contains(&format!("LIMIT {SEGMENT_ID_SAMPLE_LIMIT}")));
        assert!(statement.sql.contains("$2::bigint"));
    }
}
