//! Pure SQL builders for cross-runtime catalog **reads**.
//!
//! Ownership:
//! - this module: relation resolve, managed snapshots, flush policy/storage,
//!   cold-segment counts/stats, in-sync manifest scan context
//! - `koldstore-migrate`: schema registry **writes** and migration-only reads
//! - `koldstore-flush`: cold segment / manifest **writes**
//!
//! SPI execution stays in `pg_koldstore`.

use koldstore_common::{SqlParamType, SqlResult, SqlStatement};

/// Builds a relation name lookup by PostgreSQL OID.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_qualified_relation_by_oid() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve qualified relation by oid",
        "SELECT format('%I.%I', n.nspname, c.relname)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = $1::oid",
        [SqlParamType::Oid],
    )
}

/// Builds a JSON relation context lookup by PostgreSQL OID.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_relation_context_by_oid() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve relation context by oid",
        "SELECT jsonb_build_object('namespace', n.nspname, 'name', c.relname)::text
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = $1::oid",
        [SqlParamType::Oid],
    )
}

/// Builds an async-capture managed-relation lookup by source table OID.
///
/// Returns JSON text with `table_oid`, `mirror` (`regclass` text), and
/// `primary_key` for active schemas with `mirror_capture_mode = async`.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_async_managed_relation_by_oid() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve async managed relation by table oid",
        r#"
SELECT (SELECT jsonb_build_object(
    'table_oid', s.table_oid::text,
    'mirror', s.mirror_relation::text,
    'primary_key', s.primary_key,
    'segment_order_column_id', (s.options->>'segment_order_column_id')::int,
    'segment_order_column', (
      SELECT c->>'name'
      FROM jsonb_array_elements(s.columns) AS c
      WHERE (c->>'column_id')::int = (s.options->>'segment_order_column_id')::int
      LIMIT 1
    ),
    'segment_order_type_oid', (
      SELECT (c->>'type_oid')::bigint
      FROM jsonb_array_elements(s.columns) AS c
      WHERE (c->>'column_id')::int = (s.options->>'segment_order_column_id')::int
      LIMIT 1
    )
)::text
FROM koldstore.schemas s
WHERE s.active AND s.table_oid = $1::oid
  AND COALESCE(s.options->>'mirror_capture_mode', 'strict') = 'async'
LIMIT 1)
"#,
        [SqlParamType::Oid],
    )
}

/// Builds an active mirror relation lookup for a managed table.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_mirror_relation_by_table_oid() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve mirror relation by table oid",
        r#"
SELECT format('%I.%I', n.nspname, c.relname)
FROM koldstore.schemas s
JOIN pg_class c ON c.oid = s.mirror_relation
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE s.table_oid = $1::oid
ORDER BY s.active DESC, s.version DESC
LIMIT 1
"#,
        [SqlParamType::Oid],
    )
}

/// Builds a storage ID lookup by registered storage name.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_storage_id_by_name() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve storage id by name",
        "SELECT id FROM koldstore.storage WHERE name = $1",
        [SqlParamType::Text],
    )
}

/// Builds the active schema/storage context lookup used by flush.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_active_flush_storage_context() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve active flush storage context",
        r#"
SELECT jsonb_build_object(
    'base_path', st.base_path,
    'storage_type', st.storage_type,
    'credentials', COALESCE(st.credentials, '{}'::jsonb),
    'config', COALESCE(st.config, '{}'::jsonb),
    'schema_version', s.version,
    'compression', COALESCE(s.options->>'compression', 'zstd')
)::text
FROM koldstore.schemas s
JOIN koldstore.storage st ON st.id = s.storage_id
WHERE s.table_oid = $1::oid
  AND s.active
  AND s.initialization_state = 'complete'
ORDER BY s.version DESC
LIMIT 1
"#,
        [SqlParamType::Oid],
    )
}

/// Builds the stable managed-table snapshot lookup.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_managed_table_snapshot() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve managed table snapshot",
        r#"
SELECT jsonb_build_object(
    'table_oid', s.table_oid::bigint,
    'schema_version', s.version,
    'active', s.active,
    'initialization_state', s.initialization_state,
    'mirror_relation', format('%I.%I', n.nspname, c.relname),
    'primary_key', s.primary_key,
    'primary_key_shape', s.primary_key_shape,
    'scope_column', s.scope_column,
    'options', s.options
)::text
FROM koldstore.schemas s
JOIN pg_class c ON c.oid = s.mirror_relation
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE s.table_oid = $1::oid
ORDER BY s.active DESC, s.version DESC
LIMIT 1
"#,
        [SqlParamType::Oid],
    )
}

/// Builds the active flush-policy options lookup for a managed table.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_active_flush_policy_options() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve active flush policy options",
        r#"
SELECT options
FROM koldstore.schemas
WHERE table_oid = $1::oid
  AND active
ORDER BY version DESC
LIMIT 1
"#,
        [SqlParamType::Oid],
    )
}

/// Builds the latest published manifest scan context for merge-scan planning.
///
/// Returns one JSON text row with manifest path, generation, storage base path,
/// and active shared-scope cold-segment stats when a published manifest exists.
///
/// `sync_state = 'pending_write'` after hot DML still exposes the last published
/// cold segments; only the placeholder pre-flush row (`manifest_path = 'pending'`)
/// is treated as hot-only.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_in_sync_manifest_scan_context() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve published manifest scan context",
        r#"
SELECT jsonb_build_object(
  'manifest_path', m.manifest_path,
  'generation', m.generation,
  'base_path', st.base_path,
  'storage_type', st.storage_type,
  'credentials', COALESCE(st.credentials, '{}'::jsonb),
  'config', COALESCE(st.config, '{}'::jsonb),
  'segments', COALESCE((
      SELECT jsonb_agg(
          jsonb_build_object(
              'object_path', cs.object_path,
              'schema_version', cs.schema_version,
              'physical_names', COALESCE((
                  SELECT jsonb_object_agg(
                      (column_value->>'column_id'),
                      (column_value->>'name')
                  )
                  FROM koldstore.schemas historical_schema
                  CROSS JOIN LATERAL jsonb_array_elements(historical_schema.columns) column_value
                  WHERE historical_schema.table_oid = cs.table_oid
                    AND historical_schema.version = cs.schema_version
                    AND (column_value->>'column_id')::smallint IN (
                        SELECT value::smallint
                        FROM pg_catalog.jsonb_array_elements_text($2::jsonb) AS requested(value)
                    )
              ), '{}'::jsonb),
              'byte_size', cs.byte_size
          )
          ORDER BY cs.batch_number
      )
      FROM koldstore.cold_segments cs
      WHERE cs.table_oid = $1::oid
        AND cs.scope_key = ''
        AND cs.status = 'active'
  ), '[]'::jsonb)
)::text
FROM koldstore.manifest m
JOIN koldstore.schemas s ON s.table_oid = m.table_oid AND s.active AND s.initialization_state = 'complete'
JOIN koldstore.storage st ON st.id = s.storage_id
WHERE m.table_oid = $1::oid
  AND m.manifest_path IS DISTINCT FROM 'pending'
  AND m.generation > 0
ORDER BY m.generation DESC
LIMIT 1
"#,
        [SqlParamType::Oid, SqlParamType::Jsonb],
    )
}

/// Builds active cold-segment candidates for an inclusive closed range.
///
/// Parameters are table OID, scope key, stable column ID, type OID, codec
/// version, encoded lower bound, and encoded upper bound.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_cold_segment_candidates_closed_range() -> SqlResult<SqlStatement> {
    plan_cold_segment_candidates(
        "resolve active cold segment candidates for closed range",
        "AND csi.min_value <= $7::bytea\n  AND csi.max_value >= $6::bytea",
        vec![
            SqlParamType::Oid,
            SqlParamType::Text,
            SqlParamType::Integer,
            SqlParamType::Oid,
            SqlParamType::Integer,
            SqlParamType::Bytea,
            SqlParamType::Bytea,
        ],
    )
}

/// Builds active cold-segment candidates for an inclusive lower bound.
///
/// Parameters are table OID, scope key, stable column ID, type OID, codec
/// version, and encoded lower bound.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_cold_segment_candidates_lower_bound() -> SqlResult<SqlStatement> {
    plan_cold_segment_candidates(
        "resolve active cold segment candidates for lower bound",
        "AND csi.max_value >= $6::bytea",
        vec![
            SqlParamType::Oid,
            SqlParamType::Text,
            SqlParamType::Integer,
            SqlParamType::Oid,
            SqlParamType::Integer,
            SqlParamType::Bytea,
        ],
    )
}

/// Builds active cold-segment candidates for an inclusive upper bound.
///
/// Parameters are table OID, scope key, stable column ID, type OID, codec
/// version, and encoded upper bound.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_cold_segment_candidates_upper_bound() -> SqlResult<SqlStatement> {
    plan_cold_segment_candidates(
        "resolve active cold segment candidates for upper bound",
        "AND csi.min_value <= $6::bytea",
        vec![
            SqlParamType::Oid,
            SqlParamType::Text,
            SqlParamType::Integer,
            SqlParamType::Oid,
            SqlParamType::Integer,
            SqlParamType::Bytea,
        ],
    )
}

fn plan_cold_segment_candidates(
    operation: &str,
    bound_predicate: &str,
    param_types: Vec<SqlParamType>,
) -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        operation,
        &format!(
            r#"
SELECT
    cs.object_path,
    cs.byte_size,
    cs.schema_version,
    cs.min_seq,
    cs.max_seq,
    cs.min_commit_seq,
    cs.max_commit_seq,
    COALESCE((
        SELECT jsonb_object_agg(
            (column_value->>'column_id'),
            (column_value->>'name')
        )
        FROM koldstore.schemas historical_schema
        CROSS JOIN LATERAL jsonb_array_elements(historical_schema.columns) column_value
        WHERE historical_schema.table_oid = cs.table_oid
          AND historical_schema.version = cs.schema_version
    ), '{{}}'::jsonb)::text AS physical_names
FROM koldstore.cold_segment_index csi
JOIN koldstore.cold_segments cs
  ON cs.segment_id = csi.segment_id
 AND cs.table_oid = csi.table_oid
 AND cs.scope_key = csi.scope_key
WHERE csi.table_oid = $1::oid
  AND csi.scope_key = $2::text
  AND csi.column_id = $3::smallint
  AND csi.type_oid = $4::oid
  AND csi.codec_version = $5::smallint
  AND cs.status = 'active'
  {bound_predicate}
ORDER BY cs.batch_number, cs.segment_id
"#
        ),
        param_types,
    )
}

/// Builds the next flush batch number lookup for shared-scope cold segments.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_next_flush_batch_number() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve next flush batch number",
        "SELECT COALESCE(max(batch_number), 0) + 1 FROM koldstore.cold_segments WHERE table_oid = $1::oid AND scope_key = ''",
        [SqlParamType::Oid],
    )
}

/// Counts `pending` + `active` shared-scope segments (flush reconcile before activate).
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_publishable_cold_segment_count() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve publishable cold segment count",
        "SELECT count(*)::bigint FROM koldstore.cold_segments WHERE table_oid = $1::oid AND scope_key = '' AND status IN ('pending', 'active')",
        [SqlParamType::Oid],
    )
}

/// Builds pending+active cold-segment rows for derived `manifest.json` before activate.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_publishable_cold_segments_for_manifest_json() -> SqlResult<SqlStatement> {
    plan_cold_segments_for_manifest_json_with_statuses("pending', 'active")
}

fn plan_cold_segments_for_manifest_json_with_statuses(
    status_in_list: &str,
) -> SqlResult<SqlStatement> {
    // status_in_list is a trusted internal fragment ('active' or "pending', 'active").
    SqlStatement::read_with_params(
        "resolve cold segments for manifest",
        &format!(
            r#"
SELECT COALESCE(jsonb_agg(
    jsonb_build_object(
        'object_path', object_path,
        'batch_number', batch_number,
        'min_seq', min_seq,
        'max_seq', max_seq,
        'min_commit_seq', min_commit_seq,
        'max_commit_seq', max_commit_seq,
        'row_count', row_count,
        'byte_size', byte_size,
        'schema_version', schema_version,
        'column_stats', column_stats
    )
    ORDER BY batch_number, segment_id
)::text, '[]')
FROM koldstore.cold_segments
WHERE table_oid = $1::oid
  AND scope_key = ''
  AND status IN ('{status_in_list}')
"#
        ),
        [SqlParamType::Oid],
    )
}

/// Reads the current catalog manifest generation for CAS activate.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_manifest_generation() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve manifest generation",
        "SELECT generation FROM koldstore.manifest WHERE table_oid = $1::oid AND scope_key = ''",
        [SqlParamType::Oid],
    )
}

/// Lists expired pending segment object paths for recovery.
///
/// `$2` is the TTL in seconds (`bigint`).
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_expired_pending_segment_paths() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve expired pending segment paths",
        r#"
SELECT COALESCE(jsonb_agg(object_path ORDER BY created_at, segment_id)::text, '[]')
FROM koldstore.cold_segments
WHERE table_oid = $1::oid
  AND scope_key = ''
  AND status = 'pending'
  AND created_at < now() - ($2::bigint * interval '1 second')
"#,
        [SqlParamType::Oid, SqlParamType::BigInt],
    )
}

/// Deletes expired pending catalog rows (after objects are quarantined).
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_delete_expired_pending_segments() -> SqlResult<SqlStatement> {
    SqlStatement::write_with_params(
        "delete expired pending segments",
        r#"
DELETE FROM koldstore.cold_segments
WHERE table_oid = $1::oid
  AND scope_key = ''
  AND status = 'pending'
  AND created_at < now() - ($2::bigint * interval '1 second')
"#,
        [SqlParamType::Oid, SqlParamType::BigInt],
    )
}

#[cfg(test)]
mod tests {
    use super::{
        plan_cold_segment_candidates_closed_range, plan_cold_segment_candidates_lower_bound,
        plan_cold_segment_candidates_upper_bound, plan_in_sync_manifest_scan_context,
    };
    use koldstore_common::SqlParamType;

    #[test]
    fn merge_scan_context_omits_binary_index_bounds() {
        let statement = plan_in_sync_manifest_scan_context().unwrap();

        assert!(statement
            .sql
            .contains("'schema_version', cs.schema_version"));
        assert!(statement.sql.contains("'physical_names'"));
        assert!(statement
            .sql
            .contains("historical_schema.version = cs.schema_version"));
        assert!(statement
            .sql
            .contains("jsonb_array_elements_text($2::jsonb)"));
        assert!(!statement.sql.contains("'column_stats'"));
        assert!(!statement.sql.contains("cold_segment_stats"));
        assert!(!statement.sql.contains("cold_segment_index"));
        assert!(!statement.sql.contains("convert_from"));
        assert_eq!(statement.param_types.len(), 2);
    }

    #[test]
    fn closed_range_candidates_use_both_non_nullable_bounds() {
        let statement = plan_cold_segment_candidates_closed_range().unwrap();

        assert!(statement.sql.contains("koldstore.cold_segment_index"));
        assert!(statement.sql.contains("csi.min_value <= $7::bytea"));
        assert!(statement.sql.contains("csi.max_value >= $6::bytea"));
        assert!(statement.sql.contains("cs.status = 'active'"));
        assert!(statement.sql.contains("AS physical_names"));
        assert!(!statement.sql.contains("IS NULL OR"));
        assert_eq!(
            statement.param_types,
            vec![
                SqlParamType::Oid,
                SqlParamType::Text,
                SqlParamType::Integer,
                SqlParamType::Oid,
                SqlParamType::Integer,
                SqlParamType::Bytea,
                SqlParamType::Bytea,
            ]
        );
    }

    #[test]
    fn one_sided_candidate_plans_use_only_the_relevant_index_bound() {
        let lower = plan_cold_segment_candidates_lower_bound().unwrap();
        assert!(lower.sql.contains("csi.max_value >= $6::bytea"));
        assert!(!lower.sql.contains("csi.min_value <="));
        assert!(!lower.sql.contains("IS NULL OR"));

        let upper = plan_cold_segment_candidates_upper_bound().unwrap();
        assert!(upper.sql.contains("csi.min_value <= $6::bytea"));
        assert!(!upper.sql.contains("csi.max_value >="));
        assert!(!upper.sql.contains("IS NULL OR"));

        let expected = vec![
            SqlParamType::Oid,
            SqlParamType::Text,
            SqlParamType::Integer,
            SqlParamType::Oid,
            SqlParamType::Integer,
            SqlParamType::Bytea,
        ];
        assert_eq!(lower.param_types, expected);
        assert_eq!(upper.param_types, expected);
    }
}
