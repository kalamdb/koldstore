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

/// Builds the complete active schema-version lookup for a managed table.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_active_schema_version() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve active schema version",
        r#"
SELECT jsonb_build_object(
    'id', id,
    'table_oid', table_oid::bigint,
    'version', version,
    'columns', columns,
    'next_column_id', next_column_id,
    'active', active
)::text
FROM koldstore.schemas
WHERE table_oid = $1::oid
  AND active
ORDER BY version DESC
LIMIT 1
"#,
        [SqlParamType::Oid],
    )
}

/// Builds a complete historical schema-version lookup.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_schema_version_at() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve schema version",
        r#"
SELECT jsonb_build_object(
    'id', id,
    'table_oid', table_oid::bigint,
    'version', version,
    'columns', columns,
    'next_column_id', next_column_id,
    'active', active
)::text
FROM koldstore.schemas
WHERE table_oid = $1::oid
  AND version = $2
LIMIT 1
"#,
        [SqlParamType::Oid, SqlParamType::Integer],
    )
}

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
    'manifest_generation', (
        SELECT m.generation
        FROM koldstore.manifest m
        WHERE m.table_oid = s.table_oid
          AND m.manifest_path IS DISTINCT FROM 'pending'
          AND COALESCE(m.generation, '') <> ''
          AND EXISTS (
              SELECT 1
              FROM koldstore.segments cs
              WHERE cs.table_oid = m.table_oid
                AND cs.scope_key = m.scope_key
                AND cs.status = 'published'
          )
        ORDER BY m.updated_at DESC
        LIMIT 1
    )
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
              'column_stats', COALESCE((
                  SELECT jsonb_object_agg(
                      css.column_id::text,
                      jsonb_strip_nulls(jsonb_build_object(
                          'min', CASE
                              WHEN css.min_value IS NULL THEN NULL
                              ELSE pg_catalog.convert_from(css.min_value, 'UTF8')::jsonb
                          END,
                          'max', CASE
                              WHEN css.max_value IS NULL THEN NULL
                              ELSE pg_catalog.convert_from(css.max_value, 'UTF8')::jsonb
                          END
                      ))
                  )
                  FROM koldstore.segment_stats css
                  WHERE css.segment_id = cs.segment_id
                    AND css.table_oid = cs.table_oid
                    AND css.scope_key = cs.scope_key
                    AND css.column_id IN (
                        SELECT pg_catalog.jsonb_array_elements_text($2::jsonb)::bigint
                    )
              ), '{}'::jsonb),
              'byte_size', cs.byte_size
          )
          ORDER BY cs.batch_number
      )
      FROM koldstore.segments cs
      WHERE cs.table_oid = $1::oid
        AND cs.scope_key = ''
        AND cs.status = 'published'
  ), '[]'::jsonb)
)::text
FROM koldstore.manifest m
JOIN koldstore.schemas s ON s.table_oid = m.table_oid AND s.active AND s.initialization_state = 'complete'
JOIN koldstore.storage st ON st.id = s.storage_id
WHERE m.table_oid = $1::oid
  AND m.manifest_path IS DISTINCT FROM 'pending'
  AND COALESCE(m.generation, '') <> ''
ORDER BY m.generation DESC
LIMIT 1
"#,
        [SqlParamType::Oid, SqlParamType::Jsonb],
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
        "SELECT COALESCE(max(batch_number), 0) + 1 FROM koldstore.segments WHERE table_oid = $1::oid AND scope_key = ''",
        [SqlParamType::Oid],
    )
}

/// Builds a cold-segment count for flush manifest reconciliation.
///
/// Includes `staged` (written this flush, not yet promoted) and `published`.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_active_segment_count() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve active cold segment count",
        "SELECT count(*)::bigint FROM koldstore.segments WHERE table_oid = $1::oid AND scope_key = '' AND status IN ('staged', 'published')",
        [SqlParamType::Oid],
    )
}

/// Builds cold-segment rows for flush manifest finalization.
///
/// Includes `staged` and `published` so a flush that just inserted staged rows
/// can assemble a complete manifest before promoting to `published`.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_active_segments_for_manifest_json() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve active cold segments for manifest",
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
FROM koldstore.segments
WHERE table_oid = $1::oid
  AND scope_key = ''
  AND status IN ('staged', 'published')
"#,
        [SqlParamType::Oid],
    )
}

#[cfg(test)]
mod tests {
    use koldstore_common::SqlParamType;

    use super::{
        plan_active_schema_version, plan_in_sync_manifest_scan_context, plan_schema_version_at,
    };

    #[test]
    fn merge_scan_context_reads_only_requested_normalized_stats() {
        let statement = plan_in_sync_manifest_scan_context().unwrap();

        assert!(statement.sql.contains("koldstore.segment_stats"));
        assert!(statement
            .sql
            .contains("jsonb_array_elements_text($2::jsonb)"));
        assert!(!statement.sql.contains("'column_stats', cs.column_stats"));
        assert_eq!(statement.param_types.len(), 2);
    }

    #[test]
    fn managed_snapshot_uses_a_compact_published_cold_presence_probe() {
        let statement = super::plan_managed_table_snapshot().unwrap();

        assert!(statement.sql.contains("'manifest_generation'"));
        assert!(statement.sql.contains("EXISTS"));
        assert!(statement.sql.contains("status = 'published'"));
        assert!(!statement.sql.contains("jsonb_agg"));
    }

    #[test]
    fn schema_version_queries_return_complete_catalog_rows() {
        let active = plan_active_schema_version().unwrap();
        let historical = plan_schema_version_at().unwrap();

        for statement in [&active, &historical] {
            assert!(statement.sql.contains("'columns', columns"));
            assert!(statement.sql.contains("'next_column_id', next_column_id"));
            assert!(statement.sql.contains("'version', version"));
        }
        assert_eq!(active.param_types, vec![SqlParamType::Oid]);
        assert_eq!(
            historical.param_types,
            vec![SqlParamType::Oid, SqlParamType::Integer]
        );
    }
}
