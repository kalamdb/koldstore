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
    'scope_column', s.scope_column
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
              'column_stats', cs.column_stats,
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
  AND COALESCE(m.generation, '') <> ''
ORDER BY m.generation DESC
LIMIT 1
"#,
        [SqlParamType::Oid],
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

/// Builds an active shared-scope cold-segment count lookup.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_active_cold_segment_count() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve active cold segment count",
        "SELECT count(*)::bigint FROM koldstore.cold_segments WHERE table_oid = $1::oid AND scope_key = '' AND status = 'active'",
        [SqlParamType::Oid],
    )
}

/// Builds an active cold-segment manifest row lookup for flush finalization.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_active_cold_segments_for_manifest_json() -> SqlResult<SqlStatement> {
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
FROM koldstore.cold_segments
WHERE table_oid = $1::oid
  AND scope_key = ''
  AND status = 'active'
"#,
        [SqlParamType::Oid],
    )
}
