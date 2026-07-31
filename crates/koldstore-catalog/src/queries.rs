//! Pure SQL builders for cross-runtime catalog **reads**.
//!
//! Ownership:
//! - this module: relation resolve, managed snapshots, flush policy/storage,
//!   cold-segment counts/stats, in-sync manifest scan context, O(1) row
//!   counters, operator backup/validate/export catalog SELECTs, and active
//!   schema refresh context
//! - `koldstore-migrate`: schema registry **writes** and `pg_catalog`
//!   introspection for migration
//! - `koldstore-flush`: cold segment / manifest **writes**, jobs, and
//!   operator plan wrappers that bind optional table/scope args
//!
//! SPI execution stays in `pg_koldstore`.

use koldstore_common::{SqlParamType, SqlResult, SqlStatement};

/// Trailing-slash table prefix from `st.regular_path_tmpl` + `n`/`c` relation names.
///
/// This fragment is inserted via `{SQL_TABLE_PREFIX}` into outer `format!`
/// templates, so braces here are copied verbatim into the emitted SQL.
const SQL_TABLE_PREFIX: &str = r#"CASE
      WHEN regexp_replace(
          replace(replace(st.regular_path_tmpl, '{namespace}', n.nspname), '{tableName}', c.relname),
          '(^/+)|(/+$)',
          '',
          'g'
      ) = ''
      THEN ''
      ELSE regexp_replace(
          replace(replace(st.regular_path_tmpl, '{namespace}', n.nspname), '{tableName}', c.relname),
          '(^/+)|(/+$)',
          '',
          'g'
      ) || '/'
  END"#;

/// `jsonb_object_agg(column_id → name)` over one historical schema version.
///
/// Inserted via `{SQL_PHYSICAL_NAMES_*}`; braces are literal SQL (not `format!`
/// escapes). Empty object default must be `'{}'::jsonb`, not `'{{}}'::jsonb`.
const SQL_PHYSICAL_NAMES_ALL: &str = r#"COALESCE((
        SELECT jsonb_object_agg(
            (column_value->>'column_id'),
            (column_value->>'name')
        )
        FROM koldstore.schemas historical_schema
        CROSS JOIN LATERAL jsonb_array_elements(historical_schema.columns) column_value
        WHERE historical_schema.table_oid = cs.table_oid
          AND historical_schema.version = cs.schema_version
    ), '{}'::jsonb)"#;

/// Like [`SQL_PHYSICAL_NAMES_ALL`] but filtered to requested column ids in `$2`.
const SQL_PHYSICAL_NAMES_REQUESTED: &str = r#"COALESCE((
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
              ), '{}'::jsonb)"#;

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

/// Builds a managed-relation lookup by source table OID for WAL capture.
///
/// Returns JSON text with `table_oid`, `mirror` (`regclass` text), and
/// `primary_key` for active schemas.
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
    'primary_key', (
      SELECT COALESCE(jsonb_agg(elem->>'name' ORDER BY ord), '[]'::jsonb)
      FROM jsonb_array_elements(s.primary_key) WITH ORDINALITY AS t(elem, ord)
    ),
    'segment_order_column_id', (s.options->>'segment_order_column_id')::int,
    'segment_order_column', (
      SELECT c->>'name'
      FROM jsonb_array_elements(s.columns) AS c
      WHERE (c->>'column_id')::int = (s.options->>'segment_order_column_id')::int
      LIMIT 1
    ),
    'segment_order_type_oid', (
      SELECT a.atttypid::bigint
      FROM jsonb_array_elements(s.columns) AS c
      JOIN pg_catalog.pg_attribute a
        ON a.attrelid = s.table_oid
       AND a.attnum = (c->>'column_id')::smallint
       AND NOT a.attisdropped
      WHERE (c->>'column_id')::int = (s.options->>'segment_order_column_id')::int
      LIMIT 1
    )
)::text
FROM koldstore.schemas s
WHERE s.active AND s.table_oid = $1::oid
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

/// Builds async mirror slot status JSON (`pg_replication_slots`).
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_async_mirror_slot_status() -> SqlResult<SqlStatement> {
    // Prefer CAST(... AS text) over `expr::text`: nested jsonb_build_object
    // casts with `::` have failed SPI with `syntax error at or near "."`.
    SqlStatement::read_with_params(
        "async mirror slot status",
        "SELECT COALESCE(\
           (SELECT CAST(jsonb_build_object(\
              'slot_name', slot_name,\
              'active', active,\
              'confirmed_flush_lsn', CAST(confirmed_flush_lsn AS text),\
              'retained_bytes', pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)\
            ) AS text)\
            FROM pg_catalog.pg_replication_slots WHERE slot_name = $1), \
           CAST(jsonb_build_object('slot_name', $1, 'present', false) AS text)\
         )",
        [SqlParamType::Text],
    )
}

/// Builds async mirror durable apply-state status JSON.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_async_mirror_state_status() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "async mirror durable state status",
        "SELECT COALESCE(\
           (SELECT CAST(jsonb_build_object(\
              'applied_lsn', CAST(applied_lsn AS text),\
              'updated_at', updated_at,\
              'updated_at_age_seconds', EXTRACT(EPOCH FROM (now() - updated_at))\
            ) AS text)\
            FROM koldstore.async_mirror_state WHERE database_oid = $1), \
           CAST(jsonb_build_object('present', false) AS text)\
         )",
        [SqlParamType::Oid],
    )
}

/// Builds a probe for whether any schema row exists for a table OID.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_table_already_managed() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "probe whether table is already managed",
        "SELECT EXISTS (SELECT 1 FROM koldstore.schemas WHERE table_oid = $1::oid)",
        [SqlParamType::Oid],
    )
}

/// Builds the ALTER TABLE management options lookup (storage name + options).
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_management_options_lookup() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "lookup management storage and options",
        "SELECT (SELECT jsonb_build_object('storage', st.name, 'options', s.options) \
         FROM koldstore.schemas s \
         JOIN koldstore.storage st ON st.id = s.storage_id \
         WHERE s.table_oid = $1)",
        [SqlParamType::Oid],
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
    'regular_path_tmpl', st.regular_path_tmpl,
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

/// Builds the active managed-schema refresh context lookup.
///
/// Used before schema-refresh planning in migrate; the SQL is a shared catalog
/// read so it lives here rather than in `koldstore-migrate`.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_active_schema_refresh_context_json() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve active schema refresh context",
        r#"
SELECT jsonb_build_object(
    'version', version,
    'table_type', table_type,
    'storage_id', storage_id::text,
    'scope_column', scope_column,
    'mirror_relation', mirror_relation::text,
    'primary_key', primary_key,
    'columns', columns,
    'indexed_columns', indexed_columns,
    'options', options
)::text
FROM koldstore.schemas
WHERE table_oid = $1::oid
  AND active
  AND initialization_state = 'complete'
ORDER BY version DESC
LIMIT 1
"#,
        [SqlParamType::Oid],
    )
}

/// Builds a lean published-manifest hint for merge-scan planning.
///
/// Returns `(generation, active_segment_count)` without loading segment
/// metadata, storage credentials, or physical-name maps. Planner hot-only
/// prune and cost estimates only need these two scalars.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_published_manifest_planner_hint() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve published manifest planner hint",
        r#"
SELECT m.generation::bigint, count(cs.segment_id)::bigint AS segment_count
FROM koldstore.manifest m
LEFT JOIN koldstore.cold_segments cs
  ON cs.table_oid = m.table_oid
 AND cs.scope_key = ''
 AND cs.status = 'active'
WHERE m.table_oid = $1::oid
  AND m.scope_key = ''
  AND m.generation > 0
GROUP BY m.generation
ORDER BY m.generation DESC
LIMIT 1
"#,
        [SqlParamType::Oid],
    )
}

/// Builds the latest published manifest scan context for merge-scan planning.
///
/// Returns one JSON text row with table prefix, generation, storage base path,
/// and active shared-scope cold-segment stats when a published manifest exists.
///
/// `sync_state = 'pending_write'` after hot DML still exposes the last published
/// cold segments; only rows with a published generation are returned.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_in_sync_manifest_scan_context() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve published manifest scan context",
        &format!(
            r#"
WITH active_segments AS (
    SELECT
        cs.path,
        cs.schema_version,
        cs.min_seq,
        cs.max_seq,
        cs.byte_size,
        cs.batch_number,
        cs.table_oid
    FROM koldstore.cold_segments cs
    WHERE cs.table_oid = $1::oid
      AND cs.scope_key = ''
      AND cs.status = 'active'
),
-- Expand schema JSON once per distinct version, not once per segment.
schema_physical_names AS (
    SELECT
        cs.schema_version,
        {SQL_PHYSICAL_NAMES_REQUESTED} AS physical_names
    FROM (
        SELECT DISTINCT table_oid, schema_version
        FROM active_segments
    ) cs
)
SELECT jsonb_build_object(
  'table_prefix', {SQL_TABLE_PREFIX},
  'generation', m.generation,
  'base_path', st.base_path,
  'storage_type', st.storage_type,
  'credentials', COALESCE(st.credentials, '{{}}'::jsonb),
  'config', COALESCE(st.config, '{{}}'::jsonb),
  'segments', COALESCE((
      SELECT jsonb_agg(
          jsonb_build_object(
              'path', a.path,
              'schema_version', a.schema_version,
              'min_seq', a.min_seq,
              'max_seq', a.max_seq,
              'physical_names', COALESCE(names.physical_names, '{{}}'::jsonb),
              'byte_size', a.byte_size
          )
          ORDER BY a.batch_number
      )
      FROM active_segments a
      LEFT JOIN schema_physical_names names
        ON names.schema_version = a.schema_version
  ), '[]'::jsonb)
)::text
FROM koldstore.manifest m
JOIN koldstore.schemas s ON s.table_oid = m.table_oid AND s.active AND s.initialization_state = 'complete'
JOIN pg_catalog.pg_class c ON c.oid = s.table_oid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN koldstore.storage st ON st.id = s.storage_id
WHERE m.table_oid = $1::oid
  AND m.generation > 0
ORDER BY m.generation DESC
LIMIT 1
"#
        ),
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
        "AND csi.min_value <= $7::bytea\n      AND csi.max_value >= $6::bytea",
        "csi.min_value IS NULL",
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
        "csi.max_value IS NULL",
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
        "csi.min_value IS NULL",
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

/// Builds a compact aggregate-bound lookup for one indexed cold column.
///
/// The result contains the active-segment count, the matching index-row count,
/// the count of index rows with unknown scalar bounds, and the lowest/highest
/// Sort Key V1 byte strings. Callers may prune the entire cold side only when
/// the two counts match and no bound is unknown.
///
/// Parameters are table OID, scope key, stable column ID, type OID, and codec
/// version.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_cold_column_aggregate_bounds() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve aggregate active cold column bounds",
        r#"
SELECT
    (
        SELECT count(*)::bigint
        FROM koldstore.cold_segments cs
        WHERE cs.table_oid = $1::oid
          AND cs.scope_key = $2::text
          AND cs.status = 'active'
    ) AS active_segment_count,
    count(*)::bigint AS indexed_segment_count,
    count(*) FILTER (
        WHERE csi.min_value IS NULL OR csi.max_value IS NULL
    )::bigint AS unknown_bound_count,
    -- bytea has no min()/max() aggregates; use ordered LIMIT instead.
    (
        SELECT csi2.min_value
        FROM koldstore.cold_segment_index csi2
        JOIN koldstore.cold_segments cs2
          ON cs2.segment_id = csi2.segment_id
         AND cs2.table_oid = csi2.table_oid
         AND cs2.scope_key = csi2.scope_key
        WHERE csi2.table_oid = $1::oid
          AND csi2.scope_key = $2::text
          AND csi2.column_id = $3::smallint
          AND csi2.type_oid = $4::oid
          AND csi2.codec_version = $5::smallint
          AND cs2.status = 'active'
          AND csi2.min_value IS NOT NULL
        ORDER BY csi2.min_value ASC
        LIMIT 1
    ) AS min_value,
    (
        SELECT csi2.max_value
        FROM koldstore.cold_segment_index csi2
        JOIN koldstore.cold_segments cs2
          ON cs2.segment_id = csi2.segment_id
         AND cs2.table_oid = csi2.table_oid
         AND cs2.scope_key = csi2.scope_key
        WHERE csi2.table_oid = $1::oid
          AND csi2.scope_key = $2::text
          AND csi2.column_id = $3::smallint
          AND csi2.type_oid = $4::oid
          AND csi2.codec_version = $5::smallint
          AND cs2.status = 'active'
          AND csi2.max_value IS NOT NULL
        ORDER BY csi2.max_value DESC
        LIMIT 1
    ) AS max_value
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
"#,
        [
            SqlParamType::Oid,
            SqlParamType::Text,
            SqlParamType::Integer,
            SqlParamType::Oid,
            SqlParamType::Integer,
        ],
    )
}

/// Loads packed row-group arrays for already-selected candidate segments.
///
/// Parameters are table OID, scope key, candidate segment UUIDs, and indexed
/// column IDs. Segment-level lookup happens first through the scalar B-tree
/// plans; this statement only refines that bounded candidate set.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_cold_segment_candidate_row_group_indexes() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve packed row-group indexes for candidate segments",
        r#"
SELECT
    csi.segment_id,
    csi.column_id,
    cs.row_group_count,
    cs.row_group_row_counts,
    csi.row_group_min_values,
    csi.row_group_max_values,
    csi.row_group_null_counts
FROM koldstore.cold_segment_index csi
JOIN koldstore.cold_segments cs
  ON cs.segment_id = csi.segment_id
 AND cs.table_oid = csi.table_oid
 AND cs.scope_key = csi.scope_key
WHERE csi.table_oid = $1::oid
  AND csi.scope_key = $2::text
  AND csi.segment_id = ANY($3::uuid[])
  AND csi.column_id = ANY($4::smallint[])
  AND cs.status = 'active'
ORDER BY csi.segment_id, csi.column_id
"#,
        [
            SqlParamType::Oid,
            SqlParamType::Text,
            SqlParamType::UuidArray,
            SqlParamType::SmallIntArray,
        ],
    )
}

fn plan_cold_segment_candidates(
    operation: &str,
    bound_predicate: &str,
    unknown_predicate: &str,
    param_types: Vec<SqlParamType>,
) -> SqlResult<SqlStatement> {
    // UNION ALL (not OR) keeps each arm index-friendly on cold_segment_index.
    // Physical-name JSON is expanded once per schema version, not per segment.
    SqlStatement::read_with_params(
        operation,
        &format!(
            r#"
WITH matching_index AS (
    SELECT
        csi.segment_id,
        csi.table_oid,
        csi.scope_key,
        csi.column_id,
        csi.row_group_min_values,
        csi.row_group_max_values,
        csi.row_group_null_counts
    FROM koldstore.cold_segment_index csi
    WHERE csi.table_oid = $1::oid
      AND csi.scope_key = $2::text
      AND csi.column_id = $3::smallint
      AND csi.type_oid = $4::oid
      AND csi.codec_version = $5::smallint
      {bound_predicate}

    UNION ALL

    SELECT
        csi.segment_id,
        csi.table_oid,
        csi.scope_key,
        csi.column_id,
        csi.row_group_min_values,
        csi.row_group_max_values,
        csi.row_group_null_counts
    FROM koldstore.cold_segment_index csi
    WHERE csi.table_oid = $1::oid
      AND csi.scope_key = $2::text
      AND csi.column_id = $3::smallint
      AND csi.type_oid = $4::oid
      AND csi.codec_version = $5::smallint
      AND {unknown_predicate}
),
table_prefix AS (
    SELECT {SQL_TABLE_PREFIX} AS prefix
    FROM koldstore.schemas s
    JOIN koldstore.storage st ON st.id = s.storage_id
    JOIN pg_catalog.pg_class c ON c.oid = s.table_oid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE s.table_oid = $1::oid
      AND s.active
      AND s.initialization_state = 'complete'
    ORDER BY s.version DESC
    LIMIT 1
),
schema_physical_names AS (
    SELECT
        cs.schema_version,
        {SQL_PHYSICAL_NAMES_ALL} AS physical_names
    FROM (
        SELECT DISTINCT cs.table_oid, cs.schema_version
        FROM matching_index mi
        JOIN koldstore.cold_segments cs
          ON cs.segment_id = mi.segment_id
         AND cs.table_oid = mi.table_oid
         AND cs.scope_key = mi.scope_key
        WHERE cs.status = 'active'
    ) cs
)
SELECT
    CASE
      WHEN COALESCE(pref.prefix, '') = '' THEN cs.path
      ELSE pref.prefix || cs.path
    END,
    cs.byte_size,
    cs.schema_version,
    cs.min_seq,
    cs.max_seq,
    COALESCE(names.physical_names, '{{}}'::jsonb)::text AS physical_names,
    cs.segment_id,
    mi.column_id,
    cs.row_group_count,
    cs.row_group_row_counts,
    mi.row_group_min_values,
    mi.row_group_max_values,
    mi.row_group_null_counts
FROM matching_index mi
JOIN koldstore.cold_segments cs
  ON cs.segment_id = mi.segment_id
 AND cs.table_oid = mi.table_oid
 AND cs.scope_key = mi.scope_key
LEFT JOIN schema_physical_names names
  ON names.schema_version = cs.schema_version
LEFT JOIN table_prefix pref ON true
WHERE cs.status = 'active'
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
    // column_stats for export are derived from cold_segment_index — not stored on
    // cold_segments — so object-store manifests stay aligned with query prune.
    SqlStatement::read_with_params(
        "resolve cold segments for manifest",
        &format!(
            r#"
SELECT COALESCE(jsonb_agg(
    jsonb_build_object(
        'segment_id', cs.segment_id::text,
        'path', cs.path,
        'batch_number', cs.batch_number,
        'min_seq', cs.min_seq,
        'max_seq', cs.max_seq,
        'min_commit_seq', cs.min_commit_seq,
        'max_commit_seq', cs.max_commit_seq,
        'row_count', cs.row_count,
        'byte_size', cs.byte_size,
        'schema_version', cs.schema_version,
        'row_group_count', cs.row_group_count,
        'row_group_row_counts', to_jsonb(cs.row_group_row_counts),
        'row_group_min_seqs', to_jsonb(cs.row_group_min_seqs),
        'row_group_max_seqs', to_jsonb(cs.row_group_max_seqs),
        'status', cs.status,
        'checksum', cs.checksum,
        'object_etag', cs.object_etag,
        'created_at', cs.created_at,
        'index_bounds', (
            SELECT COALESCE(jsonb_agg(
                jsonb_build_object(
                    'column_id', i.column_id,
                    'type_oid', i.type_oid::bigint,
                    'codec_version', i.codec_version,
                    'min_value', CASE
                        WHEN i.min_value IS NULL THEN NULL
                        ELSE encode(i.min_value, 'hex')
                    END,
                    'max_value', CASE
                        WHEN i.max_value IS NULL THEN NULL
                        ELSE encode(i.max_value, 'hex')
                    END,
                    'row_group_min_values', (
                        SELECT jsonb_agg(
                            CASE
                                WHEN value IS NULL THEN NULL
                                ELSE to_jsonb(encode(value, 'hex'))
                            END
                            ORDER BY ordinal
                        )
                        FROM unnest(i.row_group_min_values)
                            WITH ORDINALITY AS bounds(value, ordinal)
                    ),
                    'row_group_max_values', (
                        SELECT jsonb_agg(
                            CASE
                                WHEN value IS NULL THEN NULL
                                ELSE to_jsonb(encode(value, 'hex'))
                            END
                            ORDER BY ordinal
                        )
                        FROM unnest(i.row_group_max_values)
                            WITH ORDINALITY AS bounds(value, ordinal)
                    ),
                    'row_group_null_counts', to_jsonb(i.row_group_null_counts)
                )
                ORDER BY i.column_id
            ), '[]'::jsonb)
            FROM koldstore.cold_segment_index i
            WHERE i.segment_id = cs.segment_id
        )
    )
    ORDER BY cs.batch_number, cs.segment_id
)::text, '[]')
FROM koldstore.cold_segments cs
WHERE cs.table_oid = $1::oid
  AND cs.scope_key = ''
  AND cs.status IN ('{status_in_list}')
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

/// Plans a read of cached O(1) row counters from `koldstore.manifest`.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_read_table_row_counters() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "read manifest row counters",
        r#"
SELECT jsonb_build_object(
  'hot_row_count', COALESCE(m.hot_row_count, 0)::bigint,
  'mirror_row_count', COALESCE(m.mirror_row_count, 0)::bigint,
  'cold_row_count', COALESCE(m.cold_row_count, 0)::bigint,
  'cold_segment_count', COALESCE(m.segment_count, 0)::bigint
)::text
FROM koldstore.manifest m
WHERE m.table_oid = $1::oid
  AND m.scope_key = ''
"#,
        [SqlParamType::Oid],
    )
}

/// Plans `koldstore.backup_manifest` catalog rows.
///
/// Optional filters: `$1` table (`regclass`, nullable) and `$2` scope key
/// (nullable).
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_backup_manifest_rows() -> SqlResult<SqlStatement> {
    // `$1`/`$2` are optional filters bound as nullable regclass/text by callers;
    // param metadata stays empty to match the historical ops SPI contract.
    SqlStatement::read(
        "backup manifest",
        "SELECT etag, generation, max_seq, max_commit_seq \
FROM koldstore.manifest \
WHERE ($1::regclass IS NULL OR table_oid = $1::regclass::oid) \
  AND ($2::text IS NULL OR scope_key = $2)",
    )
}

/// Plans `koldstore.validate_cold_storage` catalog rows.
///
/// Optional filter: `$1` table (`regclass`, nullable).
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_validate_cold_storage_rows() -> SqlResult<SqlStatement> {
    SqlStatement::read(
        "validate cold storage",
        "SELECT m.generation, cs.path, cs.row_count \
FROM koldstore.manifest m \
LEFT JOIN koldstore.cold_segments cs \
  ON cs.table_oid = m.table_oid \
 AND cs.scope_key = m.scope_key \
 AND cs.status = 'active' \
WHERE ($1::regclass IS NULL OR m.table_oid = $1::regclass::oid)",
    )
}

/// Plans `EXPORT TABLE` archive segment listing for one managed table.
///
/// Bind `$1` as the source table `regclass`.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_export_table_archive_segments() -> SqlResult<SqlStatement> {
    SqlStatement::read(
        "export table archive",
        "SELECT m.generation, cs.path, cs.row_count, cs.byte_size \
FROM koldstore.manifest m \
LEFT JOIN koldstore.cold_segments cs \
  ON cs.table_oid = m.table_oid \
 AND cs.scope_key = m.scope_key \
 AND cs.status = 'active' \
WHERE m.table_oid = $1::regclass::oid",
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
SELECT COALESCE(jsonb_agg(path ORDER BY created_at, segment_id)::text, '[]')
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
        plan_async_managed_relation_by_oid, plan_cold_column_aggregate_bounds,
        plan_cold_segment_candidate_row_group_indexes, plan_cold_segment_candidates_closed_range,
        plan_cold_segment_candidates_lower_bound, plan_cold_segment_candidates_upper_bound,
        plan_in_sync_manifest_scan_context, plan_publishable_cold_segments_for_manifest_json,
        plan_published_manifest_planner_hint,
    };
    use koldstore_common::SqlParamType;

    #[test]
    fn merge_scan_context_omits_binary_index_bounds() {
        let statement = plan_in_sync_manifest_scan_context().unwrap();

        assert!(statement.sql.contains("active_segments"));
        assert!(statement.sql.contains("schema_physical_names"));
        assert!(statement.sql.contains("'schema_version', a.schema_version"));
        assert!(statement.sql.contains("'physical_names'"));
        assert!(statement.sql.contains("'min_seq', a.min_seq"));
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
    fn interpolated_sql_fragments_keep_literal_json_and_path_braces() {
        let scan = plan_in_sync_manifest_scan_context().unwrap();
        let candidates = plan_cold_segment_candidates_closed_range().unwrap();
        for statement in [&scan, &candidates] {
            assert!(
                statement.sql.contains("'{}'::jsonb"),
                "empty jsonb default must be valid JSON literal"
            );
            assert!(
                !statement.sql.contains("'{{}}'::jsonb"),
                "doubled braces survive format! insertion and break ::jsonb"
            );
            assert!(statement.sql.contains("'{namespace}'"));
            assert!(statement.sql.contains("'{tableName}'"));
            assert!(!statement.sql.contains("'{{namespace}}'"));
            assert!(!statement.sql.contains("'{{tableName}}'"));
        }
    }

    #[test]
    fn closed_range_candidates_keep_unknown_bounds_then_fetch_packed_arrays() {
        let statement = plan_cold_segment_candidates_closed_range().unwrap();
        let packed = plan_cold_segment_candidate_row_group_indexes().unwrap();

        assert!(statement.sql.contains("koldstore.cold_segment_index"));
        assert!(statement.sql.contains("csi.min_value <= $7::bytea"));
        assert!(statement.sql.contains("csi.max_value >= $6::bytea"));
        assert!(statement.sql.contains("cs.status = 'active'"));
        assert!(statement.sql.contains("AS physical_names"));
        assert!(statement.sql.contains("schema_physical_names"));
        assert!(statement.sql.contains("matching_index"));
        assert!(!statement.sql.contains("matched_segments"));
        assert!(statement.sql.contains("UNION ALL"));
        assert!(statement.sql.contains("AND csi.min_value IS NULL"));
        assert!(!statement.sql.contains("\n    OR csi."));
        assert!(statement.sql.contains("cs.segment_id"));
        assert!(statement.sql.contains("row_group_min_values"));
        assert!(statement.sql.contains("row_group_max_values"));
        assert!(statement.sql.contains("row_group_null_counts"));
        assert!(packed.sql.contains("cs.row_group_count"));
        assert!(packed.sql.contains("cs.row_group_row_counts"));
        assert!(packed.sql.contains("csi.row_group_min_values"));
        assert!(packed.sql.contains("csi.row_group_max_values"));
        assert!(!packed.sql.contains("unnest("));
        assert!(!packed.sql.contains("jsonb_agg"));
        assert!(!packed.sql.contains("encode("));
        assert!(packed.sql.contains("csi.row_group_null_counts"));
        assert!(packed.sql.contains("csi.segment_id = ANY($3::uuid[])"));
        assert_eq!(
            packed.param_types,
            vec![
                SqlParamType::Oid,
                SqlParamType::Text,
                SqlParamType::UuidArray,
                SqlParamType::SmallIntArray,
            ]
        );
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
        assert!(lower.sql.contains("UNION ALL"));
        assert!(lower.sql.contains("csi.max_value IS NULL"));
        assert!(!lower.sql.contains("csi.max_value IS NULL OR"));
        assert!(!lower.sql.contains("csi.min_value <="));
        assert!(!lower.sql.contains("INDEX"));
        assert!(!lower.sql.contains("column_name"));

        let upper = plan_cold_segment_candidates_upper_bound().unwrap();
        assert!(upper.sql.contains("csi.min_value <= $6::bytea"));
        assert!(upper.sql.contains("UNION ALL"));
        assert!(upper.sql.contains("csi.min_value IS NULL"));
        assert!(!upper.sql.contains("csi.min_value IS NULL OR"));
        assert!(!upper.sql.contains("csi.max_value >="));
        assert!(!upper.sql.contains("INDEX"));
        assert!(!upper.sql.contains("column_name"));

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

    #[test]
    fn aggregate_bounds_require_complete_active_segment_coverage() {
        let statement = plan_cold_column_aggregate_bounds().unwrap();

        assert!(statement.sql.contains("active_segment_count"));
        assert!(statement.sql.contains("indexed_segment_count"));
        assert!(statement.sql.contains("unknown_bound_count"));
        assert!(statement.sql.contains("cs.status = 'active'"));
        assert!(statement.sql.contains("count(*) FILTER"));
        assert!(statement.sql.contains("ORDER BY csi2.min_value ASC"));
        assert!(statement.sql.contains("ORDER BY csi2.max_value DESC"));
        assert!(!statement.sql.contains("AS MATERIALIZED"));
        assert!(!statement.sql.contains("min(min_value)"));
        assert!(!statement.sql.contains("max(max_value)"));
        assert!(!statement.sql.contains("row_group_min_values"));
        assert!(!statement.sql.contains("row_group_max_values"));
        assert_eq!(
            statement.param_types,
            vec![
                SqlParamType::Oid,
                SqlParamType::Text,
                SqlParamType::Integer,
                SqlParamType::Oid,
                SqlParamType::Integer,
            ]
        );
    }

    #[test]
    fn planner_hint_avoids_segment_payload_and_credentials() {
        let statement = plan_published_manifest_planner_hint().unwrap();

        assert!(statement.sql.contains("m.generation::bigint"));
        assert!(statement.sql.contains("cs.status = 'active'"));
        assert!(statement.sql.contains("count(cs.segment_id)::bigint"));
        assert!(statement.sql.contains("GROUP BY m.generation"));
        assert!(!statement.sql.contains("credentials"));
        assert!(!statement.sql.contains("physical_names"));
        assert!(!statement.sql.contains("jsonb_agg"));
        assert!(!statement.sql.contains("base_path"));
        assert_eq!(statement.param_types, vec![SqlParamType::Oid]);
    }

    #[test]
    fn candidate_sql_never_forces_both_indexes() {
        for statement in [
            plan_cold_segment_candidates_closed_range().unwrap(),
            plan_cold_segment_candidates_lower_bound().unwrap(),
            plan_cold_segment_candidates_upper_bound().unwrap(),
        ] {
            assert!(!statement.sql.contains("BitmapAnd"));
            assert!(!statement.sql.contains("IndexScan"));
            assert!(!statement.sql.contains("FORCE"));
            assert!(!statement.sql.contains("column_name"));
        }
    }

    #[test]
    fn async_managed_relation_projects_primary_key_names() {
        let statement = plan_async_managed_relation_by_oid().unwrap();
        assert!(statement.sql.contains("jsonb_agg(elem->>'name'"));
        assert!(statement
            .sql
            .contains("jsonb_array_elements(s.primary_key)"));
        assert!(!statement.sql.contains("'primary_key', s.primary_key"));
        assert!(
            statement.sql.contains("a.atttypid"),
            "order-column type must come from pg_attribute, not missing columns.type_oid"
        );
        assert!(!statement.sql.contains("c->>'type_oid'"));
    }

    #[test]
    fn publishable_manifest_rows_join_cold_segment_index() {
        let statement = plan_publishable_cold_segments_for_manifest_json().unwrap();
        assert!(statement.sql.contains("koldstore.cold_segment_index"));
        assert!(statement.sql.contains("'index_bounds'"));
        assert!(statement.sql.contains("encode(i.min_value, 'hex')"));
        assert!(statement.sql.contains("'row_group_min_values'"));
        assert!(statement.sql.contains("'row_group_null_counts'"));
        assert!(!statement.sql.contains("'column_stats'"));
        assert!(!statement.sql.contains("cs.column_stats"));
    }
}
