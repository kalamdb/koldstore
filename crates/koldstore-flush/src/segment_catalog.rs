//! Flush catalog SQL plans for cold segments and manifest rows.
//!
//! Manifest assembly and filesystem I/O live in `koldstore-manifest`. This
//! module owns parameterized catalog write plans only. SPI execution stays in
//! `pg_koldstore`.

use koldstore_common::SqlStatement;
use koldstore_manifest::SyncState;
use koldstore_parquet::ColdMetadataColumn;
use thiserror::Error;

use crate::stats::FlushStats;

pub use koldstore_manifest::{
    build_manifest_segment_from_catalog_row, load_manifest_from_path, manifest_from_catalog_rows,
    write_manifest_to_path, CatalogManifestSegmentRow, ManifestAssemblyError,
};

/// Flush catalog planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SegmentCatalogError {
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
    /// Manifest assembly failed.
    #[error("{0}")]
    Manifest(String),
}

impl From<ManifestAssemblyError> for SegmentCatalogError {
    fn from(error: ManifestAssemblyError) -> Self {
        Self::Manifest(error.to_string())
    }
}

/// Builds indexed column stats JSON for one flushed segment chunk.
#[must_use]
pub fn indexed_column_stats_json(
    indexed_bounds: &std::collections::BTreeMap<String, (serde_json::Value, serde_json::Value)>,
    stats: &FlushStats,
) -> serde_json::Value {
    let mut values = serde_json::Map::new();
    values.insert(
        ColdMetadataColumn::Seq.name().to_string(),
        serde_json::json!({"min": stats.min_seq, "max": stats.max_seq}),
    );
    for (column, bounds) in indexed_bounds {
        values.insert(
            column.clone(),
            serde_json::json!({
                "min": bounds.0,
                "max": bounds.1,
            }),
        );
    }
    serde_json::Value::Object(values)
}

/// Plans combined multi-row segment and normalized-stat inserts.
///
/// Segment prune metadata lives in `koldstore.cold_segment_stats` (and the
/// mirrored `column_stats` jsonb). Exact per-PK catalog rows are not written.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_flush_segments_batch_insert() -> Result<SqlStatement, SegmentCatalogError> {
    SqlStatement::write(
        "flush insert cold segments batch",
        r#"
WITH segment_input AS (
    SELECT *
    FROM unnest(
        $2::uuid[],
        $3::text[],
        $4::integer[],
        $5::bigint[],
        $6::bigint[],
        $7::bigint[],
        $8::bigint[],
        $9::bigint[],
        $10::bigint[],
        $11::integer[],
        $12::jsonb[]
    ) AS u(
        segment_id,
        object_path,
        batch_number,
        min_seq,
        max_seq,
        min_commit_seq,
        max_commit_seq,
        row_count,
        byte_size,
        schema_version,
        column_stats
    )
),
inserted_segments AS (
    INSERT INTO koldstore.cold_segments (
        segment_id,
        table_oid,
        scope_key,
        object_path,
        batch_number,
        min_seq,
        max_seq,
        min_commit_seq,
        max_commit_seq,
        row_count,
        byte_size,
        schema_version,
        column_stats,
        status
    )
    SELECT
        u.segment_id,
        $1::oid,
        '',
        u.object_path,
        u.batch_number,
        u.min_seq,
        u.max_seq,
        u.min_commit_seq,
        u.max_commit_seq,
        u.row_count,
        u.byte_size,
        u.schema_version,
        u.column_stats,
        'active'
    FROM segment_input u
    RETURNING segment_id, table_oid, scope_key, column_stats
)
INSERT INTO koldstore.cold_segment_stats (
    segment_id,
    table_oid,
    scope_key,
    column_name,
    type_oid,
    min_value,
    max_value
)
SELECT
    cs.segment_id,
    cs.table_oid,
    cs.scope_key,
    stat.column_name::name,
    COALESCE(
        (
            SELECT attribute.atttypid
            FROM pg_catalog.pg_attribute attribute
            WHERE attribute.attrelid = cs.table_oid
              AND attribute.attname = stat.column_name::name
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
        ),
        'pg_catalog.int8'::regtype::oid
    ),
    pg_catalog.convert_to((stat.bounds->'min')::text, 'UTF8'),
    pg_catalog.convert_to((stat.bounds->'max')::text, 'UTF8')
FROM inserted_segments cs
CROSS JOIN LATERAL pg_catalog.jsonb_each(cs.column_stats)
    AS stat(column_name, bounds)
ON CONFLICT (segment_id, column_name)
DO UPDATE SET
    table_oid = EXCLUDED.table_oid,
    scope_key = EXCLUDED.scope_key,
    type_oid = EXCLUDED.type_oid,
    min_value = EXCLUDED.min_value,
    max_value = EXCLUDED.max_value
"#,
    )
    .map_err(|error| SegmentCatalogError::Sql(error.to_string()))
}

/// Plans `koldstore.manifest` upsert after flush finalization.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_manifest_row_upsert() -> Result<SqlStatement, SegmentCatalogError> {
    let in_sync = SyncState::InSync.as_str();
    SqlStatement::write(
        "flush upsert manifest row",
        &format!(
            r#"
INSERT INTO koldstore.manifest (
    table_oid,
    scope_key,
    manifest_path,
    etag,
    generation,
    sync_state,
    segment_count,
    max_seq,
    max_commit_seq,
    last_error,
    updated_at
)
VALUES ($1::oid, '', $2::text, NULL, $3::text, '{in_sync}', $4::integer, $5::bigint, $6::bigint, NULL, now())
ON CONFLICT (table_oid, scope_key)
DO UPDATE SET
    manifest_path = EXCLUDED.manifest_path,
    generation = EXCLUDED.generation,
    sync_state = '{in_sync}',
    segment_count = EXCLUDED.segment_count,
    max_seq = EXCLUDED.max_seq,
    max_commit_seq = EXCLUDED.max_commit_seq,
    last_error = NULL,
    updated_at = now()
"#
        ),
    )
    .map_err(|error| SegmentCatalogError::Sql(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::plan_flush_segments_batch_insert;

    #[test]
    fn flush_insert_persists_normalized_stats_without_pk_hints() {
        let statement = plan_flush_segments_batch_insert().unwrap();

        assert!(statement.sql.contains("koldstore.cold_segment_stats"));
        assert!(statement.sql.contains("jsonb_each"));
        assert!(!statement.sql.contains("koldstore.cold_pk_hints"));
        assert!(!statement.sql.contains("md5(inserted_segments.object_path)"));
    }
}
