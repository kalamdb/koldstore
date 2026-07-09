//! Flush catalog SQL plans for cold segments and manifest rows.
//!
//! Manifest assembly and filesystem I/O live in `koldstore-manifest`. This
//! module owns parameterized catalog write plans only. SPI execution stays in
//! `pg_koldstore`.

use koldstore_catalog::HintKind;
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

/// Plans a combined multi-row `koldstore.cold_segments` + `koldstore.cold_pk_hints`
/// insert for every segment written by one `flush_table` call.
///
/// PERFORMANCE: one SPI round trip for the *entire* flush instead of one (or two)
/// round trips per segment. Per-segment columns are bound as native PostgreSQL
/// arrays and expanded with `unnest`.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_flush_segments_batch_insert() -> Result<SqlStatement, SegmentCatalogError> {
    let exact = HintKind::Exact.as_str();
    SqlStatement::write(
        "flush insert cold segments and pk hints batch",
        &format!(
            r#"
WITH inserted_segments AS (
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
    RETURNING segment_id, object_path, max_seq, max_commit_seq
)
INSERT INTO koldstore.cold_pk_hints (
    table_oid,
    scope_key,
    pk_hash,
    segment_id,
    hint_kind,
    latest_seq,
    latest_commit_seq
)
SELECT $1::oid, '', decode(md5(inserted_segments.object_path), 'hex'), inserted_segments.segment_id, '{exact}', inserted_segments.max_seq, inserted_segments.max_commit_seq
FROM inserted_segments
ON CONFLICT DO NOTHING
"#
        ),
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
