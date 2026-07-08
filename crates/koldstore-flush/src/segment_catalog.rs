//! Flush catalog SQL plans and manifest assembly from catalog rows.
//!
//! Owns PG-free manifest construction and parameterized catalog write plans.
//! SPI execution stays in `pg_koldstore`.

use std::collections::BTreeMap;

use koldstore_common::SqlStatement;
use koldstore_manifest::{
    Manifest, ManifestBloomFilter, ManifestColumnStats, ManifestSegment, PkFilter,
};
use serde::Deserialize;
use thiserror::Error;

/// Catalog row shape returned by [`plan_active_cold_segments_for_manifest_json`].
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct CatalogManifestSegmentRow {
    /// Final object-store path.
    pub object_path: String,
    /// Segment batch number.
    pub batch_number: i32,
    /// Minimum `_seq`.
    pub min_seq: i64,
    /// Maximum `_seq`.
    pub max_seq: i64,
    /// Minimum `_commit_seq`.
    pub min_commit_seq: i64,
    /// Maximum `_commit_seq`.
    pub max_commit_seq: i64,
    /// Segment row count.
    pub row_count: i64,
    /// Segment byte size.
    pub byte_size: i64,
    /// Segment schema version.
    pub schema_version: i32,
    /// Segment column stats JSON.
    pub column_stats: serde_json::Value,
}

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

/// Builds a shared manifest from active catalog segment rows.
///
/// # Errors
///
/// Returns an error when segment metadata cannot be converted into manifest form.
pub fn manifest_from_catalog_rows(
    namespace: &str,
    table_name: &str,
    schema_version: u32,
    primary_key_columns: &[String],
    rows: Vec<CatalogManifestSegmentRow>,
) -> std::result::Result<Manifest, SegmentCatalogError> {
    let mut manifest = Manifest::new_shared(
        namespace.to_string(),
        table_name.to_string(),
        schema_version,
    );
    for row in rows {
        manifest.append_segment(manifest_segment_from_catalog_row(
            namespace,
            table_name,
            primary_key_columns,
            row,
        )?);
    }
    Ok(manifest)
}

fn manifest_segment_from_catalog_row(
    namespace: &str,
    table_name: &str,
    primary_key_columns: &[String],
    row: CatalogManifestSegmentRow,
) -> std::result::Result<ManifestSegment, SegmentCatalogError> {
    let manifest_path = manifest_relative_segment_path(namespace, table_name, &row.object_path);
    let mut segment = ManifestSegment::committed(
        u32::try_from(row.batch_number)
            .map_err(|error| SegmentCatalogError::Manifest(error.to_string()))?,
        manifest_path,
        row.min_seq..=row.max_seq,
        row.min_commit_seq..=row.max_commit_seq,
        u64::try_from(row.row_count)
            .map_err(|error| SegmentCatalogError::Manifest(error.to_string()))?,
        u64::try_from(row.byte_size)
            .map_err(|error| SegmentCatalogError::Manifest(error.to_string()))?,
        u32::try_from(row.schema_version)
            .map_err(|error| SegmentCatalogError::Manifest(error.to_string()))?,
    );
    segment.column_stats = manifest_column_stats(row.column_stats);
    segment.bloom_filters.push(ManifestBloomFilter::bloom(
        primary_key_columns.to_vec(),
        Some(0.01),
    ));
    segment.pk_filter = Some(PkFilter::exact(vec![1]));
    Ok(segment)
}

fn manifest_relative_segment_path(namespace: &str, table_name: &str, object_path: &str) -> String {
    let prefix = format!("{namespace}/{table_name}/");
    object_path
        .strip_prefix(&prefix)
        .unwrap_or(object_path)
        .to_string()
}

fn manifest_column_stats(column_stats: serde_json::Value) -> BTreeMap<String, ManifestColumnStats> {
    let mut stats = BTreeMap::new();
    let Some(columns) = column_stats.as_object() else {
        return stats;
    };

    for (column, value) in columns {
        let Some(min) = value.get("min") else {
            continue;
        };
        let Some(max) = value.get("max") else {
            continue;
        };
        stats.insert(
            column.clone(),
            ManifestColumnStats::new(min.clone(), max.clone()),
        );
    }

    stats
}

/// Plans `koldstore.cold_segments` insertion during inline flush.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_flush_cold_segment_insert() -> std::result::Result<SqlStatement, SegmentCatalogError> {
    SqlStatement::write(
        "flush insert cold segment",
        r#"
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
VALUES (
    $1::uuid,
    $2::oid,
    '',
    $3::text,
    $4::integer,
    $5::bigint,
    $6::bigint,
    $7::bigint,
    $8::bigint,
    $9::bigint,
    $10::bigint,
    $11::integer,
    $12::jsonb,
    'active'
)
"#,
    )
    .map_err(|error| SegmentCatalogError::Sql(error.to_string()))
}

/// Plans `koldstore.cold_pk_hints` insertion during inline flush.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_flush_pk_hint_insert() -> std::result::Result<SqlStatement, SegmentCatalogError> {
    SqlStatement::write(
        "flush insert cold pk hint",
        r#"
INSERT INTO koldstore.cold_pk_hints (
    table_oid,
    scope_key,
    pk_hash,
    segment_id,
    hint_kind,
    latest_seq,
    latest_commit_seq
)
VALUES ($1::oid, '', decode(md5($2::text), 'hex'), $3::uuid, 'exact', $4::bigint, $5::bigint)
ON CONFLICT DO NOTHING
"#,
    )
    .map_err(|error| SegmentCatalogError::Sql(error.to_string()))
}

/// Plans `koldstore.manifest` upsert after flush finalization.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_manifest_row_upsert() -> std::result::Result<SqlStatement, SegmentCatalogError> {
    SqlStatement::write(
        "flush upsert manifest row",
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
VALUES ($1::oid, '', $2::text, NULL, $3::text, 'in_sync', $4::integer, $5::bigint, $6::bigint, NULL, now())
ON CONFLICT (table_oid, scope_key)
DO UPDATE SET
    manifest_path = EXCLUDED.manifest_path,
    generation = EXCLUDED.generation,
    sync_state = 'in_sync',
    segment_count = EXCLUDED.segment_count,
    max_seq = EXCLUDED.max_seq,
    max_commit_seq = EXCLUDED.max_commit_seq,
    last_error = NULL,
    updated_at = now()
"#,
    )
    .map_err(|error| SegmentCatalogError::Sql(error.to_string()))
}
