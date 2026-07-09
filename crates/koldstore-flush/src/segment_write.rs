//! Cold-segment file writes and manifest assembly for one flush chunk.
//!
//! Owns PG-free object-path planning, Parquet emission, and manifest segment
//! construction. Catalog SPI inserts stay in `pg_koldstore`.

use std::path::PathBuf;

use koldstore_manifest::ManifestSegment;
use koldstore_parquet::write_parquet_segment_file;

use crate::segment_catalog::{
    build_manifest_segment_from_catalog_row, indexed_column_stats_json, CatalogManifestSegmentRow,
};
use crate::stats::FlushStats;
use crate::write::FlushWriteChunk;

/// One cold segment written to the object-store mount.
#[derive(Debug, Clone, PartialEq)]
pub struct WrittenFlushSegment {
    /// New segment id for catalog inserts.
    pub segment_id: uuid::Uuid,
    /// Relative object path under the table prefix.
    pub object_path: String,
    /// Final on-disk byte size.
    pub byte_size: i64,
    /// Column stats JSON stored in `koldstore.cold_segments`.
    pub column_stats: serde_json::Value,
    /// Catalog row shape for manifest assembly.
    pub catalog_row: CatalogManifestSegmentRow,
    /// Manifest segment appended after flush.
    pub manifest_segment: ManifestSegment,
}

/// Writes one Parquet segment file and assembles manifest/catalog metadata.
///
/// # Errors
///
/// Returns an error when directories cannot be created, Parquet encoding fails,
/// or manifest assembly fails.
#[allow(clippy::too_many_arguments)]
pub fn write_flush_segment_file(
    namespace: &str,
    table_name: &str,
    base_path: &str,
    compression: &str,
    primary_key_columns: &[String],
    indexed_columns: &[String],
    schema_version: i32,
    batch_number: i32,
    chunk: &FlushWriteChunk,
    chunk_stats: &FlushStats,
) -> Result<WrittenFlushSegment, String> {
    let prefix = format!("{namespace}/{table_name}");
    let object_path = format!("{prefix}/batch-{batch_number}.parquet");
    let absolute_segment_path = PathBuf::from(base_path).join(&object_path);
    // Parent directory is created once per table flush by the caller when possible;
    // keep create_dir_all here as a safe fallback for the first segment.
    if let Some(parent) = absolute_segment_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }

    let column_stats = indexed_column_stats_json(&chunk.cold_batch.indexed_bounds, chunk_stats);
    let byte_size = write_parquet_segment_file(
        &absolute_segment_path,
        &chunk.cold_batch.batch,
        primary_key_columns,
        indexed_columns,
        compression,
    )?;
    let catalog_row = CatalogManifestSegmentRow {
        object_path: object_path.clone(),
        batch_number,
        min_seq: chunk_stats.min_seq,
        max_seq: chunk_stats.max_seq,
        min_commit_seq: chunk_stats.min_commit_seq,
        max_commit_seq: chunk_stats.max_commit_seq,
        row_count: chunk_stats.row_count,
        byte_size,
        schema_version,
        column_stats: column_stats.clone(),
    };
    let manifest_segment = build_manifest_segment_from_catalog_row(
        namespace,
        table_name,
        primary_key_columns,
        catalog_row.clone(),
    )
    .map_err(|error| error.to_string())?;

    Ok(WrittenFlushSegment {
        segment_id: uuid::Uuid::new_v4(),
        object_path,
        byte_size,
        column_stats,
        catalog_row,
        manifest_segment,
    })
}
