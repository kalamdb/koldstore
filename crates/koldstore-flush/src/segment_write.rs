//! Cold-segment file writes and manifest assembly for one flush chunk.
//!
//! Owns PG-free object-path planning, Parquet encoding, durable object publish,
//! and manifest segment construction. Catalog SPI inserts stay in `pg_koldstore`.

use koldstore_manifest::{table_object_prefix, CatalogManifestSegmentRow};
use koldstore_parquet::validate_parquet_bytes;
use koldstore_storage::{
    open_filesystem_client, publish_immutable_object, temp_object_key, unique_temp_file_name,
    ObjectStoreClient,
};
use uuid::Uuid;

use crate::segment_catalog::indexed_column_stats_json;
use crate::stats::FlushStats;
use crate::write::FlushWriteChunk;

/// One cold segment written to the object-store mount.
///
/// Inserted into `koldstore.cold_segments` as `pending` until flush activate
/// CAS makes it `active`. Checksum/etag come from the single publish pass.
#[derive(Debug, Clone, PartialEq)]
pub struct WrittenFlushSegment {
    /// New segment id for catalog inserts.
    pub segment_id: uuid::Uuid,
    /// Relative object path under the table prefix.
    pub object_path: String,
    /// Final on-disk byte size.
    pub byte_size: i64,
    /// Sha256 hex of the published Parquet bytes.
    pub checksum: String,
    /// Optional object-store etag from publish.
    pub object_etag: Option<String>,
    /// Column stats JSON stored in `koldstore.cold_segments`.
    pub column_stats: serde_json::Value,
    /// Catalog row shape for manifest assembly (single source of truth).
    pub catalog_row: CatalogManifestSegmentRow,
}

/// Builds the immutable object key for one flush segment write attempt.
///
/// Keys include `segment_id` so a retry after a rolled-back flush cannot collide
/// with an orphaned final object left by the previous attempt at the same
/// `batch_number`.
#[must_use]
pub fn flush_segment_object_path(prefix: &str, batch_number: i32, segment_id: Uuid) -> String {
    let prefix = prefix.trim_matches('/');
    format!("{prefix}/batch-{batch_number}-{segment_id}.parquet")
}

/// Writes one Parquet segment via encode → validate → durable Create publish.
///
/// Final keys are never truncated in place. Crash before publish leaves at most
/// a temp object under `{prefix}/.tmp/…`; crash after publish but before activate
/// leaves a `pending` catalog row (or unreferenced final) that recovery can expire.
///
/// # Errors
///
/// Returns an error when encoding, validation, or durable publish fails.
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
    let client = open_filesystem_client(base_path).map_err(|error| error.to_string())?;
    write_flush_segment_with_client(
        &client,
        namespace,
        table_name,
        compression,
        primary_key_columns,
        indexed_columns,
        schema_version,
        batch_number,
        chunk,
        chunk_stats,
    )
}

/// Same as [`write_flush_segment_file`] but uses an existing storage client.
///
/// # Errors
///
/// Returns an error when encoding, validation, or durable publish fails.
#[allow(clippy::too_many_arguments)]
pub fn write_flush_segment_with_client(
    client: &ObjectStoreClient,
    namespace: &str,
    table_name: &str,
    _compression: &str,
    _primary_key_columns: &[String],
    _indexed_columns: &[String],
    schema_version: i32,
    batch_number: i32,
    chunk: &FlushWriteChunk,
    chunk_stats: &FlushStats,
) -> Result<WrittenFlushSegment, String> {
    let prefix = table_object_prefix(namespace, table_name);
    // Allocate the segment id before publish so the final key is unique per
    // write attempt. Retries after abort must not reuse an orphaned object.
    let segment_id = Uuid::new_v4();
    let object_path = flush_segment_object_path(&prefix, batch_number, segment_id);
    let writer_id = Uuid::new_v4().to_string();
    let temp_key = temp_object_key(
        &prefix,
        &writer_id,
        &unique_temp_file_name(&format!("batch-{batch_number}-{segment_id}.parquet")),
    );

    let bytes = &chunk.parquet_bytes;
    let validation = validate_parquet_bytes(bytes)?;
    let expected_rows = u64::try_from(chunk_stats.row_count.max(0)).unwrap_or(0);
    if validation.row_count != expected_rows {
        return Err(format!(
            "parquet row count {} does not match flush chunk stats {}",
            validation.row_count, chunk_stats.row_count
        ));
    }

    // Publish verifies byte identity and returns checksum from this same buffer.
    let published = publish_immutable_object(client, &temp_key, &object_path, bytes)
        .map_err(|error| error.to_string())?;

    let column_stats = indexed_column_stats_json(&chunk.indexed_bounds, chunk_stats);
    let byte_size = i64::try_from(published.byte_size).map_err(|error| error.to_string())?;
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

    Ok(WrittenFlushSegment {
        segment_id,
        object_path,
        byte_size,
        checksum: published.checksum,
        object_etag: published.etag,
        column_stats,
        catalog_row,
    })
}
