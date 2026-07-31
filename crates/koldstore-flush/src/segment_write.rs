//! Cold-segment file writes and manifest assembly for one flush chunk.
//!
//! Owns PG-free object-path planning, Parquet encoding, durable object publish,
//! and manifest segment construction. Catalog SPI inserts stay in `pg_koldstore`.

use koldstore_catalog::{CatalogManifestSegmentRow, CatalogSegmentIndexBound};
use koldstore_manifest::{segment_path_token, segment_relative_object_path};
use koldstore_parquet::{validate_finalized_parquet_segment, PackedSegmentMetadata};
use koldstore_storage::{
    join_object_key, publish_immutable_object, temp_object_key, unique_temp_file_name,
    ObjectStoreClient,
};
use uuid::Uuid;

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
    /// Full object key under the table prefix used for publication.
    pub object_path: String,
    /// Sha256 hex of the published Parquet bytes.
    pub checksum: String,
    /// Optional object-store etag from publish.
    pub object_etag: Option<String>,
    /// Compact footer-derived segment and row-group metadata.
    pub packed_metadata: PackedSegmentMetadata,
    /// Catalog row shape for manifest assembly (relative `path`, sizes, seq bounds).
    pub catalog_row: CatalogManifestSegmentRow,
}

/// Builds the table-relative object key for one flush segment.
#[must_use]
pub fn flush_segment_relative_path(batch_number: i32, segment_id: Uuid) -> String {
    segment_relative_object_path(batch_number, segment_path_token(segment_id))
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
pub fn write_flush_segment_with_client(
    client: &ObjectStoreClient,
    table_prefix: &str,
    schema_version: i32,
    batch_number: i32,
    chunk: &FlushWriteChunk,
    chunk_stats: &FlushStats,
) -> Result<WrittenFlushSegment, String> {
    let prefix = table_prefix.trim_matches('/');
    // Allocate the segment id before publish so the final key is unique per
    // write attempt. Retries after abort must not reuse an orphaned object.
    let segment_id = Uuid::new_v4();
    let relative_path = flush_segment_relative_path(batch_number, segment_id);
    let object_path = join_object_key(prefix, &relative_path);
    // Flat temp under `{prefix}/.tmp/` — uniqueness is in the file name UUID.
    // Avoids leaving empty per-attempt directories after temp cleanup.
    let temp_key = temp_object_key(
        prefix,
        "",
        &unique_temp_file_name(&format!(
            "segment-{batch_number:04}-{}.parquet",
            segment_path_token(segment_id)
        )),
    );

    let bytes = &chunk.parquet_bytes;
    let validation = validate_finalized_parquet_segment(bytes, chunk.parquet_metadata.as_ref())?;
    let expected_rows = u64::try_from(chunk_stats.row_count.max(0)).unwrap_or(0);
    if validation.row_count != expected_rows {
        return Err(format!(
            "parquet row count {} does not match flush chunk stats {}",
            validation.row_count, chunk_stats.row_count
        ));
    }
    if validation.row_group_count != chunk.packed_metadata.row_group_count {
        return Err(format!(
            "finalized Parquet metadata has {} row groups but packed metadata has {}",
            validation.row_group_count, chunk.packed_metadata.row_group_count
        ));
    }

    // Publish verifies byte identity and returns checksum from this same buffer.
    let published = publish_immutable_object(client, &temp_key, &object_path, bytes)
        .map_err(|error| error.to_string())?;

    let byte_size = i64::try_from(published.byte_size).map_err(|error| error.to_string())?;
    let catalog_row = CatalogManifestSegmentRow {
        segment_id: segment_id.to_string(),
        path: relative_path,
        batch_number,
        min_seq: chunk_stats.min_seq,
        max_seq: chunk_stats.max_seq,
        min_commit_seq: chunk_stats.min_commit_seq,
        max_commit_seq: chunk_stats.max_commit_seq,
        row_count: chunk_stats.row_count,
        byte_size,
        schema_version,
        row_group_count: i32::try_from(chunk.packed_metadata.row_group_count)
            .map_err(|error| error.to_string())?,
        row_group_row_counts: chunk.packed_metadata.row_group_row_counts.clone(),
        row_group_min_seqs: chunk.packed_metadata.row_group_min_seqs.clone(),
        row_group_max_seqs: chunk.packed_metadata.row_group_max_seqs.clone(),
        status: "pending".to_string(),
        checksum: published.checksum.clone(),
        object_etag: published.etag.clone(),
        created_at: None,
        index_bounds: chunk
            .packed_metadata
            .column_indexes
            .iter()
            .map(|index| CatalogSegmentIndexBound {
                column_id: index.column_id.get(),
                type_oid: index.type_oid,
                codec_version: index.codec_version,
                min_value: index.min_value.as_deref().map(hex::encode),
                max_value: index.max_value.as_deref().map(hex::encode),
                row_group_min_values: index
                    .row_group_min_values
                    .iter()
                    .map(|value| value.as_deref().map(hex::encode))
                    .collect(),
                row_group_max_values: index
                    .row_group_max_values
                    .iter()
                    .map(|value| value.as_deref().map(hex::encode))
                    .collect(),
                row_group_null_counts: index.row_group_null_counts.clone(),
            })
            .collect(),
    };

    Ok(WrittenFlushSegment {
        segment_id,
        object_path,
        checksum: published.checksum,
        object_etag: published.etag,
        packed_metadata: chunk.packed_metadata.clone(),
        catalog_row,
    })
}
