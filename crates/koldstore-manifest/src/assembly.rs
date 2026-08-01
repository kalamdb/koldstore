//! Assemble a [`Manifest`] from cold-segment catalog rows.
//!
//! Catalog SQL and the [`CatalogManifestSegmentRow`] wire type live in
//! `koldstore-catalog`. This module owns the pure conversion into the on-disk
//! manifest model.

use koldstore_catalog::CatalogManifestSegmentRow;
use koldstore_common::ColumnRef;
use thiserror::Error;

use crate::model::{
    Manifest, ManifestBloomFilter, ManifestColumnIndex, ManifestSegment, PkFilter, SegmentStatus,
};

/// Manifest assembly error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ManifestAssemblyError {
    /// Segment metadata could not be converted into manifest form.
    #[error("{0}")]
    InvalidSegment(String),
}

/// Builds a shared manifest from active catalog segment rows.
///
/// Uses one reserved append batch so watermarks update once.
///
/// # Errors
///
/// Returns an error when segment metadata cannot be converted into manifest form.
pub fn manifest_from_catalog_rows(
    namespace: &str,
    table_name: &str,
    schema_version: u32,
    primary_key_columns: &[ColumnRef],
    rows: Vec<CatalogManifestSegmentRow>,
) -> Result<Manifest, ManifestAssemblyError> {
    let mut manifest = Manifest::new_shared(
        namespace.to_string(),
        table_name.to_string(),
        schema_version,
    );
    let segments = rows
        .into_iter()
        .map(|row| build_manifest_segment_from_catalog_row(primary_key_columns, row))
        .collect::<Result<Vec<_>, _>>()?;
    let _ = manifest.append_segment_batch(segments);
    Ok(manifest)
}

/// Builds one manifest segment from an active cold-segment catalog row.
///
/// Packed Sort Key bounds are copied from `cold_segment_index` rows carried on
/// [`CatalogManifestSegmentRow::index_bounds`].
///
/// # Errors
///
/// Returns an error when segment metadata cannot be converted into manifest form.
pub fn build_manifest_segment_from_catalog_row(
    primary_key_columns: &[ColumnRef],
    row: CatalogManifestSegmentRow,
) -> Result<ManifestSegment, ManifestAssemblyError> {
    let row_group_count = usize::try_from(row.row_group_count)
        .map_err(|error| ManifestAssemblyError::InvalidSegment(error.to_string()))?;
    if row_group_count == 0
        || row.row_group_row_counts.len() != row_group_count
        || row.row_group_min_seqs.len() != row_group_count
        || row.row_group_max_seqs.len() != row_group_count
    {
        return Err(ManifestAssemblyError::InvalidSegment(format!(
            "segment {} has malformed row-group arrays",
            row.segment_id
        )));
    }
    for index in &row.index_bounds {
        if index.row_group_min_values.len() != row_group_count
            || index.row_group_max_values.len() != row_group_count
            || index.row_group_null_counts.len() != row_group_count
        {
            return Err(ManifestAssemblyError::InvalidSegment(format!(
                "segment {} column {} has malformed row-group arrays",
                row.segment_id, index.column_id
            )));
        }
    }
    let mut segment = ManifestSegment::committed(
        u32::try_from(row.batch_number)
            .map_err(|error| ManifestAssemblyError::InvalidSegment(error.to_string()))?,
        row.path,
        row.min_seq..=row.max_seq,
        u64::try_from(row.row_count)
            .map_err(|error| ManifestAssemblyError::InvalidSegment(error.to_string()))?,
        u64::try_from(row.byte_size)
            .map_err(|error| ManifestAssemblyError::InvalidSegment(error.to_string()))?,
        u32::try_from(row.schema_version)
            .map_err(|error| ManifestAssemblyError::InvalidSegment(error.to_string()))?,
    );
    segment.segment_id = Some(row.segment_id);
    segment.row_group_count = u32::try_from(row_group_count)
        .map_err(|error| ManifestAssemblyError::InvalidSegment(error.to_string()))?;
    segment.row_group_row_counts = row.row_group_row_counts;
    segment.row_group_min_seqs = row.row_group_min_seqs;
    segment.row_group_max_seqs = row.row_group_max_seqs;
    segment.column_indexes = row
        .index_bounds
        .into_iter()
        .map(|index| ManifestColumnIndex {
            column_id: index.column_id,
            type_oid: index.type_oid,
            codec_version: index.codec_version,
            min_value: index.min_value,
            max_value: index.max_value,
            row_group_min_values: index.row_group_min_values,
            row_group_max_values: index.row_group_max_values,
            row_group_null_counts: index.row_group_null_counts,
        })
        .collect();
    segment.status = match row.status.as_str() {
        "pending" => SegmentStatus::Pending,
        "active" => SegmentStatus::Active,
        "compacted" => SegmentStatus::Compacted,
        "deleted" => SegmentStatus::Deleted,
        other => {
            return Err(ManifestAssemblyError::InvalidSegment(format!(
                "segment has unsupported status `{other}`"
            )))
        }
    };
    segment.checksum = Some(row.checksum);
    segment.etag = row.object_etag;
    segment.created_at = row
        .created_at
        .map(|value| {
            chrono::DateTime::parse_from_rfc3339(&value)
                .map(|timestamp| timestamp.with_timezone(&chrono::Utc))
                .map_err(|error| ManifestAssemblyError::InvalidSegment(error.to_string()))
        })
        .transpose()?;
    if !primary_key_columns.is_empty() {
        let column_ids = primary_key_columns
            .iter()
            .map(|column| column.column_id.get())
            .collect::<Vec<_>>();
        segment
            .bloom_filters
            .push(ManifestBloomFilter::bloom(column_ids.clone(), Some(0.01)));
        segment.pk_filter.replace(PkFilter::exact(column_ids));
    }
    Ok(segment)
}

/// Strips `{namespace}/{table}/` from an object path when present.
#[must_use]
pub fn manifest_relative_segment_path(
    namespace: &str,
    table_name: &str,
    object_path: &str,
) -> String {
    let prefix = format!("{namespace}/{table_name}/");
    object_path
        .strip_prefix(&prefix)
        .unwrap_or(object_path)
        .to_string()
}
