//! Assemble a [`Manifest`] from cold-segment catalog rows.
//!
//! Catalog SQL and the [`CatalogManifestSegmentRow`] wire type live in
//! `koldstore-catalog`. This module owns the pure conversion into the on-disk
//! manifest model.

use std::collections::BTreeMap;

use koldstore_catalog::{column_stats_min_max_map_into, CatalogManifestSegmentRow};
use thiserror::Error;

use crate::model::{Manifest, ManifestBloomFilter, ManifestColumnStats, ManifestSegment, PkFilter};

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
    primary_key_columns: &[String],
    rows: Vec<CatalogManifestSegmentRow>,
) -> Result<Manifest, ManifestAssemblyError> {
    let mut manifest = Manifest::new_shared(
        namespace.to_string(),
        table_name.to_string(),
        schema_version,
    );
    let segments = rows
        .into_iter()
        .map(|row| {
            build_manifest_segment_from_catalog_row(namespace, table_name, primary_key_columns, row)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let _ = manifest.append_segment_batch(segments);
    Ok(manifest)
}

/// Builds one manifest segment from an active cold-segment catalog row.
///
/// # Errors
///
/// Returns an error when segment metadata cannot be converted into manifest form.
pub fn build_manifest_segment_from_catalog_row(
    namespace: &str,
    table_name: &str,
    primary_key_columns: &[String],
    row: CatalogManifestSegmentRow,
) -> Result<ManifestSegment, ManifestAssemblyError> {
    let manifest_path = manifest_relative_segment_path(namespace, table_name, &row.object_path);
    let mut segment = ManifestSegment::committed(
        u32::try_from(row.batch_number)
            .map_err(|error| ManifestAssemblyError::InvalidSegment(error.to_string()))?,
        manifest_path,
        row.min_seq..=row.max_seq,
        row.min_commit_seq..=row.max_commit_seq,
        u64::try_from(row.row_count)
            .map_err(|error| ManifestAssemblyError::InvalidSegment(error.to_string()))?,
        u64::try_from(row.byte_size)
            .map_err(|error| ManifestAssemblyError::InvalidSegment(error.to_string()))?,
        u32::try_from(row.schema_version)
            .map_err(|error| ManifestAssemblyError::InvalidSegment(error.to_string()))?,
    );
    segment.column_stats = manifest_column_stats(row.column_stats);
    if !primary_key_columns.is_empty() {
        segment.bloom_filters.push(ManifestBloomFilter::bloom(
            primary_key_columns.to_vec(),
            Some(0.01),
        ));
        let column_ids = (1..=primary_key_columns.len() as u32).collect::<Vec<_>>();
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

fn manifest_column_stats(column_stats: serde_json::Value) -> BTreeMap<String, ManifestColumnStats> {
    column_stats_min_max_map_into(column_stats)
        .into_iter()
        .map(|(column, (min, max))| (column, ManifestColumnStats::new(min, max)))
        .collect()
}
