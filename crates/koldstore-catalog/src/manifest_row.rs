//! Wire type for cold-segment rows used to assemble `manifest.json`.

use serde::Deserialize;

/// Catalog row shape used to rebuild a shared-scope object-store manifest.
///
/// Produced by [`crate::queries::plan_publishable_cold_segments_for_manifest_json`]
/// (and related SPI). Assembly into [`koldstore_manifest::Manifest`] stays in
/// `koldstore-manifest`.
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
