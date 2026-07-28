//! Wire type for cold-segment rows used to assemble `manifest.json`.

use serde::Deserialize;

/// One Sort Key V1 bound row loaded for manifest export.
///
/// Produced by joining `koldstore.cold_segment_index` when assembling
/// `manifest.json`. Bounds are hex-encoded Storekey bytes (`encode(..., 'hex')`).
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CatalogSegmentIndexBound {
    /// Stable source-column ID (`pg_attribute.attnum`).
    pub column_id: i16,
    /// PostgreSQL type OID used to decode the Sort Key bytes.
    pub type_oid: u32,
    /// Persisted Sort Key codec version.
    pub codec_version: i16,
    /// Inclusive lower bound (hex-encoded Sort Key V1 bytes).
    pub min_value: String,
    /// Inclusive upper bound (hex-encoded Sort Key V1 bytes).
    pub max_value: String,
}

/// Catalog row shape used to rebuild a shared-scope object-store manifest.
///
/// Produced by [`crate::queries::plan_publishable_cold_segments_for_manifest_json`]
/// (and related SPI). Assembly into [`koldstore_manifest::Manifest`] stays in
/// `koldstore-manifest`. Segment column stats are derived from [`index_bounds`],
/// not a duplicated JSON column on `cold_segments`.
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
    /// Sort Key index rows for this segment (empty when none were indexed).
    #[serde(default)]
    pub index_bounds: Vec<CatalogSegmentIndexBound>,
}
