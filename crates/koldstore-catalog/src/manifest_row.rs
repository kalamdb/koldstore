//! Wire type for cold-segment rows used to assemble `manifest.json`.

use serde::{Deserialize, Serialize};

/// One Sort Key V1 bound row loaded for manifest export.
///
/// Produced by joining `koldstore.cold_segment_index` when assembling
/// `manifest.json`. Bounds are hex-encoded Storekey bytes (`encode(..., 'hex')`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogSegmentIndexBound {
    /// Stable source-column ID (`pg_attribute.attnum`).
    pub column_id: i16,
    /// PostgreSQL type OID used to decode the Sort Key bytes.
    pub type_oid: u32,
    /// Persisted Sort Key codec version.
    pub codec_version: i16,
    /// Inclusive lower bound (hex-encoded Sort Key V1 bytes).
    pub min_value: Option<String>,
    /// Inclusive upper bound (hex-encoded Sort Key V1 bytes).
    pub max_value: Option<String>,
    /// Per-row-group inclusive lower bounds as optional hex Sort Key bytes.
    pub row_group_min_values: Vec<Option<String>>,
    /// Per-row-group inclusive upper bounds as optional hex Sort Key bytes.
    pub row_group_max_values: Vec<Option<String>>,
    /// Per-row-group null counts; `None` means statistics are unknown.
    pub row_group_null_counts: Vec<Option<i64>>,
}

/// Catalog row shape used to rebuild a shared-scope object-store manifest.
///
/// Produced by [`crate::queries::plan_publishable_cold_segments_for_manifest_json`]
/// (and related SPI). Assembly into [`koldstore_manifest::Manifest`] stays in
/// `koldstore-manifest`. Segment column stats are derived from [`index_bounds`],
/// not a duplicated JSON column on `cold_segments`.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct CatalogManifestSegmentRow {
    /// Stable cold-segment UUID.
    pub segment_id: String,
    /// Final object-store path.
    pub path: String,
    /// Segment batch number.
    pub batch_number: i32,
    /// Minimum `_seq`.
    pub min_seq: i64,
    /// Maximum `_seq`.
    pub max_seq: i64,
    /// Segment row count.
    pub row_count: i64,
    /// Segment byte size.
    pub byte_size: i64,
    /// Segment schema version.
    pub schema_version: i32,
    /// Number of Parquet row groups.
    pub row_group_count: i32,
    /// Logical row counts aligned by row-group ID.
    pub row_group_row_counts: Vec<i64>,
    /// Minimum SeqIds aligned by row-group ID.
    pub row_group_min_seqs: Vec<i64>,
    /// Maximum SeqIds aligned by row-group ID.
    pub row_group_max_seqs: Vec<i64>,
    /// Catalog publication state.
    pub status: String,
    /// Segment content checksum.
    pub checksum: String,
    /// Optional object-store etag.
    pub object_etag: Option<String>,
    /// Catalog creation timestamp rendered as RFC 3339 text.
    pub created_at: Option<String>,
    /// Sort Key index rows for this segment (empty when none were indexed).
    #[serde(default)]
    pub index_bounds: Vec<CatalogSegmentIndexBound>,
}
