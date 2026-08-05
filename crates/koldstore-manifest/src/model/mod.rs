//! Manifest serialized model types.
//!
//! Object-store layout is folder-sharded only: a thin root `manifest.json`
//! lists [`ManifestShardRef`] entries; segment bodies live in
//! `{folder}/manifest-shard-{sha256-prefix}.json`. The root retains the complete
//! digest. The in-memory [`Manifest`] may hold a full `segments` list while
//! assembling from catalog or after a merged load.

use std::ops::RangeInclusive;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Root `manifest.json` document version (folder-sharded layout).
pub const MANIFEST_VERSION: u32 = 2;
/// Shard document format version for content-addressed folder shards.
pub const MANIFEST_SHARD_VERSION: u32 = 2;

/// Object-store / in-memory manifest.
///
/// On disk, roots are version [`MANIFEST_VERSION`] with `shards` and no segment
/// bodies. In memory, `segments` holds the working list during assembly and
/// after [`crate::io::try_load_manifest_with_client`] merges shards.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub table: String,
    pub namespace: Option<String>,
    pub scope_id: Option<String>,
    pub schema_version: u32,
    pub max_seq: i64,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish: Option<PublishState>,
    /// Folder shard index written on every root export.
    pub shards: Vec<ManifestShardRef>,
    /// Working / merged segment list. Omitted from root object-store JSON.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub segments: Vec<ManifestSegment>,
    pub files: FilesState,
}

/// Pointer from a root manifest to one folder shard file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestShardRef {
    /// Zero-padded folder name (`001`).
    pub folder: String,
    /// Table-relative content-addressed shard path.
    pub path: String,
    /// SHA-256 of the exact shard JSON bytes written before this root.
    pub content_sha256: String,
    pub segment_count: u32,
    pub min_seq: i64,
    pub max_seq: i64,
}

/// Per-folder shard document written beside cold segment objects.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestShard {
    pub version: u32,
    pub folder: String,
    pub table: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub schema_version: u32,
    pub segments: Vec<ManifestSegment>,
}

/// Result of applying a batch of manifest segment appends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestBatchAppend {
    /// Number of segment entries appended.
    pub appended_segments: usize,
    /// Number of object-store root+shard publish cycles needed for the batch.
    pub manifest_writes_required: usize,
}

/// Backend-specific publish metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishState {
    pub generation: Option<String>,
    pub etag: Option<String>,
    pub backend: Option<String>,
    pub writer_id: Option<String>,
}

/// Manifest segment entry (shard document / in-memory).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestSegment {
    /// Catalog segment UUID when assembled from PostgreSQL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_id: Option<String>,
    pub batch: u32,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temp_path: Option<String>,
    pub min_seq: i64,
    pub max_seq: i64,
    pub row_count: u64,
    pub byte_size: u64,
    pub schema_version: u32,
    /// Number of positionally aligned Parquet row groups.
    pub row_group_count: u32,
    /// Logical row count for each row group.
    pub row_group_row_counts: Vec<i64>,
    /// Minimum SeqId for each row group.
    pub row_group_min_seqs: Vec<i64>,
    /// Maximum SeqId for each row group.
    pub row_group_max_seqs: Vec<i64>,
    pub pk_filter: Option<PkFilter>,
    /// Per-column Sort Key V1 bounds mirrored from `cold_segment_index`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_indexes: Vec<ManifestColumnIndex>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bloom_filters: Vec<ManifestBloomFilter>,
    pub status: SegmentStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
}

impl ManifestSegment {
    /// Creates a committed segment with required metadata.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn committed(
        batch: u32,
        path: impl Into<String>,
        seq_range: RangeInclusive<i64>,
        row_count: u64,
        byte_size: u64,
        schema_version: u32,
    ) -> Self {
        let min_seq = *seq_range.start();
        let max_seq = *seq_range.end();
        let row_group_rows = i64::try_from(row_count).unwrap_or(i64::MAX);
        Self {
            segment_id: None,
            batch,
            path: path.into(),
            temp_path: None,
            min_seq,
            max_seq,
            row_count,
            byte_size,
            schema_version,
            row_group_count: 1,
            row_group_row_counts: vec![row_group_rows],
            row_group_min_seqs: vec![min_seq],
            row_group_max_seqs: vec![max_seq],
            pk_filter: None,
            column_indexes: Vec::new(),
            bloom_filters: Vec::new(),
            status: SegmentStatus::Committed,
            checksum: None,
            etag: None,
            created_at: Some(Utc::now()),
        }
    }
}

/// Sort Key V1 segment and row-group bounds for one indexed column.
///
/// Same wire shape as [`koldstore_catalog::CatalogSegmentIndexBound`] (hex Storekey
/// bounds). Kept as a type alias so catalog assembly and object-store JSON share
/// one model.
pub type ManifestColumnIndex = koldstore_catalog::CatalogSegmentIndexBound;

/// Segment status in object-store manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentStatus {
    Committed,
    Pending,
    Active,
    Compacted,
    Deleted,
}

/// PK filter metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PkFilter {
    pub kind: String,
    pub column_ids: Vec<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub false_positive_rate: Option<f64>,
}

/// Bloom filter availability metadata for manifest consumers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestBloomFilter {
    pub kind: String,
    pub column_ids: Vec<i16>,
    pub false_positive_rate: Option<f64>,
}

impl ManifestBloomFilter {
    /// Creates bloom filter metadata for the given stable column IDs.
    #[must_use]
    pub fn bloom(column_ids: Vec<i16>, false_positive_rate: Option<f64>) -> Self {
        Self {
            kind: "bloom".to_string(),
            column_ids,
            false_positive_rate,
        }
    }
}

impl PkFilter {
    /// Creates exact PK metadata.
    #[must_use]
    pub fn exact(column_ids: Vec<i16>) -> Self {
        Self {
            kind: "exact".to_string(),
            column_ids,
            false_positive_rate: None,
        }
    }
}

/// Kalamdb FILE state placeholder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilesState {
    pub current_subfolder: String,
    pub subfolder_count: u32,
    pub max_files_per_subfolder: u32,
    pub total_files: Option<u64>,
}

impl Default for FilesState {
    fn default() -> Self {
        Self {
            current_subfolder: "001".to_string(),
            subfolder_count: 0,
            max_files_per_subfolder: crate::paths::SEGMENTS_PER_FOLDER,
            total_files: Some(0),
        }
    }
}
