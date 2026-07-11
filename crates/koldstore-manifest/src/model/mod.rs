//! Manifest serialized model types.

use std::collections::BTreeMap;
use std::ops::RangeInclusive;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Object-store manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub table: String,
    pub namespace: Option<String>,
    pub scope_id: Option<String>,
    pub schema_version: u32,
    pub max_seq: i64,
    pub max_commit_seq: i64,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish: Option<PublishState>,
    pub segments: Vec<ManifestSegment>,
    pub files: FilesState,
}

/// Result of applying a batch of manifest segment appends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestBatchAppend {
    /// Number of segment entries appended.
    pub appended_segments: usize,
    /// Number of object-store `manifest.json` writes needed for the batch.
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

/// Manifest segment entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestSegment {
    pub batch: u32,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temp_path: Option<String>,
    pub min_seq: i64,
    pub max_seq: i64,
    pub min_commit_seq: i64,
    pub max_commit_seq: i64,
    pub row_count: u64,
    pub byte_size: u64,
    pub schema_version: u32,
    pub pk_filter: Option<PkFilter>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub column_stats: BTreeMap<String, ManifestColumnStats>,
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
    /// Creates a published segment with required metadata.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn published(
        batch: u32,
        path: impl Into<String>,
        seq_range: RangeInclusive<i64>,
        commit_range: RangeInclusive<i64>,
        row_count: u64,
        byte_size: u64,
        schema_version: u32,
    ) -> Self {
        let min_seq = *seq_range.start();
        let max_seq = *seq_range.end();
        let min_commit_seq = *commit_range.start();
        let max_commit_seq = *commit_range.end();
        Self {
            batch,
            path: path.into(),
            temp_path: None,
            min_seq,
            max_seq,
            min_commit_seq,
            max_commit_seq,
            row_count,
            byte_size,
            schema_version,
            pk_filter: None,
            column_stats: BTreeMap::new(),
            bloom_filters: Vec::new(),
            status: SegmentStatus::Published,
            checksum: None,
            etag: None,
            created_at: Some(Utc::now()),
        }
    }

    /// Alias for [`Self::published`] (legacy call-site name during cutover).
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn committed(
        batch: u32,
        path: impl Into<String>,
        seq_range: RangeInclusive<i64>,
        commit_range: RangeInclusive<i64>,
        row_count: u64,
        byte_size: u64,
        schema_version: u32,
    ) -> Self {
        Self::published(
            batch,
            path,
            seq_range,
            commit_range,
            row_count,
            byte_size,
            schema_version,
        )
    }
}

/// Min/max stats for one manifest column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestColumnStats {
    pub min: serde_json::Value,
    pub max: serde_json::Value,
}

impl ManifestColumnStats {
    /// Creates manifest min/max stats.
    #[must_use]
    pub fn new(min: serde_json::Value, max: serde_json::Value) -> Self {
        Self { min, max }
    }
}

/// Segment status in object-store manifest (aligned with catalog lifecycle).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentStatus {
    Pending,
    Staged,
    Published,
    Superseded,
    Deleting,
    Deleted,
    Orphaned,
}

impl SegmentStatus {
    /// Returns whether this segment is query-visible in the current snapshot.
    #[must_use]
    pub const fn is_query_visible(self) -> bool {
        matches!(self, Self::Published)
    }
}

/// PK filter metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PkFilter {
    pub kind: String,
    pub column_ids: Vec<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub false_positive_rate: Option<f64>,
}

/// Bloom filter availability metadata for manifest consumers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestBloomFilter {
    pub kind: String,
    pub columns: Vec<String>,
    pub false_positive_rate: Option<f64>,
}

impl ManifestBloomFilter {
    /// Creates bloom filter metadata for the given columns.
    #[must_use]
    pub fn bloom(columns: Vec<String>, false_positive_rate: Option<f64>) -> Self {
        Self {
            kind: "bloom".to_string(),
            columns,
            false_positive_rate,
        }
    }
}

impl PkFilter {
    /// Creates exact PK metadata.
    #[must_use]
    pub fn exact(column_ids: Vec<u32>) -> Self {
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
            current_subfolder: "files-0".to_string(),
            subfolder_count: 0,
            max_files_per_subfolder: 10_000,
            total_files: Some(0),
        }
    }
}
