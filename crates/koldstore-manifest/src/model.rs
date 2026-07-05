//! Manifest model.

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

impl Manifest {
    /// Creates a shared-table manifest.
    #[must_use]
    pub fn new_shared(
        namespace: impl Into<String>,
        table: impl Into<String>,
        schema_version: u32,
    ) -> Self {
        Self::new(namespace, table, None, schema_version)
    }

    /// Creates a user-scoped manifest.
    #[must_use]
    pub fn new_user(
        namespace: impl Into<String>,
        table: impl Into<String>,
        scope_id: impl Into<String>,
        schema_version: u32,
    ) -> Self {
        Self::new(namespace, table, Some(scope_id.into()), schema_version)
    }

    fn new(
        namespace: impl Into<String>,
        table: impl Into<String>,
        scope_id: Option<String>,
        schema_version: u32,
    ) -> Self {
        Self {
            version: 1,
            table: table.into(),
            namespace: Some(namespace.into()),
            scope_id,
            schema_version,
            max_seq: 0,
            max_commit_seq: 0,
            updated_at: Utc::now(),
            publish: None,
            segments: Vec::new(),
            files: FilesState::default(),
        }
    }

    /// Appends a segment and updates watermarks for visible committed segments.
    pub fn append_segment(&mut self, segment: ManifestSegment) {
        let _ = self.append_segment_batch([segment]);
    }

    /// Appends several segments with one reserved vector growth and one manifest write.
    #[must_use]
    pub fn append_segment_batch(
        &mut self,
        segments: impl IntoIterator<Item = ManifestSegment>,
    ) -> ManifestBatchAppend {
        let segments = segments.into_iter();
        let (lower_bound, _) = segments.size_hint();
        self.segments.reserve(lower_bound);

        let mut appended_segments = 0usize;
        for segment in segments {
            if segment.status != SegmentStatus::Deleted {
                self.max_seq = self.max_seq.max(segment.max_seq);
                self.max_commit_seq = self.max_commit_seq.max(segment.max_commit_seq);
            }
            self.segments.push(segment);
            appended_segments += 1;
        }

        if appended_segments > 0 {
            self.updated_at = Utc::now();
        }

        ManifestBatchAppend {
            appended_segments,
            manifest_writes_required: usize::from(appended_segments > 0),
        }
    }

    /// Serializes the manifest to JSON.
    ///
    /// # Errors
    ///
    /// Returns JSON serialization errors.
    pub fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }
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
    /// Creates a committed segment with required metadata.
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
            status: SegmentStatus::Committed,
            checksum: None,
            etag: None,
            created_at: Some(Utc::now()),
        }
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

/// Segment status in object-store manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentStatus {
    Committed,
    Compacted,
    Deleted,
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
