//! Synchronous `flush_table` orchestration helpers.
//!
//! Owns PG-free path planning and batch outcome shapes. SPI execution and file
//! writes stay in `pg_koldstore`.

use std::path::PathBuf;

use koldstore_manifest::Manifest;

/// Prepared context for one synchronous flush attempt.
#[derive(Debug, Clone, PartialEq)]
pub struct TableFlushPreparedContext {
    /// Flush job id.
    pub job_id: uuid::Uuid,
    /// Whether policy limits were bypassed.
    pub force: bool,
    /// Relation namespace.
    pub namespace: String,
    /// Relation name.
    pub table_name: String,
    /// Object-store base path.
    pub base_path: String,
    /// Active schema version.
    pub schema_version: i32,
    /// Compression codec.
    pub compression: String,
    /// Primary-key columns.
    pub primary_key_columns: Vec<String>,
    /// Maximum rows per Parquet segment.
    pub max_rows_per_file: usize,
}

/// Outcome of writing one or more flush batches.
#[derive(Debug, Clone, PartialEq)]
pub struct TableFlushBatchOutcome {
    /// Total rows flushed.
    pub total_rows_flushed: i64,
    /// Last flushed `_seq`.
    pub last_max_seq: i64,
    /// Last flushed `_commit_seq`.
    pub last_max_commit_seq: i64,
    /// Manifest assembled from active catalog segments.
    pub manifest: Manifest,
    /// Relative manifest path under the table prefix.
    pub manifest_path: String,
    /// Absolute manifest path on local/object-store mount.
    pub absolute_manifest_path: PathBuf,
}

/// Relative and absolute manifest paths for a managed table flush.
#[must_use]
pub fn manifest_paths(namespace: &str, table_name: &str, base_path: &str) -> (String, PathBuf) {
    let prefix = format!("{namespace}/{table_name}");
    let manifest_path = format!("{prefix}/manifest.json");
    let absolute_manifest_path = PathBuf::from(base_path).join(&manifest_path);
    (manifest_path, absolute_manifest_path)
}

/// Resolves the configured max rows per Parquet file from flush policy.
#[must_use]
pub fn max_rows_per_file_from_policy(max_rows_per_file: Option<u64>) -> usize {
    max_rows_per_file
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(usize::MAX)
        .max(1)
}
