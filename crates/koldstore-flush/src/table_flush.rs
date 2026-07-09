//! Synchronous `flush_table` orchestration helpers.
//!
//! Owns PG-free batch outcome shapes and max-rows policy resolution. Manifest
//! path helpers live in `koldstore-manifest`. SPI execution and file writes stay
//! in `pg_koldstore`.

use std::path::PathBuf;

use koldstore_manifest::Manifest;

pub use koldstore_manifest::manifest_paths;

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
    /// Mirror operations used to select and later prune this flush.
    pub mirror_ops: Option<Vec<i16>>,
    /// Sequence watermark used for conditional mirror/hot cleanup.
    pub prune_max_seq: i64,
    /// Manifest assembled from active catalog segments.
    pub manifest: Manifest,
    /// Relative manifest path under the table prefix.
    pub manifest_path: String,
    /// Absolute manifest path on local/object-store mount.
    pub absolute_manifest_path: PathBuf,
}

/// Resolves the configured max rows per Parquet file from flush policy.
///
/// # Errors
///
/// Returns an error when the configured value is below `min_floor`.
pub fn max_rows_per_file_from_policy(
    max_rows_per_file: Option<u64>,
    min_floor: u64,
) -> Result<usize, String> {
    if let Some(value) = max_rows_per_file {
        koldstore_common::validate_max_rows_per_file(value, min_floor, None)?;
        let resolved = usize::try_from(value)
            .map_err(|_| format!("max_rows_per_file {value} is too large for this platform"))?;
        return Ok(resolved.max(1));
    }

    Ok(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::max_rows_per_file_from_policy;
    use koldstore_common::DEFAULT_MIN_MAX_ROWS_PER_FILE;

    #[test]
    fn max_rows_per_file_from_policy_uses_unbounded_chunking_when_unset() {
        assert_eq!(
            max_rows_per_file_from_policy(None, DEFAULT_MIN_MAX_ROWS_PER_FILE).unwrap(),
            usize::MAX
        );
    }

    #[test]
    fn max_rows_per_file_from_policy_rejects_values_below_floor() {
        let error =
            max_rows_per_file_from_policy(Some(100), DEFAULT_MIN_MAX_ROWS_PER_FILE).unwrap_err();
        assert!(error.contains("must be at least 1000"));
    }

    #[test]
    fn max_rows_per_file_from_policy_accepts_configured_value_at_floor() {
        assert_eq!(
            max_rows_per_file_from_policy(Some(1_000), 1_000).unwrap(),
            1_000
        );
    }
}
