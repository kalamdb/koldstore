//! Request and row types for clean-schema Parquet reads.

use koldstore_common::RowImage;

use super::options::ParquetReadOptions;

/// Direct object-store Parquet read request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetReadRequest {
    /// Final object-store path.
    pub object_path: String,
    /// Projection and pruning options.
    pub options: ParquetReadOptions,
}

/// Logical row read from a clean-schema cold Parquet segment.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct CleanColdRow {
    /// Primary-key values encoded by column name.
    pub pk_json: serde_json::Value,
    /// Base table row image (empty for delete markers).
    pub row_image: RowImage,
    /// KoldStore sequence number.
    pub seq: i64,
    /// Mirror operation code (`1` insert, `2` update, `3` delete).
    pub op: i16,
    /// Whether this row is a cold delete marker.
    pub deleted: bool,
    /// Schema version used to write the segment.
    pub schema_version: u32,
}

impl ParquetReadRequest {
    /// Creates a direct Parquet read request.
    #[must_use]
    pub fn new(object_path: impl Into<String>, options: ParquetReadOptions) -> Self {
        Self {
            object_path: object_path.into(),
            options,
        }
    }

    /// Returns true because the direct reader inspects footer metadata before column chunks.
    #[must_use]
    pub const fn uses_footer_before_columns(&self) -> bool {
        true
    }

    /// Returns true when PK bloom/may-contain metadata can be checked.
    #[must_use]
    pub fn uses_pk_bloom_checks(&self) -> bool {
        self.options.pk_values.is_some()
    }
}
