//! Direct ObjectStore-backed Parquet reader surface.

use std::pin::Pin;

use arrow_array::RecordBatch;
use koldstore_core::{CommitSeq, SeqId};

/// Boxed record-batch stream.
pub type RecordBatchFileStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<RecordBatch, String>> + Send>>;

/// Read options for projection and pruning.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParquetReadOptions {
    pub columns: Vec<String>,
    pub row_groups: Option<Vec<usize>>,
    pub seq_range: Option<SeqRange>,
    pub commit_seq_range: Option<CommitSeqRange>,
    pub pk_values: Option<PkValues>,
}

impl ParquetReadOptions {
    /// Creates default read options.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds projection columns.
    #[must_use]
    pub fn with_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Adds selected row groups after footer/stat/bloom pruning.
    #[must_use]
    pub fn with_row_groups<I>(mut self, row_groups: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        self.row_groups = Some(row_groups.into_iter().collect());
        self
    }

    /// Adds `_seq` range pruning.
    #[must_use]
    pub fn with_seq_range(mut self, column: impl Into<String>, min: SeqId, max: SeqId) -> Self {
        self.seq_range = Some(SeqRange {
            column: column.into(),
            min,
            max,
        });
        self
    }

    /// Adds `_commit_seq` range pruning.
    #[must_use]
    pub fn with_commit_seq_range(
        mut self,
        column: impl Into<String>,
        min: CommitSeq,
        max: CommitSeq,
    ) -> Self {
        self.commit_seq_range = Some(CommitSeqRange {
            column: column.into(),
            min,
            max,
        });
        self
    }

    /// Adds PK may-contain values for bloom/exact pruning.
    #[must_use]
    pub fn with_pk_values<I, S>(mut self, column: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.pk_values = Some(PkValues {
            column: column.into(),
            values: values.into_iter().map(Into::into).collect(),
        });
        self
    }
}

/// Sequence range pruning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeqRange {
    pub column: String,
    pub min: SeqId,
    pub max: SeqId,
}

/// Commit sequence range pruning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitSeqRange {
    pub column: String,
    pub min: CommitSeq,
    pub max: CommitSeq,
}

/// PK values for pruning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkValues {
    pub column: String,
    pub values: Vec<String>,
}

/// Direct object-store Parquet read request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetReadRequest {
    /// Final object-store path.
    pub object_path: String,
    /// Projection and pruning options.
    pub options: ParquetReadOptions,
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
