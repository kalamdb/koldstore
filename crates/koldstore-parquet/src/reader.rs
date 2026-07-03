//! Direct ObjectStore-backed Parquet reader surface.

use std::pin::Pin;

use arrow::record_batch::RecordBatch;

/// Boxed record-batch stream.
pub type RecordBatchFileStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<RecordBatch, String>> + Send>>;

/// Read options for projection and pruning.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParquetReadOptions {
    pub columns: Vec<String>,
    pub row_groups: Option<Vec<usize>>,
    pub seq_range: Option<SeqRange>,
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

    /// Adds `_seq` range pruning.
    #[must_use]
    pub fn with_seq_range(mut self, column: impl Into<String>, min: i64, max: i64) -> Self {
        self.seq_range = Some(SeqRange {
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
    pub min: i64,
    pub max: i64,
}

/// PK values for pruning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkValues {
    pub column: String,
    pub values: Vec<String>,
}
