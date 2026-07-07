//! Direct ObjectStore-backed Parquet reader surface.

use std::path::Path;
use std::pin::Pin;

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, UInt32Array};
use koldstore_common::{CommitSeq, SeqId};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::schema::{ColdMetadataColumn, PgColumn};

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

    /// Projects clean-schema change metadata columns.
    #[must_use]
    pub fn with_clean_change_metadata(mut self) -> Self {
        self.columns = vec![
            "seq".to_string(),
            "op".to_string(),
            "deleted".to_string(),
            "schema_version".to_string(),
        ];
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

    /// Adds clean-schema `seq` range pruning.
    #[must_use]
    pub fn with_clean_seq_range(mut self, min: SeqId, max: SeqId) -> Self {
        self.seq_range = Some(SeqRange {
            column: crate::schema::ColdMetadataColumn::Seq.name().to_string(),
            min,
            max,
        });
        self
    }

    /// Adds sequence range pruning for the given column name.
    #[must_use]
    pub fn with_seq_range(mut self, column: impl Into<String>, min: SeqId, max: SeqId) -> Self {
        self.seq_range = Some(SeqRange {
            column: column.into(),
            min,
            max,
        });
        self
    }

    /// Adds commit-sequence range pruning for the given column name.
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

/// Logical row read from a clean-schema cold Parquet segment.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct CleanColdRow {
    /// Primary-key values encoded by column name.
    pub pk_json: serde_json::Value,
    /// Base table row image, or `null` for delete markers.
    pub row_image: serde_json::Value,
    /// KoldStore sequence number.
    pub seq: i64,
    /// Commit sequence used for winner ordering. Clean segments currently use
    /// `seq` as the commit ordering value.
    pub commit_seq: i64,
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

/// Reads clean-schema cold rows from a local Parquet file.
///
/// # Errors
///
/// Returns an error when the file cannot be opened, Parquet decoding fails, or
/// required metadata/primary-key columns are missing.
pub fn read_clean_cold_rows_from_path(
    path: impl AsRef<Path>,
    columns: &[PgColumn],
    primary_key_columns: &[String],
) -> Result<Vec<CleanColdRow>, String> {
    let file = std::fs::File::open(path.as_ref()).map_err(|error| error.to_string())?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| error.to_string())?
        .build()
        .map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|error| error.to_string())?;
        rows.extend(clean_rows_from_batch(&batch, columns, primary_key_columns)?);
    }
    Ok(rows)
}

fn clean_rows_from_batch(
    batch: &RecordBatch,
    columns: &[PgColumn],
    primary_key_columns: &[String],
) -> Result<Vec<CleanColdRow>, String> {
    let seq = required_column(batch, ColdMetadataColumn::Seq.name())?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "cold seq column has unexpected Arrow type".to_string())?;
    let deleted = required_column(batch, ColdMetadataColumn::Deleted.name())?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "cold deleted column has unexpected Arrow type".to_string())?;
    let schema_version = required_column(batch, ColdMetadataColumn::SchemaVersion.name())?
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| "cold schema_version column has unexpected Arrow type".to_string())?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        let deleted_value = deleted.value(row_index);
        let mut row_image = serde_json::Map::new();
        for column in columns {
            row_image.insert(
                column.name.clone(),
                crate::pg_type_codec::json_value_from_arrow_column(batch, column, row_index)?,
            );
        }
        let mut pk_json = serde_json::Map::new();
        for column in primary_key_columns {
            pk_json.insert(
                column.clone(),
                row_image
                    .get(column)
                    .cloned()
                    .ok_or_else(|| format!("cold row is missing primary-key field `{column}`"))?,
            );
        }
        let row_image = if deleted_value {
            serde_json::Value::Null
        } else {
            serde_json::Value::Object(row_image)
        };
        let seq_value = seq.value(row_index);
        rows.push(CleanColdRow {
            pk_json: serde_json::Value::Object(pk_json),
            row_image,
            seq: seq_value,
            commit_seq: seq_value,
            deleted: deleted_value,
            schema_version: schema_version.value(row_index),
        });
    }
    Ok(rows)
}

fn required_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a dyn Array, String> {
    batch
        .column_by_name(name)
        .map(|column| column.as_ref())
        .ok_or_else(|| format!("cold segment is missing required column `{name}`"))
}
