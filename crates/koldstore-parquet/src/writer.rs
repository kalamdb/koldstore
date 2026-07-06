//! Parquet writer surface.

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;

use crate::footer::ColumnStats;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::{
    arrow::ArrowWriter,
    errors::ParquetError,
    file::properties::{EnabledStatistics, WriterProperties},
    schema::types::ColumnPath,
};
use serde_json::json;

/// Writer options.
#[derive(Debug, Clone, PartialEq)]
pub struct WriterOptions {
    pub compression: String,
    pub row_group_size: usize,
    pub statistics_columns: Vec<String>,
    pub bloom_filter_columns: Vec<String>,
    pub bloom_filter_false_positive_rate: Option<f64>,
}

/// Planned clean-schema cold record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanColdRecordPlan {
    /// Values to write into the cold row.
    pub values: BTreeMap<String, serde_json::Value>,
    /// Whether this row is a delete marker.
    pub deleted: bool,
}

/// Plans one clean-schema cold record from base row values and mirror metadata.
///
/// For delete markers (`op = 3`), only primary-key values plus KoldStore cold
/// metadata are authoritative; non-key base values are intentionally omitted.
///
/// # Errors
///
/// Returns an error when a primary-key value is missing or an unsupported op
/// code is supplied.
pub fn plan_clean_cold_record<I, K, P>(
    row_values: I,
    pk_columns: P,
    seq: i64,
    op: i16,
    changed_at: impl Into<String>,
    schema_version: u32,
) -> Result<CleanColdRecordPlan, String>
where
    I: IntoIterator<Item = (K, serde_json::Value)>,
    K: Into<String>,
    P: IntoIterator,
    P::Item: AsRef<str>,
{
    if !matches!(op, 1..=3) {
        return Err(format!("unsupported mirror operation code {op}"));
    }

    let input_values = row_values
        .into_iter()
        .map(|(column, value)| (column.into(), value))
        .collect::<BTreeMap<_, _>>();
    let pk_columns = pk_columns
        .into_iter()
        .map(|column| column.as_ref().to_string())
        .collect::<Vec<_>>();
    let deleted = op == 3;
    let mut values = BTreeMap::new();

    if deleted {
        for column in &pk_columns {
            let value = input_values
                .get(column)
                .ok_or_else(|| format!("delete marker missing primary-key column {column}"))?;
            values.insert(column.clone(), value.clone());
        }
    } else {
        values.extend(input_values);
    }

    values.insert("seq".to_string(), json!(seq));
    values.insert("op".to_string(), json!(op));
    values.insert("changed_at".to_string(), json!(changed_at.into()));
    values.insert("deleted".to_string(), json!(deleted));
    values.insert("schema_version".to_string(), json!(schema_version));

    Ok(CleanColdRecordPlan { values, deleted })
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            compression: "snappy".to_string(),
            row_group_size: 64 * 1024,
            statistics_columns: Vec::new(),
            bloom_filter_columns: Vec::new(),
            bloom_filter_false_positive_rate: Some(0.01),
        }
    }
}

impl WriterOptions {
    /// Sets columns with Parquet statistics enabled.
    #[must_use]
    pub fn with_statistics_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.statistics_columns = dedupe_nonblank(columns);
        self
    }

    /// Sets columns with Parquet bloom filters enabled.
    #[must_use]
    pub fn with_bloom_filter_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.bloom_filter_columns = dedupe_nonblank(columns);
        self
    }

    /// Builds native Parquet writer properties for stats and bloom metadata.
    #[must_use]
    pub fn native_writer_properties(&self) -> WriterProperties {
        let mut builder =
            WriterProperties::builder().set_statistics_enabled(EnabledStatistics::None);
        for column in &self.statistics_columns {
            builder = builder.set_column_statistics_enabled(
                ColumnPath::from(column.as_str()),
                EnabledStatistics::Chunk,
            );
        }
        for column in &self.bloom_filter_columns {
            let path = ColumnPath::from(column.as_str());
            builder = builder.set_column_bloom_filter_enabled(path.clone(), true);
            if let Some(false_positive_rate) = self.bloom_filter_false_positive_rate {
                builder = builder.set_column_bloom_filter_fpp(path, false_positive_rate);
            }
        }
        builder.build()
    }
}

/// Segment writer placeholder.
#[derive(Debug, Clone)]
pub struct ParquetSegmentWriter {
    pub options: WriterOptions,
}

impl ParquetSegmentWriter {
    /// Creates a segment writer.
    #[must_use]
    pub fn new(options: WriterOptions) -> Self {
        Self { options }
    }

    /// Builds a deterministic segment write plan.
    #[must_use]
    pub fn plan_segment(
        &self,
        prefix: &str,
        batch: u32,
        min_seq: i64,
        max_seq: i64,
        min_commit_seq: i64,
        max_commit_seq: i64,
    ) -> SegmentWritePlan {
        let prefix = prefix.trim_matches('/');
        SegmentWritePlan {
            object_path: format!("{prefix}/batch-{batch}.parquet"),
            min_seq,
            max_seq,
            min_commit_seq,
            max_commit_seq,
            compression: self.options.compression.clone(),
            row_count: 0,
            byte_size: 0,
            column_stats: BTreeMap::new(),
            pk_filter_kind: None,
            pk_filter_columns: Vec::new(),
            statistics_columns: self.options.statistics_columns.clone(),
            bloom_filter_columns: self.options.bloom_filter_columns.clone(),
            writes_native_bloom_filters: !self.options.bloom_filter_columns.is_empty(),
        }
    }

    /// Builds a deterministic segment write plan with manifest metadata.
    #[must_use]
    pub fn plan_segment_with_metadata(
        &self,
        prefix: &str,
        batch: u32,
        metadata: SegmentMetadataInput,
    ) -> SegmentWritePlan {
        let mut plan = self.plan_segment(
            prefix,
            batch,
            metadata.min_seq,
            metadata.max_seq,
            metadata.min_commit_seq,
            metadata.max_commit_seq,
        );
        plan.row_count = metadata.row_count;
        plan.byte_size = metadata.byte_size;
        plan.column_stats = metadata.column_stats.into_iter().collect();
        plan.pk_filter_kind = (!metadata.pk_columns.is_empty()).then(|| "bloom".to_string());
        plan.pk_filter_columns = metadata.pk_columns;
        plan.bloom_filter_columns = dedupe_nonblank(
            metadata
                .bloom_filter_columns
                .into_iter()
                .chain(plan.bloom_filter_columns),
        );
        plan.statistics_columns = dedupe_nonblank(
            metadata
                .statistics_columns
                .into_iter()
                .chain(plan.statistics_columns),
        );
        plan.writes_native_bloom_filters = !plan.bloom_filter_columns.is_empty();
        plan
    }

    /// Plans bounded row-group streaming for a segment write.
    #[must_use]
    pub fn plan_streaming_row_groups(&self, total_rows: u64) -> StreamingRowGroupPlan {
        let row_group_size = self.options.row_group_size.max(1);
        let row_group_count = total_rows.div_ceil(row_group_size as u64) as usize;
        StreamingRowGroupPlan {
            total_rows,
            row_group_count,
            max_rows_in_memory: row_group_size,
        }
    }

    /// Writes Arrow record batches to a native Parquet writer.
    ///
    /// # Errors
    ///
    /// Returns a Parquet error if the schema, writer, or any batch write fails.
    pub fn write_record_batches<W, I>(
        &self,
        writer: W,
        schema: SchemaRef,
        batches: I,
    ) -> Result<Arc<parquet::file::metadata::ParquetMetaData>, ParquetError>
    where
        W: Write + Send,
        I: IntoIterator<Item = RecordBatch>,
    {
        let mut writer = ArrowWriter::try_new(
            writer,
            schema,
            Some(self.options.native_writer_properties()),
        )?;
        for batch in batches {
            writer.write(&batch)?;
        }
        Ok(Arc::new(writer.close()?))
    }
}

/// Bounded row-group streaming plan for writing a Parquet segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamingRowGroupPlan {
    /// Total logical rows expected in the segment.
    pub total_rows: u64,
    /// Number of row groups to write.
    pub row_group_count: usize,
    /// Maximum rows buffered before flushing a row group.
    pub max_rows_in_memory: usize,
}

/// Segment metadata captured while writing a Parquet object.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentMetadataInput {
    /// Minimum `_seq`.
    pub min_seq: i64,
    /// Maximum `_seq`.
    pub max_seq: i64,
    /// Minimum `_commit_seq`.
    pub min_commit_seq: i64,
    /// Maximum `_commit_seq`.
    pub max_commit_seq: i64,
    /// Number of rows written.
    pub row_count: u64,
    /// Final object byte size.
    pub byte_size: u64,
    /// Primary-key columns eligible for bloom metadata.
    pub pk_columns: Vec<String>,
    /// Columns configured for native Parquet bloom filters.
    pub bloom_filter_columns: Vec<String>,
    /// Columns configured for Parquet statistics.
    pub statistics_columns: Vec<String>,
    /// Column stats used by segment pruning.
    pub column_stats: Vec<(String, ColumnStats)>,
}

/// Planned segment metadata produced by the writer.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentWritePlan {
    /// Final object path.
    pub object_path: String,
    /// Minimum `_seq`.
    pub min_seq: i64,
    /// Maximum `_seq`.
    pub max_seq: i64,
    /// Minimum `_commit_seq`.
    pub min_commit_seq: i64,
    /// Maximum `_commit_seq`.
    pub max_commit_seq: i64,
    /// Compression codec.
    pub compression: String,
    /// Number of rows written.
    pub row_count: u64,
    /// Final object byte size.
    pub byte_size: u64,
    /// Column stats captured from the written footer.
    pub column_stats: BTreeMap<String, ColumnStats>,
    /// PK filter kind recorded for kalamdb-compatible manifests.
    pub pk_filter_kind: Option<String>,
    /// PK columns covered by the filter.
    pub pk_filter_columns: Vec<String>,
    /// Columns with Parquet statistics enabled.
    pub statistics_columns: Vec<String>,
    /// Columns with native Parquet bloom filters enabled.
    pub bloom_filter_columns: Vec<String>,
    /// Whether native Parquet bloom filters will be written.
    pub writes_native_bloom_filters: bool,
}

fn dedupe_nonblank<I, S>(values: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    values.into_iter().fold(Vec::new(), |mut columns, value| {
        let column = value.into();
        let column = column.trim();
        if !column.is_empty() && !columns.iter().any(|existing| existing == column) {
            columns.push(column.to_string());
        }
        columns
    })
}
