//! Parquet writer surface.
//!
//! Encodes Arrow [`RecordBatch`]es with native Parquet writer properties
//! (zstd, per-column statistics, PK bloom filters). Durable object publication
//! lives in `koldstore-storage`; this module owns encoding and footer validation
//! only so a failed encode never touches a final object key.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use crate::footer::ColumnStats;
use crate::schema::{ColdMetadataColumn, PgColumn};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use koldstore_common::dedupe_nonblank;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    errors::ParquetError,
    file::properties::{EnabledStatistics, WriterProperties},
    file::reader::{FileReader, SerializedFileReader},
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

/// Independent file-packing limits applied after flush eligibility is resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentSplitPolicy {
    target_file_size_bytes: Option<u64>,
    max_rows_per_file: usize,
}

impl SegmentSplitPolicy {
    /// Creates a split policy from an optional compressed-byte target and row cap.
    #[must_use]
    pub const fn new(target_file_size_bytes: Option<u64>, max_rows_per_file: usize) -> Self {
        Self {
            target_file_size_bytes,
            max_rows_per_file,
        }
    }

    /// Returns whether the current segment should close.
    ///
    /// The byte target is deliberately independent from row eligibility. Callers
    /// only apply this policy to rows already selected for the current flush.
    #[must_use]
    pub fn should_close(self, current_bytes: u64, current_rows: usize) -> bool {
        current_rows >= self.max_rows_per_file.max(1)
            || self
                .target_file_size_bytes
                .is_some_and(|target| current_bytes >= target)
    }
}

#[derive(Debug, Clone, Default)]
struct SharedWriteBuffer {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl SharedWriteBuffer {
    fn into_bytes(self) -> Result<Vec<u8>, ParquetError> {
        let bytes = self.bytes.lock().map_err(|_| {
            ParquetError::General("parquet output buffer lock poisoned".to_string())
        })?;
        Ok(bytes.clone())
    }
}

impl Write for SharedWriteBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let mut bytes = self
            .bytes
            .lock()
            .map_err(|_| io::Error::other("parquet output buffer lock poisoned"))?;
        bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Incremental Parquet encoder exposing compressed bytes at row-group boundaries.
pub struct StreamingParquetSegmentWriter {
    writer: ArrowWriter<SharedWriteBuffer>,
    output: SharedWriteBuffer,
}

impl StreamingParquetSegmentWriter {
    /// Opens a streaming segment writer.
    ///
    /// # Errors
    ///
    /// Returns an error when writer properties or the Arrow writer are invalid.
    pub fn try_new(schema: SchemaRef, options: WriterOptions) -> Result<Self, ParquetError> {
        let output = SharedWriteBuffer::default();
        let writer = ArrowWriter::try_new(
            output.clone(),
            schema,
            Some(
                options
                    .try_native_writer_properties()
                    .map_err(ParquetError::General)?,
            ),
        )?;
        Ok(Self { writer, output })
    }

    /// Writes and flushes one bounded row group.
    ///
    /// # Errors
    ///
    /// Returns an error when Parquet encoding or row-group flush fails.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        self.writer.write(batch)?;
        self.writer.flush()
    }

    /// Returns compressed bytes emitted so far, excluding the final footer.
    #[must_use]
    pub fn current_bytes(&self) -> u64 {
        u64::try_from(self.writer.bytes_written()).unwrap_or(u64::MAX)
    }

    /// Closes the writer and returns the complete Parquet object.
    ///
    /// # Errors
    ///
    /// Returns an error when footer encoding fails or the output lock is poisoned.
    pub fn finish(self) -> Result<Vec<u8>, ParquetError> {
        self.writer.close()?;
        self.output.into_bytes()
    }
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
    values.insert("deleted".to_string(), json!(deleted));
    values.insert("schema_version".to_string(), json!(schema_version));

    Ok(CleanColdRecordPlan { values, deleted })
}

/// Builds one Arrow record batch from planned clean-schema cold rows.
///
/// # Errors
///
/// Returns an error when schema conversion fails or any JSON value cannot be
/// coerced into the requested Arrow type.
pub fn record_batch_from_clean_cold_records(
    columns: &[PgColumn],
    rows: &[CleanColdRecordPlan],
) -> Result<RecordBatch, String> {
    let mut builder = crate::batch_builder::CleanColdRecordBatchBuilder::new(columns, &[])?;
    for row in rows {
        builder.push_plan(row)?;
    }
    Ok(builder.finish()?.batch)
}

/// Encodes one cold segment to Parquet bytes and validates the footer.
///
/// Prefer this over writing directly to a final filesystem path: callers publish
/// the returned bytes through `koldstore-storage` so partial encodes never become
/// visible cold objects.
///
/// # Errors
///
/// Returns an error when encoding fails or the encoded bytes fail footer
/// validation (truncated / missing magic / unreadable metadata).
pub fn encode_parquet_segment_bytes(
    batch: &RecordBatch,
    primary_key_columns: &[String],
    indexed_columns: &[String],
    compression: &str,
) -> Result<Vec<u8>, String> {
    let writer = ParquetSegmentWriter::new(
        WriterOptions {
            compression: compression.to_string(),
            ..WriterOptions::default()
        }
        .with_statistics_columns(
            [ColdMetadataColumn::Seq.name()]
                .into_iter()
                .chain(primary_key_columns.iter().map(String::as_str))
                .chain(indexed_columns.iter().map(String::as_str)),
        )
        .with_bloom_filter_columns(primary_key_columns.iter().map(String::as_str)),
    );
    let bytes = writer
        .encode_record_batch(batch)
        .map_err(|error| error.to_string())?;
    validate_parquet_bytes(&bytes)?;
    Ok(bytes)
}

/// Validates that `bytes` are a complete, readable Parquet file.
///
/// Checks the 4-byte magic, footer length, and that metadata can be opened.
///
/// # Errors
///
/// Returns an error when the payload is truncated, missing magic, or has an
/// unreadable footer.
pub fn validate_parquet_bytes(bytes: &[u8]) -> Result<ParquetValidation, String> {
    const MAGIC: &[u8] = b"PAR1";
    if bytes.len() < 8 {
        return Err(format!(
            "parquet payload too small ({} bytes); missing footer",
            bytes.len()
        ));
    }
    if &bytes[..4] != MAGIC {
        return Err("parquet payload missing PAR1 magic header".to_string());
    }
    if &bytes[bytes.len() - 4..] != MAGIC {
        return Err("parquet payload missing PAR1 magic footer".to_string());
    }
    let footer_len = u32::from_le_bytes(
        bytes[bytes.len() - 8..bytes.len() - 4]
            .try_into()
            .map_err(|_| "parquet footer length slice".to_string())?,
    ) as usize;
    if footer_len == 0 || bytes.len() < footer_len + 8 {
        return Err(format!(
            "parquet footer length {footer_len} is inconsistent with payload size {}",
            bytes.len()
        ));
    }

    let reader = SerializedFileReader::new(Bytes::copy_from_slice(bytes))
        .map_err(|error| format!("parquet footer: {error}"))?;
    let metadata = reader.metadata();
    let row_count = metadata.file_metadata().num_rows();
    if row_count < 0 {
        return Err("parquet footer reports negative row count".to_string());
    }
    Ok(ParquetValidation {
        byte_size: u64::try_from(bytes.len()).map_err(|error| error.to_string())?,
        row_count: u64::try_from(row_count).map_err(|error| error.to_string())?,
        row_group_count: metadata.num_row_groups(),
    })
}

/// Result of validating encoded Parquet bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParquetValidation {
    /// Encoded byte size.
    pub byte_size: u64,
    /// Rows reported by the file footer.
    pub row_count: u64,
    /// Number of row groups in the file.
    pub row_group_count: usize,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            compression: "zstd".to_string(),
            // Keep row groups small enough that PK point lookups can skip most
            // of a flush segment via bloom/min-max (default flush batch is 10k).
            row_group_size: 1_024,
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

    /// Builds native Parquet writer properties for stats, bloom metadata, and compression.
    ///
    /// Bloom filters use parquet 59's `set_column_bloom_filter_max_ndv`, sized to
    /// the configured row-group row count (library-recommended heuristic).
    ///
    /// # Errors
    ///
    /// Returns an error when the configured compression codec is unsupported.
    pub fn try_native_writer_properties(&self) -> Result<WriterProperties, String> {
        let row_group_size = self.row_group_size.max(1);
        let mut builder = WriterProperties::builder()
            .set_compression(parquet_compression(&self.compression)?)
            .set_max_row_group_row_count(Some(row_group_size))
            .set_statistics_enabled(EnabledStatistics::None);
        for column in &self.statistics_columns {
            builder = builder.set_column_statistics_enabled(
                ColumnPath::from(column.as_str()),
                EnabledStatistics::Chunk,
            );
        }
        for column in &self.bloom_filter_columns {
            let path = ColumnPath::from(column.as_str());
            builder = builder
                .set_column_bloom_filter_enabled(path.clone(), true)
                .set_column_bloom_filter_max_ndv(path.clone(), row_group_size as u64);
            if let Some(false_positive_rate) = self.bloom_filter_false_positive_rate {
                builder = builder.set_column_bloom_filter_fpp(path, false_positive_rate);
            }
        }
        Ok(builder.build())
    }
}

/// Segment writer.
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
        let folder = (batch.max(1) - 1) / 100 + 1;
        SegmentWritePlan {
            object_path: format!("{prefix}/{folder:03}/segment-{batch:04}.parquet"),
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

    /// Encodes one Arrow record batch to Parquet bytes.
    ///
    /// # Errors
    ///
    /// Returns a Parquet error if the schema, writer, or batch write fails.
    pub fn encode_record_batch(&self, batch: &RecordBatch) -> Result<Vec<u8>, ParquetError> {
        let mut buffer = Vec::with_capacity(estimate_buffer_capacity(batch));
        let metadata =
            self.write_record_batches(&mut buffer, batch.schema(), std::iter::once(batch.clone()))?;
        debug_assert_eq!(
            metadata.file_metadata().num_rows() as usize,
            batch.num_rows()
        );
        Ok(buffer)
    }

    /// Writes one Arrow record batch to a native Parquet writer.
    ///
    /// # Errors
    ///
    /// Returns a Parquet error if the schema, writer, or batch write fails.
    pub fn write_record_batch<W>(
        &self,
        writer: W,
        batch: &RecordBatch,
    ) -> Result<Arc<parquet::file::metadata::ParquetMetaData>, ParquetError>
    where
        W: Write + Send,
    {
        self.write_record_batches(writer, batch.schema(), std::iter::once(batch.clone()))
    }

    /// Writes Arrow record batches to a native Parquet writer.
    ///
    /// Row-group boundaries are controlled by
    /// [`WriterProperties::max_row_group_row_count`] — `ArrowWriter::write`
    /// already splits oversized batches, so callers must not also force
    /// per-slice `flush()` (that would create needless tiny row groups).
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
            Some(
                self.options
                    .try_native_writer_properties()
                    .map_err(ParquetError::General)?,
            ),
        )?;
        for batch in batches {
            writer.write(&batch)?;
        }
        // Finalize footer. Dropping without close() would leave an unreadable file.
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

fn parquet_compression(codec: &str) -> Result<Compression, String> {
    match codec.trim().to_ascii_lowercase().as_str() {
        // Empty inherits the WriterOptions default (zstd), not snappy.
        "" | "zstd" => Ok(Compression::ZSTD(ZstdLevel::default())),
        "snappy" => Ok(Compression::SNAPPY),
        "uncompressed" | "none" => Ok(Compression::UNCOMPRESSED),
        other => Err(format!("unsupported parquet compression `{other}`")),
    }
}

fn estimate_buffer_capacity(batch: &RecordBatch) -> usize {
    // Rough lower bound: avoid repeated realloc for small segments.
    64 * 1024 + batch.num_rows().saturating_mul(64)
}
