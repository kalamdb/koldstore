//! Mirror-row streaming encoder: typed rows → Arrow chunks.
//!
//! SPI fetch stays in `pg_koldstore`; this module owns the PG-free encode loop.
//! Post-flush cleanup uses a seq-range DELETE (see `cleanup::plan_seq_range_cleanup`)
//! so this path no longer materializes per-row cleanup JSON.

use koldstore_common::{QualifiedTableName, SqlStatement};
use koldstore_parquet::{
    CleanColdRecordBatchBuilder, ColdMetadataColumn, ColdRecordBatch, FlushMirrorRow, PgColumn,
    SegmentSplitPolicy, StreamingParquetSegmentWriter, WriterOptions,
};

use crate::ops::plan_mirror_flush_selection_batch;
use crate::table_counters::FLUSH_MIRROR_FETCH_BATCH_SIZE;
use crate::write::FlushWriteChunk;

/// Input for one streaming flush encode pass.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamEncodeInput {
    /// Managed user table.
    pub table: QualifiedTableName,
    /// Mirror table for the managed table.
    pub mirror: QualifiedTableName,
    /// Primary-key column names.
    pub primary_key_columns: Vec<String>,
    /// Application column names in catalog order.
    pub base_column_names: Vec<String>,
    /// Parquet schema columns.
    pub parquet_columns: Vec<PgColumn>,
    /// Indexed columns tracked for segment stats.
    pub indexed_columns: Vec<String>,
    /// Active cold schema version.
    pub schema_version: u32,
    /// Maximum selected mirror `seq`.
    pub max_seq: i64,
    /// Maximum rows per Parquet segment file.
    pub max_rows_per_file: usize,
    /// Optional compressed-byte target for each Parquet segment.
    pub target_file_size_bytes: Option<u64>,
    /// Parquet compression codec.
    pub compression: String,
    /// Rows encoded per streaming row group.
    pub row_group_size: usize,
    /// When set, mirror fetch is restricted to these operation codes.
    pub mirror_ops: Option<Vec<i16>>,
}

struct SegmentBuilder {
    options: WriterOptions,
    split_policy: SegmentSplitPolicy,
    writer: Option<StreamingParquetSegmentWriter>,
    batches: Vec<ColdRecordBatch>,
    row_count: usize,
}

impl SegmentBuilder {
    fn new(input: &StreamEncodeInput) -> Self {
        let options = WriterOptions {
            compression: input.compression.clone(),
            row_group_size: input.row_group_size.max(1),
            ..WriterOptions::default()
        }
        .with_statistics_columns(
            [ColdMetadataColumn::Seq.name()]
                .into_iter()
                .chain(input.primary_key_columns.iter().map(String::as_str))
                .chain(input.indexed_columns.iter().map(String::as_str)),
        )
        .with_bloom_filter_columns(input.primary_key_columns.iter().map(String::as_str));
        Self {
            options,
            split_policy: SegmentSplitPolicy::new(
                input.target_file_size_bytes,
                input.max_rows_per_file,
            ),
            writer: None,
            batches: Vec::new(),
            row_count: 0,
        }
    }

    fn remaining_rows(&self, max_rows_per_file: usize) -> usize {
        max_rows_per_file.max(1).saturating_sub(self.row_count)
    }

    fn push_batch(&mut self, batch: ColdRecordBatch) -> Result<bool, String> {
        let writer = if let Some(writer) = self.writer.as_mut() {
            writer
        } else {
            self.writer.insert(
                StreamingParquetSegmentWriter::try_new(batch.batch.schema(), self.options.clone())
                    .map_err(|error| error.to_string())?,
            )
        };
        writer
            .write_batch(&batch.batch)
            .map_err(|error| error.to_string())?;
        self.row_count = self.row_count.saturating_add(batch.row_count);
        self.batches.push(batch);
        Ok(self
            .split_policy
            .should_close(writer.current_bytes(), self.row_count))
    }

    fn finish_segment(&mut self) -> Result<Option<FlushWriteChunk>, String> {
        let Some(writer) = self.writer.take() else {
            return Ok(None);
        };
        let parquet_bytes = writer.finish().map_err(|error| error.to_string())?;
        let batches = std::mem::take(&mut self.batches);
        self.row_count = 0;
        Ok(Some(FlushWriteChunk::from_encoded_batches(
            parquet_bytes,
            &batches,
        )))
    }
}

/// Outcome of streaming mirror rows into Parquet segment chunks.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamEncodeOutcome {
    /// Inclusive upper bound of flushed mirror `seq` values.
    pub max_seq: i64,
    /// Number of mirror rows streamed from the fetch callback.
    pub rows_written: usize,
}

struct ChunkBuilder {
    parquet_columns: Vec<PgColumn>,
    indexed_columns: Vec<String>,
    batch_builder: CleanColdRecordBatchBuilder,
}

impl ChunkBuilder {
    fn new(parquet_columns: &[PgColumn], indexed_columns: &[String]) -> Result<Self, String> {
        Ok(Self {
            parquet_columns: parquet_columns.to_vec(),
            indexed_columns: indexed_columns.to_vec(),
            batch_builder: CleanColdRecordBatchBuilder::new(parquet_columns, indexed_columns)?,
        })
    }

    fn push_row(
        &mut self,
        row: &FlushMirrorRow,
        primary_key_columns: &[String],
        schema_version: u32,
    ) -> Result<(), String> {
        self.batch_builder.push_typed_row(
            &row.values,
            primary_key_columns,
            row.seq,
            row.op,
            schema_version,
        )
    }

    fn len(&self) -> usize {
        self.batch_builder.row_count()
    }

    fn take_batch(&mut self) -> Result<ColdRecordBatch, String> {
        let cold_batch = std::mem::replace(
            &mut self.batch_builder,
            CleanColdRecordBatchBuilder::new(&self.parquet_columns, &self.indexed_columns)?,
        )
        .finish()?;
        Ok(cold_batch)
    }
}

/// Streams mirror rows through `fetch_batch` and invokes `write_chunk` per segment.
///
/// # Errors
///
/// Returns an error when selection planning, encoding, or a chunk write fails.
pub fn stream_flush_chunks<F, W>(
    input: &StreamEncodeInput,
    mut fetch_batch: F,
    mut write_chunk: W,
) -> Result<StreamEncodeOutcome, String>
where
    F: FnMut(&SqlStatement, i64, i64) -> Result<Vec<FlushMirrorRow>, String>,
    W: FnMut(FlushWriteChunk) -> Result<(), String>,
{
    let selection = plan_mirror_flush_selection_batch(
        &input.table,
        &input.mirror,
        &input.primary_key_columns,
        &input.base_column_names,
        None,
        input.mirror_ops.as_deref(),
    )
    .map_err(|error| error.to_string())?;

    let mut after_seq = 0_i64;
    let mut rows_written = 0_usize;
    let mut max_seq = 0_i64;
    let mut chunk_builder = ChunkBuilder::new(&input.parquet_columns, &input.indexed_columns)?;
    let mut segment_builder = SegmentBuilder::new(input);

    loop {
        let batch = fetch_batch(&selection.statement, input.max_seq, after_seq)?;
        if batch.is_empty() {
            break;
        }
        after_seq = batch.last().map(|row| row.seq).unwrap_or(after_seq);
        max_seq = after_seq;
        let batch_len = batch.len();
        for row in batch {
            chunk_builder.push_row(&row, &input.primary_key_columns, input.schema_version)?;
            rows_written += 1;
            let row_group_limit = input.row_group_size.max(1).min(
                segment_builder
                    .remaining_rows(input.max_rows_per_file)
                    .max(1),
            );
            if chunk_builder.len() >= row_group_limit {
                let cold_batch = chunk_builder.take_batch()?;
                if segment_builder.push_batch(cold_batch)? {
                    if let Some(chunk) = segment_builder.finish_segment()? {
                        write_chunk(chunk)?;
                    }
                }
            }
        }
        if (batch_len as i64) < FLUSH_MIRROR_FETCH_BATCH_SIZE {
            break;
        }
    }

    if chunk_builder.len() > 0 {
        let cold_batch = chunk_builder.take_batch()?;
        let _ = segment_builder.push_batch(cold_batch)?;
    }
    if let Some(chunk) = segment_builder.finish_segment()? {
        write_chunk(chunk)?;
    }

    Ok(StreamEncodeOutcome {
        max_seq,
        rows_written,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use koldstore_parquet::{FlushColumnValue, PgType};

    fn input(target_file_size_bytes: Option<u64>, max_rows_per_file: usize) -> StreamEncodeInput {
        StreamEncodeInput {
            table: QualifiedTableName::parse("app.items").unwrap(),
            mirror: QualifiedTableName::parse("koldstore.items__cl").unwrap(),
            primary_key_columns: vec!["id".to_string()],
            base_column_names: vec!["id".to_string(), "body".to_string()],
            parquet_columns: vec![
                PgColumn::new("id", PgType::Int8, false),
                PgColumn::new("body", PgType::Text, true),
            ],
            indexed_columns: vec!["id".to_string()],
            schema_version: 1,
            max_seq: 5,
            max_rows_per_file,
            target_file_size_bytes,
            compression: "zstd".to_string(),
            row_group_size: 1,
            mirror_ops: None,
        }
    }

    fn rows() -> Vec<FlushMirrorRow> {
        (1..=5)
            .map(|seq| FlushMirrorRow {
                seq,
                op: 1,
                values: vec![
                    FlushColumnValue::Int64(seq),
                    FlushColumnValue::Utf8("payload".repeat(20)),
                ],
            })
            .collect()
    }

    fn run(input: StreamEncodeInput) -> (StreamEncodeOutcome, Vec<usize>) {
        let mut fetched = false;
        let mut segment_rows = Vec::new();
        let outcome = stream_flush_chunks(
            &input,
            |_, _, _| {
                if fetched {
                    Ok(Vec::new())
                } else {
                    fetched = true;
                    Ok(rows())
                }
            },
            |chunk| {
                segment_rows.push(chunk.row_count());
                Ok(())
            },
        )
        .unwrap();
        (outcome, segment_rows)
    }

    #[test]
    fn row_cap_splits_only_the_selected_rows() {
        let (outcome, segment_rows) = run(input(None, 2));

        assert_eq!(outcome.rows_written, 5);
        assert_eq!(segment_rows, vec![2, 2, 1]);
    }

    #[test]
    fn compressed_size_target_closes_segments_without_filling_past_selection() {
        let (outcome, segment_rows) = run(input(Some(1), 100));

        assert_eq!(outcome.rows_written, 5);
        assert_eq!(segment_rows, vec![1, 1, 1, 1, 1]);
    }
}
