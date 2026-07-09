//! Mirror-row streaming encoder: typed rows → Arrow chunks.
//!
//! SPI fetch stays in `pg_koldstore`; this module owns the PG-free encode loop.
//! Post-flush cleanup uses a seq-range DELETE (see `cleanup::plan_seq_range_cleanup`)
//! so this path no longer materializes per-row cleanup JSON.

use koldstore_common::{QualifiedTableName, SqlStatement};
use koldstore_parquet::{CleanColdRecordBatchBuilder, FlushMirrorRow, PgColumn};

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
    /// When set, mirror fetch is restricted to these operation codes.
    pub mirror_ops: Option<Vec<i16>>,
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

    fn take_chunk(&mut self) -> Result<FlushWriteChunk, String> {
        let cold_batch = std::mem::replace(
            &mut self.batch_builder,
            CleanColdRecordBatchBuilder::new(&self.parquet_columns, &self.indexed_columns)?,
        )
        .finish()?;
        Ok(FlushWriteChunk { cold_batch })
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
            if chunk_builder.len() >= input.max_rows_per_file.max(1) {
                write_chunk(chunk_builder.take_chunk()?)?;
            }
        }
        if (batch_len as i64) < FLUSH_MIRROR_FETCH_BATCH_SIZE {
            break;
        }
    }

    if chunk_builder.len() > 0 {
        write_chunk(chunk_builder.take_chunk()?)?;
    }

    Ok(StreamEncodeOutcome {
        max_seq,
        rows_written,
    })
}
