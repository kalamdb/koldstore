//! Mirror row selection planning for flush writes.
//!
//! Owns PG-free cold-record and cleanup-row planning from mirror selection rows.
//! SPI execution stays in `pg_koldstore`.

use koldstore_mirror::MirrorSelectionRow;
use koldstore_parquet::CleanColdRecordPlan;

/// Planned flush write input before chunking and Parquet emission.
#[derive(Debug, Clone, PartialEq)]
pub struct FlushWriteInput {
    /// Parquet column schema.
    pub columns: Vec<koldstore_parquet::PgColumn>,
    /// Cold records to write.
    pub rows: Vec<CleanColdRecordPlan>,
    /// Cleanup rows for hot heap pruning after flush.
    pub cleanup_rows: Vec<serde_json::Value>,
}

/// One chunk of rows selected for a single Parquet segment.
#[derive(Debug, Clone, PartialEq)]
pub struct FlushWriteChunk {
    /// Cold records in this chunk.
    pub rows: Vec<CleanColdRecordPlan>,
    /// Cleanup rows paired with this chunk.
    pub cleanup_rows: Vec<serde_json::Value>,
}

/// Splits a flush write input into bounded Parquet segment chunks.
#[must_use]
pub fn chunk_flush_write_input(
    write_input: &FlushWriteInput,
    max_rows_per_file: usize,
) -> Vec<FlushWriteChunk> {
    if write_input.rows.is_empty() {
        return Vec::new();
    }
    let chunk_size = max_rows_per_file.max(1);
    write_input
        .rows
        .chunks(chunk_size)
        .zip(write_input.cleanup_rows.chunks(chunk_size))
        .map(|(rows, cleanup_rows)| FlushWriteChunk {
            rows: rows.to_vec(),
            cleanup_rows: cleanup_rows.to_vec(),
        })
        .collect()
}

/// Plans one mirror selection row into a cold Parquet record.
///
/// # Errors
///
/// Returns an error when mirror metadata or row values are invalid.
pub fn plan_flush_cold_record(
    row: MirrorSelectionRow,
    base_columns: &[String],
    primary_key_columns: &[String],
    schema_version: u32,
) -> Result<CleanColdRecordPlan, String> {
    let op = i16::try_from(row.op).map_err(|error| error.to_string())?;
    let row_values = base_columns
        .iter()
        .map(|column| {
            (
                column.clone(),
                row.fields
                    .get(column)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            )
        })
        .collect::<Vec<_>>();
    koldstore_parquet::plan_clean_cold_record(
        row_values,
        primary_key_columns,
        row.seq,
        op,
        schema_version,
    )
}

/// Plans one cleanup JSON row for hot heap pruning after flush.
///
/// # Errors
///
/// Returns an error when primary-key fields are missing or invalid.
pub fn plan_flush_cleanup_row(
    row: &MirrorSelectionRow,
    primary_key_columns: &[String],
) -> Result<serde_json::Value, String> {
    let op = i16::try_from(row.op).map_err(|error| error.to_string())?;
    let mut cleanup = serde_json::Map::new();
    for column in primary_key_columns {
        let value = row
            .fields
            .get(column)
            .ok_or_else(|| format!("flush row is missing primary-key field `{column}`"))?;
        cleanup.insert(
            column.clone(),
            serde_json::Value::String(cleanup_text_value(value)?),
        );
    }
    cleanup.insert("seq".to_string(), serde_json::json!(row.seq));
    cleanup.insert("op".to_string(), serde_json::json!(op));
    Ok(serde_json::Value::Object(cleanup))
}

fn cleanup_text_value(value: &serde_json::Value) -> Result<String, String> {
    match value {
        serde_json::Value::Null => {
            Err("cleanup row cannot contain null primary-key values".to_string())
        }
        serde_json::Value::String(text) => Ok(text.clone()),
        serde_json::Value::Number(number) => Ok(number.to_string()),
        serde_json::Value::Bool(flag) => Ok(flag.to_string()),
        other => serde_json::to_string(other).map_err(|error| error.to_string()),
    }
}

/// Plans flush write input from mirror selection rows.
///
/// # Errors
///
/// Returns an error when any row cannot be planned.
pub fn plan_flush_write_input(
    rows: Vec<MirrorSelectionRow>,
    columns: Vec<koldstore_parquet::PgColumn>,
    base_columns: &[String],
    primary_key_columns: &[String],
    schema_version: u32,
) -> Result<FlushWriteInput, String> {
    let planned_rows = rows
        .iter()
        .cloned()
        .map(|row| plan_flush_cold_record(row, base_columns, primary_key_columns, schema_version))
        .collect::<Result<Vec<_>, _>>()?;
    let cleanup_rows = rows
        .iter()
        .map(|row| plan_flush_cleanup_row(row, primary_key_columns))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(FlushWriteInput {
        columns,
        rows: planned_rows,
        cleanup_rows,
    })
}
