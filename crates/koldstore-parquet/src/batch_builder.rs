//! Incremental Arrow record-batch builder for flush rows.
//!
//! PERFORMANCE: Builds columnar Arrow arrays in a single pass while rows stream
//! from SPI. Avoids per-row `BTreeMap` retention plus a second full-table scan
//! when converting planned rows to Parquet.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    StringBuilder, TimestampMicrosecondBuilder, UInt32Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use koldstore_common::compare_json_values;
use koldstore_schema::PgType;

use crate::pg_type_codec::{json_bool, json_f32, json_f64, json_i16, json_i64, json_string_cell};
use crate::schema::{build_clean_arrow_schema, ColdMetadataColumn, PgColumn};
use crate::writer::CleanColdRecordPlan;

/// One mirror row decoded from SPI for flush encoding.
#[derive(Debug, Clone, PartialEq)]
pub struct FlushMirrorRow {
    /// Mirror sequence.
    pub seq: i64,
    /// Mirror operation code.
    pub op: i16,
    /// Application column values in catalog order.
    pub values: Vec<FlushColumnValue>,
}

/// Resolves catalog positions for primary-key columns.
///
/// # Errors
///
/// Returns an error when a primary-key column is absent from the catalog.
pub fn pk_column_indices(
    columns: &[impl AsRef<str>],
    pk_columns: &[String],
) -> Result<Vec<usize>, String> {
    pk_columns
        .iter()
        .map(|pk| {
            columns
                .iter()
                .position(|column| column.as_ref() == pk)
                .ok_or_else(|| format!("primary-key column `{pk}` is missing from catalog"))
        })
        .collect()
}

/// Builds one cleanup JSON object for post-flush hot/mirror pruning.
///
/// Only primary-key cells are serialized; full row payloads are not materialized.
///
/// # Errors
///
/// Returns an error when a primary-key value is null or missing.
pub fn cleanup_row_json(
    pk_columns: &[String],
    pk_indices: &[usize],
    values: &[FlushColumnValue],
    seq: i64,
    op: i16,
) -> Result<serde_json::Value, String> {
    let mut cleanup = serde_json::Map::new();
    for (pk, index) in pk_columns.iter().zip(pk_indices) {
        let value = values
            .get(*index)
            .ok_or_else(|| format!("flush row is missing primary-key field `{pk}`"))?;
        cleanup.insert(pk.clone(), flush_cell_to_cleanup_json(value)?);
    }
    cleanup.insert("seq".to_string(), serde_json::json!(seq));
    cleanup.insert("op".to_string(), serde_json::json!(op));
    Ok(serde_json::Value::Object(cleanup))
}

fn flush_cell_to_cleanup_json(value: &FlushColumnValue) -> Result<serde_json::Value, String> {
    match value {
        FlushColumnValue::Null => {
            Err("cleanup row cannot contain null primary-key values".to_string())
        }
        FlushColumnValue::Bool(value) => Ok(serde_json::json!(value)),
        FlushColumnValue::Int16(value) => Ok(serde_json::json!(value)),
        FlushColumnValue::Int32(value) => Ok(serde_json::json!(value)),
        FlushColumnValue::Int64(value) => Ok(serde_json::json!(value)),
        FlushColumnValue::Float32(value) => Ok(serde_json::json!(value)),
        FlushColumnValue::Float64(value) => Ok(serde_json::json!(value)),
        FlushColumnValue::Utf8(value) => Ok(serde_json::Value::String(value.clone())),
        FlushColumnValue::TimestamptzMicros(_) => Ok(serde_json::Value::String(
            flush_cell_to_cleanup_text(value)?,
        )),
    }
}

fn flush_cell_to_cleanup_text(value: &FlushColumnValue) -> Result<String, String> {
    match value {
        FlushColumnValue::Null => {
            Err("cleanup row cannot contain null primary-key values".to_string())
        }
        FlushColumnValue::Bool(value) => Ok(value.to_string()),
        FlushColumnValue::Int16(value) => Ok(value.to_string()),
        FlushColumnValue::Int32(value) => Ok(value.to_string()),
        FlushColumnValue::Int64(value) => Ok(value.to_string()),
        FlushColumnValue::Float32(value) => Ok(value.to_string()),
        FlushColumnValue::Float64(value) => Ok(value.to_string()),
        FlushColumnValue::Utf8(value) => Ok(value.clone()),
        FlushColumnValue::TimestamptzMicros(value) => {
            let timestamp = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(*value)
                .ok_or_else(|| "timestamp value out of range for cleanup row".to_string())?;
            Ok(timestamp.to_rfc3339())
        }
    }
}

/// One typed column value decoded from SPI or a planned cold row.
#[derive(Debug, Clone, PartialEq)]
pub enum FlushColumnValue {
    /// SQL NULL.
    Null,
    /// Boolean column.
    Bool(bool),
    /// `int2`.
    Int16(i16),
    /// `int4`.
    Int32(i32),
    /// `int8`.
    Int64(i64),
    /// `float4`.
    Float32(f32),
    /// `float8`.
    Float64(f64),
    /// Text-like columns (`text`, `jsonb`, `uuid`, `bytea`, `numeric`, `text[]`).
    Utf8(String),
    /// `timestamptz` stored as UTC micros.
    TimestamptzMicros(i64),
}

/// Finished cold row batch plus chunk-level stats captured while building.
#[derive(Debug, Clone)]
pub struct ColdRecordBatch {
    /// Arrow batch ready for Parquet encoding.
    pub batch: RecordBatch,
    /// Minimum mirror `seq` in the chunk.
    pub min_seq: i64,
    /// Maximum mirror `seq` in the chunk.
    pub max_seq: i64,
    /// Number of logical rows encoded.
    pub row_count: usize,
    /// Running min/max for indexed columns (non-delete rows only).
    pub indexed_bounds: BTreeMap<String, (serde_json::Value, serde_json::Value)>,
}

enum TypedColumnBuilder {
    Bool(BooleanBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Timestamptz(TimestampMicrosecondBuilder),
}

impl TypedColumnBuilder {
    fn new(pg_type: PgType) -> Self {
        match pg_type {
            PgType::Bool => Self::Bool(BooleanBuilder::new()),
            PgType::Int2 => Self::Int16(Int16Builder::new()),
            PgType::Int4 => Self::Int32(Int32Builder::new()),
            PgType::Int8 => Self::Int64(Int64Builder::new()),
            PgType::Float4 => Self::Float32(Float32Builder::new()),
            PgType::Float8 => Self::Float64(Float64Builder::new()),
            PgType::Text
            | PgType::Numeric
            | PgType::Uuid
            | PgType::Jsonb
            | PgType::TextArray
            | PgType::Bytea => Self::Utf8(StringBuilder::new()),
            PgType::Timestamptz => Self::Timestamptz(TimestampMicrosecondBuilder::new()),
        }
    }

    fn append(&mut self, value: Option<&FlushColumnValue>) -> Result<(), String> {
        match self {
            Self::Bool(builder) => match value {
                None | Some(FlushColumnValue::Null) => builder.append_null(),
                Some(FlushColumnValue::Bool(value)) => builder.append_value(*value),
                Some(other) => return Err(format!("expected boolean flush value, got {other:?}")),
            },
            Self::Int16(builder) => match value {
                None | Some(FlushColumnValue::Null) => builder.append_null(),
                Some(FlushColumnValue::Int16(value)) => builder.append_value(*value),
                Some(other) => return Err(format!("expected int2 flush value, got {other:?}")),
            },
            Self::Int32(builder) => match value {
                None | Some(FlushColumnValue::Null) => builder.append_null(),
                Some(FlushColumnValue::Int32(value)) => builder.append_value(*value),
                Some(other) => return Err(format!("expected int4 flush value, got {other:?}")),
            },
            Self::Int64(builder) => match value {
                None | Some(FlushColumnValue::Null) => builder.append_null(),
                Some(FlushColumnValue::Int64(value)) => builder.append_value(*value),
                Some(other) => return Err(format!("expected int8 flush value, got {other:?}")),
            },
            Self::Float32(builder) => match value {
                None | Some(FlushColumnValue::Null) => builder.append_null(),
                Some(FlushColumnValue::Float32(value)) => builder.append_value(*value),
                Some(other) => return Err(format!("expected float4 flush value, got {other:?}")),
            },
            Self::Float64(builder) => match value {
                None | Some(FlushColumnValue::Null) => builder.append_null(),
                Some(FlushColumnValue::Float64(value)) => builder.append_value(*value),
                Some(other) => return Err(format!("expected float8 flush value, got {other:?}")),
            },
            Self::Utf8(builder) => match value {
                None | Some(FlushColumnValue::Null) => builder.append_null(),
                Some(FlushColumnValue::Utf8(value)) => builder.append_value(value),
                Some(other) => return Err(format!("expected utf8 flush value, got {other:?}")),
            },
            Self::Timestamptz(builder) => match value {
                None | Some(FlushColumnValue::Null) => builder.append_null(),
                Some(FlushColumnValue::TimestamptzMicros(value)) => builder.append_value(*value),
                Some(other) => {
                    return Err(format!("expected timestamptz flush value, got {other:?}"));
                }
            },
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Bool(mut builder) => Arc::new(builder.finish()),
            Self::Int16(mut builder) => Arc::new(builder.finish()),
            Self::Int32(mut builder) => Arc::new(builder.finish()),
            Self::Int64(mut builder) => Arc::new(builder.finish()),
            Self::Float32(mut builder) => Arc::new(builder.finish()),
            Self::Float64(mut builder) => Arc::new(builder.finish()),
            Self::Utf8(mut builder) => Arc::new(builder.finish()),
            Self::Timestamptz(mut builder) => Arc::new(builder.finish()),
        }
    }
}

/// Incremental builder for one Parquet segment chunk.
pub struct CleanColdRecordBatchBuilder {
    schema: SchemaRef,
    columns: Vec<PgColumn>,
    builders: Vec<TypedColumnBuilder>,
    seq_builder: Int64Builder,
    op_builder: Int16Builder,
    deleted_builder: BooleanBuilder,
    schema_version_builder: UInt32Builder,
    indexed_columns: Vec<String>,
    indexed_bounds: BTreeMap<String, (serde_json::Value, serde_json::Value)>,
    min_seq: Option<i64>,
    max_seq: Option<i64>,
    row_count: usize,
}

impl CleanColdRecordBatchBuilder {
    /// Returns the number of rows appended so far.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Returns the application columns encoded by this builder.
    #[must_use]
    pub fn columns(&self) -> &[PgColumn] {
        &self.columns
    }

    /// Returns indexed columns tracked for segment stats.
    #[must_use]
    pub fn indexed_columns(&self) -> &[String] {
        &self.indexed_columns
    }

    /// Creates a builder for one flush chunk.
    ///
    /// # Errors
    ///
    /// Returns an error when the Arrow schema cannot be built.
    pub fn new(columns: &[PgColumn], indexed_columns: &[String]) -> Result<Self, String> {
        Ok(Self {
            schema: Arc::new(build_clean_arrow_schema(columns).map_err(|error| error.to_string())?),
            builders: columns
                .iter()
                .map(|column| TypedColumnBuilder::new(column.pg_type))
                .collect(),
            columns: columns.to_vec(),
            seq_builder: Int64Builder::new(),
            op_builder: Int16Builder::new(),
            deleted_builder: BooleanBuilder::new(),
            schema_version_builder: UInt32Builder::new(),
            indexed_columns: indexed_columns.to_vec(),
            indexed_bounds: BTreeMap::new(),
            min_seq: None,
            max_seq: None,
            row_count: 0,
        })
    }

    /// Appends one typed mirror row without an intermediate JSON map.
    ///
    /// # Errors
    ///
    /// Returns an error when delete markers omit a primary-key value or a cell
    /// type does not match the column schema.
    pub fn push_typed_row(
        &mut self,
        column_values: &[FlushColumnValue],
        primary_key_columns: &[String],
        seq: i64,
        op: i16,
        schema_version: u32,
    ) -> Result<(), String> {
        if !matches!(op, 1..=3) {
            return Err(format!("unsupported mirror operation code {op}"));
        }
        if column_values.len() != self.columns.len() {
            return Err(format!(
                "flush row column count mismatch: expected {}, got {}",
                self.columns.len(),
                column_values.len()
            ));
        }

        let deleted = op == 3;
        for ((column, builder), value) in self
            .columns
            .iter()
            .zip(self.builders.iter_mut())
            .zip(column_values.iter())
        {
            let cell = if (deleted && !primary_key_columns.iter().any(|pk| pk == &column.name))
                || matches!(value, FlushColumnValue::Null)
            {
                None
            } else {
                Some(value)
            };
            builder.append(cell)?;
        }

        self.seq_builder.append_value(seq);
        self.op_builder.append_value(op);
        self.deleted_builder.append_value(deleted);
        self.schema_version_builder.append_value(schema_version);
        self.min_seq = Some(self.min_seq.map_or(seq, |current| current.min(seq)));
        self.max_seq = Some(self.max_seq.map_or(seq, |current| current.max(seq)));
        self.row_count += 1;

        for column_name in &self.indexed_columns {
            if deleted && !primary_key_columns.iter().any(|pk| pk == column_name) {
                continue;
            }
            let Some(column) = self
                .columns
                .iter()
                .find(|column| column.name == *column_name)
            else {
                continue;
            };
            let column_index = self
                .columns
                .iter()
                .position(|entry| entry.name == column.name)
                .expect("indexed column is present");
            let value = &column_values[column_index];
            if matches!(value, FlushColumnValue::Null) {
                continue;
            }
            let json = flush_value_to_json(value);
            update_indexed_bounds(&mut self.indexed_bounds, column_name, &json)?;
        }
        Ok(())
    }

    /// Appends one planned clean cold row (legacy/test path).
    ///
    /// # Errors
    ///
    /// Returns an error when metadata is missing or a JSON cell cannot be coerced.
    pub fn push_plan(&mut self, row: &CleanColdRecordPlan) -> Result<(), String> {
        let seq = json_i64(row.values.get(ColdMetadataColumn::Seq.name()))?
            .ok_or_else(|| "flush row is missing integer field `seq`".to_string())?;
        let op = json_i16(row.values.get(ColdMetadataColumn::Op.name()))?
            .ok_or_else(|| "flush row is missing integer field `op`".to_string())?;
        let schema_version = row
            .values
            .get(ColdMetadataColumn::SchemaVersion.name())
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| "flush row is missing integer field `schema_version`".to_string())?;

        for (builder, column) in self.builders.iter_mut().zip(self.columns.iter()) {
            let cell = plan_value_to_flush_cell(column.pg_type, row.values.get(&column.name))?;
            builder.append(if matches!(cell, FlushColumnValue::Null) {
                None
            } else {
                Some(&cell)
            })?;
        }

        self.seq_builder.append_value(seq);
        self.op_builder.append_value(op);
        self.deleted_builder.append_value(row.deleted);
        self.schema_version_builder
            .append_value(u32::try_from(schema_version).map_err(|error| error.to_string())?);
        self.min_seq = Some(self.min_seq.map_or(seq, |current| current.min(seq)));
        self.max_seq = Some(self.max_seq.map_or(seq, |current| current.max(seq)));
        self.row_count += 1;

        for column_name in &self.indexed_columns {
            let Some(value) = row.values.get(column_name) else {
                continue;
            };
            if value.is_null() {
                continue;
            }
            update_indexed_bounds(&mut self.indexed_bounds, column_name, value)?;
        }
        Ok(())
    }

    /// Finalizes the Arrow batch and chunk stats.
    ///
    /// # Errors
    ///
    /// Returns an error when the batch is empty or Arrow assembly fails.
    pub fn finish(mut self) -> Result<ColdRecordBatch, String> {
        if self.row_count == 0 {
            return Err("flush chunk builder is empty".to_string());
        }
        let mut arrays = Vec::with_capacity(self.columns.len() + 4);
        for builder in self.builders {
            arrays.push(builder.finish());
        }
        arrays.push(Arc::new(self.seq_builder.finish()));
        arrays.push(Arc::new(self.op_builder.finish()));
        arrays.push(Arc::new(self.deleted_builder.finish()));
        arrays.push(Arc::new(self.schema_version_builder.finish()));
        let batch =
            RecordBatch::try_new(self.schema.clone(), arrays).map_err(|error| error.to_string())?;
        Ok(ColdRecordBatch {
            batch,
            min_seq: self.min_seq.expect("row_count > 0"),
            max_seq: self.max_seq.expect("row_count > 0"),
            row_count: self.row_count,
            indexed_bounds: self.indexed_bounds,
        })
    }
}

fn plan_value_to_flush_cell(
    pg_type: PgType,
    value: Option<&serde_json::Value>,
) -> Result<FlushColumnValue, String> {
    if value.is_none() || matches!(value, Some(serde_json::Value::Null)) {
        return Ok(FlushColumnValue::Null);
    }
    let value = value.expect("checked for null");
    match pg_type {
        PgType::Bool => Ok(FlushColumnValue::Bool(
            json_bool(Some(value))?.expect("non-null"),
        )),
        PgType::Int2 => Ok(FlushColumnValue::Int16(
            json_i16(Some(value))?.expect("non-null"),
        )),
        PgType::Int4 => Ok(FlushColumnValue::Int32(
            json_i64(Some(value))?
                .and_then(|value| i32::try_from(value).ok())
                .ok_or_else(|| format!("int4 value out of range: {value}"))?,
        )),
        PgType::Int8 => Ok(FlushColumnValue::Int64(
            json_i64(Some(value))?.expect("non-null"),
        )),
        PgType::Float4 => Ok(FlushColumnValue::Float32(
            json_f32(Some(value))?.expect("non-null"),
        )),
        PgType::Float8 => Ok(FlushColumnValue::Float64(
            json_f64(Some(value))?.expect("non-null"),
        )),
        PgType::Text
        | PgType::Numeric
        | PgType::Uuid
        | PgType::Jsonb
        | PgType::TextArray
        | PgType::Bytea => Ok(FlushColumnValue::Utf8(
            json_string_cell(Some(value))?.expect("non-null"),
        )),
        PgType::Timestamptz => {
            let text = json_string_cell(Some(value))?.expect("non-null");
            let micros = chrono::DateTime::parse_from_rfc3339(&text)
                .or_else(|_| chrono::DateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f%:z"))
                .map(|timestamp| timestamp.timestamp_micros())
                .map_err(|error| format!("unsupported timestamp literal `{text}`: {error}"))?;
            Ok(FlushColumnValue::TimestamptzMicros(micros))
        }
    }
}

fn flush_value_to_json(value: &FlushColumnValue) -> serde_json::Value {
    match value {
        FlushColumnValue::Null => serde_json::Value::Null,
        FlushColumnValue::Bool(value) => serde_json::json!(value),
        FlushColumnValue::Int16(value) => serde_json::json!(value),
        FlushColumnValue::Int32(value) => serde_json::json!(value),
        FlushColumnValue::Int64(value) => serde_json::json!(value),
        FlushColumnValue::Float32(value) => serde_json::json!(value),
        FlushColumnValue::Float64(value) => serde_json::json!(value),
        FlushColumnValue::Utf8(value) => serde_json::Value::String(value.clone()),
        FlushColumnValue::TimestamptzMicros(value) => {
            let timestamp = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(*value)
                .unwrap_or_else(chrono::Utc::now);
            serde_json::Value::String(timestamp.to_rfc3339())
        }
    }
}

fn update_indexed_bounds(
    bounds: &mut BTreeMap<String, (serde_json::Value, serde_json::Value)>,
    column: &str,
    value: &serde_json::Value,
) -> Result<(), String> {
    match bounds.get_mut(column) {
        None => {
            bounds.insert(column.to_string(), (value.clone(), value.clone()));
        }
        Some((min, max)) => {
            if compare_json_values(value, min).is_some_and(|ordering| ordering.is_lt()) {
                *min = value.clone();
            }
            if compare_json_values(value, max).is_some_and(|ordering| ordering.is_gt()) {
                *max = value.clone();
            }
        }
    }
    Ok(())
}
