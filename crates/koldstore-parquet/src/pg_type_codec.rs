//! PostgreSQL [`PgType`] ↔ Arrow codec.
//!
//! All Arrow physical-type mapping and JSON cell coercion for supported PostgreSQL
//! column types lives here. Schema crates own [`PgType`] itself; this module owns
//! every Arrow and JSON value conversion boundary.

use std::borrow::Cow;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use koldstore_schema::PgType;

use crate::schema::PgColumn;

/// Returns the Arrow physical type for a supported PostgreSQL type.
#[must_use]
pub const fn arrow_data_type(pg_type: PgType) -> DataType {
    match pg_type {
        PgType::Bool => DataType::Boolean,
        PgType::Int2 => DataType::Int16,
        PgType::Int4 => DataType::Int32,
        PgType::Int8 => DataType::Int64,
        PgType::Float4 => DataType::Float32,
        PgType::Float8 => DataType::Float64,
        PgType::Text
        | PgType::Numeric
        | PgType::Uuid
        | PgType::Jsonb
        | PgType::TextArray
        | PgType::Bytea => DataType::Utf8,
        PgType::Timestamptz => DataType::Timestamp(TimeUnit::Microsecond, None),
    }
}

/// Builds an Arrow array for one application column across flush/cold rows.
///
/// # Errors
///
/// Returns an error when a JSON cell cannot be coerced to the column type.
pub fn arrow_array_for_column(
    column: &PgColumn,
    json_values: &[Option<&serde_json::Value>],
) -> Result<ArrayRef, String> {
    arrow_array_from_json(column.pg_type, &column.name, json_values)
        .map_err(|error| format!("column `{}`: {error}", column.name))
}

/// Builds an Arrow array from JSON cell values for one PostgreSQL type.
///
/// # Errors
///
/// Returns an error when a JSON cell cannot be coerced to the type.
pub fn arrow_array_from_json(
    pg_type: PgType,
    column_name: &str,
    json_values: &[Option<&serde_json::Value>],
) -> Result<ArrayRef, String> {
    let _ = column_name;
    let array = match pg_type {
        PgType::Bool => Arc::new(BooleanArray::from_iter(
            json_values
                .iter()
                .map(|value| json_bool(*value))
                .collect::<Result<Vec<_>, _>>()?,
        )) as ArrayRef,
        PgType::Int2 => Arc::new(Int16Array::from_iter(
            json_values
                .iter()
                .map(|value| json_i16(*value))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        PgType::Int4 => Arc::new(Int32Array::from_iter(
            json_values
                .iter()
                .map(|value| json_i32(*value))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        PgType::Int8 => Arc::new(Int64Array::from_iter(
            json_values
                .iter()
                .map(|value| json_i64(*value))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        PgType::Float4 => Arc::new(Float32Array::from_iter(
            json_values
                .iter()
                .map(|value| json_f32(*value))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        PgType::Float8 => Arc::new(Float64Array::from_iter(
            json_values
                .iter()
                .map(|value| json_f64(*value))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        PgType::Text
        | PgType::Numeric
        | PgType::Uuid
        | PgType::Jsonb
        | PgType::TextArray
        | PgType::Bytea => Arc::new(StringArray::from_iter(
            json_values
                .iter()
                .map(|value| json_string_cell(*value))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        PgType::Timestamptz => Arc::new(TimestampMicrosecondArray::from_iter(
            json_values
                .iter()
                .map(|value| json_timestamp_micros(*value))
                .collect::<Result<Vec<_>, _>>()?,
        )),
    };
    Ok(array)
}

/// Decodes one Arrow cell into JSON for a supported PostgreSQL column type.
///
/// # Errors
///
/// Returns an error when the Arrow physical type does not match the column type.
pub fn json_value_from_arrow_column(
    batch: &RecordBatch,
    column: &PgColumn,
    row_index: usize,
) -> Result<serde_json::Value, String> {
    let array = required_column(batch, &column.name)?;
    json_from_arrow_cell(column.pg_type, &column.name, array, row_index)
}

/// Decodes one Arrow cell into JSON for a supported PostgreSQL type.
///
/// # Errors
///
/// Returns an error when the Arrow physical type does not match the type.
pub fn json_from_arrow_cell(
    pg_type: PgType,
    column_name: &str,
    array: &dyn Array,
    row_index: usize,
) -> Result<serde_json::Value, String> {
    if array.is_null(row_index) {
        return Ok(serde_json::Value::Null);
    }
    match pg_type {
        PgType::Bool => Ok(serde_json::json!(array_for::<BooleanArray>(
            array,
            column_name
        )?
        .value(row_index))),
        PgType::Int2 => Ok(serde_json::json!(array_for::<Int16Array>(
            array,
            column_name
        )?
        .value(row_index))),
        PgType::Int4 => Ok(serde_json::json!(array_for::<Int32Array>(
            array,
            column_name
        )?
        .value(row_index))),
        PgType::Int8 => Ok(serde_json::json!(array_for::<Int64Array>(
            array,
            column_name
        )?
        .value(row_index))),
        PgType::Float4 => Ok(serde_json::json!(array_for::<Float32Array>(
            array,
            column_name
        )?
        .value(row_index))),
        PgType::Float8 => Ok(serde_json::json!(array_for::<Float64Array>(
            array,
            column_name
        )?
        .value(row_index))),
        PgType::Text
        | PgType::Numeric
        | PgType::Uuid
        | PgType::Jsonb
        | PgType::TextArray
        | PgType::Bytea => Ok(serde_json::Value::String(
            array_for::<StringArray>(array, column_name)?
                .value(row_index)
                .to_string(),
        )),
        PgType::Timestamptz => {
            let micros =
                array_for::<TimestampMicrosecondArray>(array, column_name)?.value(row_index);
            let timestamp = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(micros)
                .ok_or_else(|| format!("timestamp value out of range in `{column_name}`"))?;
            Ok(serde_json::Value::String(timestamp.to_rfc3339()))
        }
    }
}

fn required_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a dyn Array, String> {
    batch
        .column_by_name(name)
        .map(|column| column.as_ref())
        .ok_or_else(|| format!("cold segment is missing required column `{name}`"))
}

fn array_for<'a, T: 'static>(array: &'a dyn Array, name: &str) -> Result<&'a T, String> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("cold column `{name}` has unexpected Arrow type"))
}

/// Coerces a JSON cell into an optional boolean.
pub fn json_bool(value: Option<&serde_json::Value>) -> Result<Option<bool>, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Bool(value)) => Ok(Some(*value)),
        Some(other) => Err(format!("expected boolean JSON value, got {other}")),
    }
}

/// Coerces a JSON cell into an optional `i16`.
pub fn json_i16(value: Option<&serde_json::Value>) -> Result<Option<i16>, String> {
    json_i64(value)?
        .map(|value| i16::try_from(value).map_err(|error| error.to_string()))
        .transpose()
}

/// Coerces a JSON cell into an optional `u32`.
pub fn json_u32(value: Option<&serde_json::Value>) -> Result<Option<u32>, String> {
    json_i64(value)?
        .map(|value| u32::try_from(value).map_err(|error| error.to_string()))
        .transpose()
}

fn json_i32(value: Option<&serde_json::Value>) -> Result<Option<i32>, String> {
    json_i64(value)?
        .map(|value| i32::try_from(value).map_err(|error| error.to_string()))
        .transpose()
}

/// Coerces a JSON cell into an optional `i64`.
pub fn json_i64(value: Option<&serde_json::Value>) -> Result<Option<i64>, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(value)) => value
            .as_i64()
            .map(Some)
            .ok_or_else(|| format!("expected integer JSON value, got {value}")),
        Some(other) => Err(format!("expected integer JSON value, got {other}")),
    }
}

fn json_f32(value: Option<&serde_json::Value>) -> Result<Option<f32>, String> {
    json_f64(value)?
        .map(|value| {
            if value.is_finite() && value >= f32::MIN as f64 && value <= f32::MAX as f64 {
                Ok(value as f32)
            } else {
                Err(format!("float32 out of range: {value}"))
            }
        })
        .transpose()
}

fn json_f64(value: Option<&serde_json::Value>) -> Result<Option<f64>, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(value)) => value
            .as_f64()
            .map(Some)
            .ok_or_else(|| format!("expected float JSON value, got {value}")),
        Some(other) => Err(format!("expected float JSON value, got {other}")),
    }
}

fn json_string_cell(value: Option<&serde_json::Value>) -> Result<Option<String>, String> {
    match json_string_borrowed(value)? {
        None => Ok(None),
        Some(Cow::Borrowed(value)) => Ok(Some(value.to_string())),
        Some(Cow::Owned(value)) => Ok(Some(value)),
    }
}

fn json_string_borrowed(value: Option<&serde_json::Value>) -> Result<Option<Cow<'_, str>>, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(value)) => Ok(Some(Cow::Borrowed(value.as_str()))),
        Some(other) => serde_json::to_string(other)
            .map(|value| Some(Cow::Owned(value)))
            .map_err(|error| error.to_string()),
    }
}

fn json_timestamp_micros(value: Option<&serde_json::Value>) -> Result<Option<i64>, String> {
    let Some(value) = json_string_borrowed(value)? else {
        return Ok(None);
    };
    parse_timestamp_micros(value.as_ref()).map(Some)
}

fn parse_timestamp_micros(value: &str) -> Result<i64, String> {
    chrono::DateTime::parse_from_rfc3339(value)
        .or_else(|_| chrono::DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%:z"))
        .map(|timestamp| timestamp.timestamp_micros())
        .map_err(|error| format!("unsupported timestamp literal `{value}`: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{arrow_array_from_json, arrow_data_type, json_from_arrow_cell};
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use koldstore_schema::PgType;
    use std::sync::Arc;

    #[test]
    fn arrow_data_type_maps_mvp_types() {
        assert_eq!(arrow_data_type(PgType::Int8), DataType::Int64);
        assert_eq!(
            arrow_data_type(PgType::Timestamptz),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn json_round_trip_preserves_integer_cells() {
        let value = serde_json::json!(42);
        let values = [Some(&value)];
        let array = arrow_array_from_json(PgType::Int8, "id", &values).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
            vec![array],
        )
        .unwrap();
        let decoded =
            json_from_arrow_cell(PgType::Int8, "id", batch.column(0).as_ref(), 0).unwrap();
        assert_eq!(decoded, serde_json::json!(42));
    }
}
