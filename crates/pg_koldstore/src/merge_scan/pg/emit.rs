//! Typed cell → PostgreSQL Datum conversion for merge-scan projection.

use std::ffi::CString;

use koldstore_common::{CellValue, RowImage};
use koldstore_schema::PgType;
use pgrx::pg_sys;

use super::qual::ScanProjection;
use super::tuple::MaterializedRow;

/// Builds one projected row from a winner [`RowImage`].
///
/// Allocations must run inside the scan [`super::tuple::ScanMemory`] context.
///
/// # Errors
///
/// Returns an error when a non-null cell cannot be converted to the catalog type.
pub(super) unsafe fn materialize_scan_row_from_image(
    row_image: &RowImage,
    projection: &ScanProjection,
) -> Result<MaterializedRow, String> {
    let mut values = Vec::with_capacity(projection.columns.len());
    let mut is_null = Vec::with_capacity(projection.columns.len());
    for projected in &projection.columns {
        let Some(value) = row_image.get(&projected.catalog.name) else {
            return Err(format!(
                "projected column `{}` is missing from merged row image",
                projected.catalog.name
            ));
        };
        if value.is_null() {
            values.push(pg_sys::Datum::null());
            is_null.push(true);
            continue;
        }
        values.push(cell_value_to_datum(value, projected.catalog.pg_type)?);
        is_null.push(false);
    }
    Ok(MaterializedRow { values, is_null })
}

unsafe fn cell_value_to_datum(value: &CellValue, pg_type: PgType) -> Result<pg_sys::Datum, String> {
    match (pg_type, value) {
        (PgType::Bool, CellValue::Bool(flag)) => Ok(pg_sys::Datum::from(*flag)),
        (PgType::Int2, cell) => {
            let number = cell_i64(cell)?;
            let narrowed = i16::try_from(number).map_err(|error| error.to_string())?;
            Ok(pg_sys::Datum::from(i32::from(narrowed)))
        }
        (PgType::Int4, cell) => {
            let number = cell_i64(cell)?;
            let narrowed = i32::try_from(number).map_err(|error| error.to_string())?;
            Ok(pg_sys::Datum::from(narrowed))
        }
        (PgType::Int8, cell) => Ok(pg_sys::Datum::from(cell_i64(cell)?)),
        (PgType::Float4, cell) => {
            let number = cell_f64(cell)? as f32;
            Ok(pg_sys::Datum::from(f32::to_bits(number)))
        }
        (PgType::Float8, cell) => {
            let number = cell_f64(cell)?;
            Ok(pg_sys::Datum::from(f64::to_bits(number)))
        }
        (
            PgType::Text
            | PgType::Numeric
            | PgType::Uuid
            | PgType::Jsonb
            | PgType::TextArray
            | PgType::Bytea,
            _,
        ) => input_datum_from_text(&cell_input_text(value, pg_type)?, pg_type),
        // Native hot / cold Arrow decode store TimestampTzADT microseconds;
        // ISO strings still come from SPI `to_jsonb` Utf8 cells.
        (PgType::Timestamptz, CellValue::TimestamptzMicros(micros)) => {
            Ok(pg_sys::Datum::from(*micros))
        }
        (PgType::Timestamptz, CellValue::Int64(micros)) => Ok(pg_sys::Datum::from(*micros)),
        (PgType::Timestamptz, _) => {
            input_datum_from_text(&cell_input_text(value, pg_type)?, pg_type)
        }
        _ => Err(format!(
            "cannot convert cell {value:?} to PostgreSQL type {pg_type:?}"
        )),
    }
}

fn cell_i64(value: &CellValue) -> Result<i64, String> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<i64>().ok()))
        .ok_or_else(|| format!("expected integer, got {value:?}"))
}

fn cell_f64(value: &CellValue) -> Result<f64, String> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|n| n as f64))
        .or_else(|| value.as_str().and_then(|text| text.parse::<f64>().ok()))
        .ok_or_else(|| format!("expected float, got {value:?}"))
}

fn cell_input_text(value: &CellValue, pg_type: PgType) -> Result<String, String> {
    match pg_type {
        PgType::Jsonb => match value {
            CellValue::Utf8(text) => Ok(text.clone()),
            other => Ok(other.to_json().to_string()),
        },
        PgType::Text | PgType::Uuid | PgType::Numeric | PgType::Timestamptz | PgType::Bytea => {
            match value {
                CellValue::Utf8(text) => Ok(text.clone()),
                CellValue::Bool(flag) => Ok(flag.to_string()),
                CellValue::Int16(n) => Ok(n.to_string()),
                CellValue::Int32(n) => Ok(n.to_string()),
                CellValue::Int64(n) | CellValue::TimestamptzMicros(n) => Ok(n.to_string()),
                CellValue::Float32(n) => Ok(n.to_string()),
                CellValue::Float64(n) => Ok(n.to_string()),
                CellValue::Null => Err(format!("expected scalar for {:?}, got null", pg_type)),
            }
        }
        PgType::TextArray => match value {
            CellValue::Utf8(text) => Ok(text.clone()),
            other => {
                let json = other.to_json();
                match json {
                    serde_json::Value::Array(items) => {
                        let mut parts = Vec::with_capacity(items.len());
                        for item in items {
                            let text = item
                                .as_str()
                                .map(str::to_string)
                                .unwrap_or_else(|| item.to_string());
                            parts.push(format!("\"{}\"", text.replace('"', "\\\"")));
                        }
                        Ok(format!("{{{}}}", parts.join(",")))
                    }
                    serde_json::Value::String(text) => Ok(text),
                    _ => Err(format!("expected text array, got {other:?}")),
                }
            }
        },
        _ => Err(format!("unsupported text input for {:?}", pg_type)),
    }
}

unsafe fn input_datum_from_text(text: &str, pg_type: PgType) -> Result<pg_sys::Datum, String> {
    let type_oid = pg_sys::Oid::from(pg_type.type_oid());
    let mut typinput = pg_sys::InvalidOid;
    let mut typioparam = pg_sys::InvalidOid;
    pg_sys::getTypeInputInfo(type_oid, &mut typinput, &mut typioparam);
    let cstr = CString::new(text).map_err(|error| error.to_string())?;
    Ok(pg_sys::OidInputFunctionCall(
        typinput,
        cstr.as_ptr() as *mut std::os::raw::c_char,
        typioparam,
        -1,
    ))
}
