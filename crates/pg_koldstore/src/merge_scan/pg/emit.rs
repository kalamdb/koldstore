//! JSON cell → PostgreSQL Datum conversion for merge-scan projection.

use std::ffi::CString;

use koldstore_schema::PgType;
use pgrx::pg_sys;

use super::tuple::MaterializedRow;

/// Builds one projected row from a winner `row_image` JSON object.
///
/// Allocations must run inside the scan [`super::tuple::ScanMemory`] context.
///
/// # Errors
///
/// Returns an error when a non-null cell cannot be converted to the catalog type.
pub(super) unsafe fn materialize_row_from_image(
    row_image: &serde_json::Value,
    columns: &[&koldstore_migrate::order::CatalogColumn],
) -> Result<MaterializedRow, String> {
    let mut values = Vec::with_capacity(columns.len());
    let mut is_null = Vec::with_capacity(columns.len());
    for column in columns {
        match row_image.get(&column.name) {
            None | Some(serde_json::Value::Null) => {
                values.push(pg_sys::Datum::null());
                is_null.push(true);
            }
            Some(value) => {
                values.push(json_value_to_datum(value, column.pg_type)?);
                is_null.push(false);
            }
        }
    }
    Ok(MaterializedRow { values, is_null })
}

unsafe fn json_value_to_datum(
    value: &serde_json::Value,
    pg_type: PgType,
) -> Result<pg_sys::Datum, String> {
    match pg_type {
        PgType::Bool => {
            let flag = value
                .as_bool()
                .ok_or_else(|| format!("expected bool, got {value}"))?;
            Ok(pg_sys::Datum::from(flag))
        }
        PgType::Int2 => {
            let number = json_i64(value)?;
            let narrowed = i16::try_from(number).map_err(|error| error.to_string())?;
            Ok(pg_sys::Datum::from(i32::from(narrowed)))
        }
        PgType::Int4 => {
            let number = json_i64(value)?;
            let narrowed = i32::try_from(number).map_err(|error| error.to_string())?;
            Ok(pg_sys::Datum::from(narrowed))
        }
        PgType::Int8 => Ok(pg_sys::Datum::from(json_i64(value)?)),
        PgType::Float4 => {
            let number = json_f64(value)? as f32;
            Ok(pg_sys::Datum::from(f32::to_bits(number)))
        }
        PgType::Float8 => {
            let number = json_f64(value)?;
            Ok(pg_sys::Datum::from(f64::to_bits(number)))
        }
        PgType::Text
        | PgType::Numeric
        | PgType::Uuid
        | PgType::Jsonb
        | PgType::TextArray
        | PgType::Bytea
        | PgType::Timestamptz => input_datum_from_text(&json_input_text(value, pg_type)?, pg_type),
    }
}

fn json_i64(value: &serde_json::Value) -> Result<i64, String> {
    if let Some(number) = value.as_i64() {
        return Ok(number);
    }
    if let Some(number) = value.as_u64() {
        return i64::try_from(number).map_err(|error| error.to_string());
    }
    if let Some(text) = value.as_str() {
        return text
            .parse::<i64>()
            .map_err(|error| format!("invalid integer `{text}`: {error}"));
    }
    Err(format!("expected integer, got {value}"))
}

fn json_f64(value: &serde_json::Value) -> Result<f64, String> {
    if let Some(number) = value.as_f64() {
        return Ok(number);
    }
    if let Some(text) = value.as_str() {
        return text
            .parse::<f64>()
            .map_err(|error| format!("invalid float `{text}`: {error}"));
    }
    Err(format!("expected float, got {value}"))
}

fn json_input_text(value: &serde_json::Value, pg_type: PgType) -> Result<String, String> {
    match pg_type {
        PgType::Jsonb => Ok(value.to_string()),
        PgType::Text | PgType::Uuid | PgType::Numeric | PgType::Timestamptz | PgType::Bytea => {
            if let Some(text) = value.as_str() {
                Ok(text.to_string())
            } else if value.is_number() || value.as_bool().is_some() {
                Ok(value.to_string())
            } else {
                Err(format!("expected scalar for {:?}, got {value}", pg_type))
            }
        }
        PgType::TextArray => match value {
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
            serde_json::Value::String(text) => Ok(text.clone()),
            _ => Err(format!("expected text array, got {value}")),
        },
        _ => Err(format!("unsupported text input for {:?}", pg_type)),
    }
}

unsafe fn input_datum_from_text(text: &str, pg_type: PgType) -> Result<pg_sys::Datum, String> {
    let type_oid = pg_sys::Oid::from(pg_type_oid(pg_type));
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

const fn pg_type_oid(pg_type: PgType) -> u32 {
    match pg_type {
        PgType::Bool => 16,
        PgType::Int2 => 21,
        PgType::Int4 => 23,
        PgType::Int8 => 20,
        PgType::Float4 => 700,
        PgType::Float8 => 701,
        PgType::Text => 25,
        PgType::Numeric => 1700,
        PgType::Uuid => 2950,
        PgType::Jsonb => 3802,
        PgType::TextArray => 1009,
        PgType::Bytea => 17,
        PgType::Timestamptz => 1184,
    }
}
