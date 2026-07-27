//! Encode and decode Sort Key V1 values through pinned Storekey.

use std::io::Cursor;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::error::SortKeyError;
use crate::types::{
    SortKeyType, SortKeyValue, PG_EPOCH_DAYS_FROM_UNIX, PG_EPOCH_MICROS_FROM_UNIX,
};

/// Encodes a Sort Key V1 value into order-preserving bytes.
///
/// # Errors
///
/// Returns [`SortKeyError::Encode`] when Storekey cannot serialize the value.
pub fn encode_sort_key(value: &SortKeyValue) -> Result<Vec<u8>, SortKeyError> {
    let encoded = match value {
        SortKeyValue::Bool(v) => storekey::encode_vec(v),
        SortKeyValue::Int2(v) => storekey::encode_vec(v),
        SortKeyValue::Int4(v) => storekey::encode_vec(v),
        SortKeyValue::Int8(v) => storekey::encode_vec(v),
        SortKeyValue::Date(v) => storekey::encode_vec(v),
        SortKeyValue::Timestamp(v) => storekey::encode_vec(v),
        SortKeyValue::Timestamptz(v) => storekey::encode_vec(v),
        SortKeyValue::Uuid(v) => storekey::encode_vec(v),
    }
    .map_err(|error| SortKeyError::Encode(error.to_string()))?;
    Ok(encoded)
}

/// Decodes Sort Key V1 bytes for a known type.
///
/// # Errors
///
/// Returns [`SortKeyError::Decode`] when the bytes do not match `ty`.
pub fn decode_sort_key(ty: SortKeyType, bytes: &[u8]) -> Result<SortKeyValue, SortKeyError> {
    match ty {
        SortKeyType::Bool => Ok(SortKeyValue::Bool(decode_one(bytes)?)),
        SortKeyType::Int2 => Ok(SortKeyValue::Int2(decode_one(bytes)?)),
        SortKeyType::Int4 => Ok(SortKeyValue::Int4(decode_one(bytes)?)),
        SortKeyType::Int8 => Ok(SortKeyValue::Int8(decode_one(bytes)?)),
        SortKeyType::Date => Ok(SortKeyValue::Date(decode_one(bytes)?)),
        SortKeyType::Timestamp => Ok(SortKeyValue::Timestamp(decode_one(bytes)?)),
        SortKeyType::Timestamptz => Ok(SortKeyValue::Timestamptz(decode_one(bytes)?)),
        SortKeyType::Uuid => Ok(SortKeyValue::Uuid(decode_one(bytes)?)),
    }
}

/// Parses a JSON catalog/query literal into a Sort Key V1 value for `ty`.
///
/// Integers for temporal types are PostgreSQL-epoch units (days / microseconds).
/// Temporal strings accept RFC3339 and common PostgreSQL text outputs and are
/// converted into those same units.
///
/// # Errors
///
/// Returns [`SortKeyError`] when the type is unsupported or the JSON shape is wrong.
pub fn encode_sort_key_json(ty: SortKeyType, value: &Value) -> Result<Vec<u8>, SortKeyError> {
    encode_sort_key(&parse_json_value(ty, value)?)
}

/// Encodes a PostgreSQL text-output scalar (as emitted by `pgoutput`) as Sort Key V1.
///
/// # Errors
///
/// Returns [`SortKeyError`] when `text` is not a valid literal for `ty`.
pub fn encode_sort_key_pg_text(ty: SortKeyType, text: &str) -> Result<Vec<u8>, SortKeyError> {
    let value = match ty {
        SortKeyType::Bool => Value::Bool(parse_pg_bool(text)?),
        SortKeyType::Int2 | SortKeyType::Int4 | SortKeyType::Int8 => {
            let n = text
                .parse::<i64>()
                .map_err(|error| invalid_json(ty_name(ty), error.to_string()))?;
            Value::Number(n.into())
        }
        SortKeyType::Date | SortKeyType::Timestamp | SortKeyType::Timestamptz | SortKeyType::Uuid => {
            Value::String(text.to_string())
        }
    };
    encode_sort_key_json(ty, &value)
}

fn parse_pg_bool(text: &str) -> Result<bool, SortKeyError> {
    match text {
        "t" | "true" | "TRUE" | "yes" | "on" | "1" => Ok(true),
        "f" | "false" | "FALSE" | "no" | "off" | "0" => Ok(false),
        other => Err(invalid_json(
            "bool",
            format!("expected boolean text, got `{other}`"),
        )),
    }
}

fn ty_name(ty: SortKeyType) -> &'static str {
    match ty {
        SortKeyType::Bool => "bool",
        SortKeyType::Int2 => "int2",
        SortKeyType::Int4 => "int4",
        SortKeyType::Int8 => "int8",
        SortKeyType::Date => "date",
        SortKeyType::Timestamp => "timestamp",
        SortKeyType::Timestamptz => "timestamptz",
        SortKeyType::Uuid => "uuid",
    }
}

fn parse_json_value(ty: SortKeyType, value: &Value) -> Result<SortKeyValue, SortKeyError> {
    match ty {
        SortKeyType::Bool => Ok(SortKeyValue::Bool(value.as_bool().ok_or_else(|| {
            invalid_json("bool", format!("expected boolean, got {value}"))
        })?)),
        SortKeyType::Int2 => Ok(SortKeyValue::Int2(parse_i64(value, "int2").and_then(
            |n| i16::try_from(n).map_err(|error| invalid_json("int2", error.to_string())),
        )?)),
        SortKeyType::Int4 => Ok(SortKeyValue::Int4(parse_i64(value, "int4").and_then(
            |n| i32::try_from(n).map_err(|error| invalid_json("int4", error.to_string())),
        )?)),
        SortKeyType::Int8 => Ok(SortKeyValue::Int8(parse_i64(value, "int8")?)),
        SortKeyType::Date => Ok(SortKeyValue::Date(parse_date_days(value)?)),
        SortKeyType::Timestamp => Ok(SortKeyValue::Timestamp(parse_timestamp_micros(value)?)),
        SortKeyType::Timestamptz => Ok(SortKeyValue::Timestamptz(parse_timestamptz_micros(value)?)),
        SortKeyType::Uuid => {
            let text = value
                .as_str()
                .ok_or_else(|| invalid_json("uuid", format!("expected string, got {value}")))?;
            let uuid = Uuid::parse_str(text)
                .map_err(|error| invalid_json("uuid", error.to_string()))?;
            Ok(SortKeyValue::Uuid(uuid))
        }
    }
}

fn parse_date_days(value: &Value) -> Result<i32, SortKeyError> {
    if let Some(n) = try_parse_i64(value) {
        return i32::try_from(n).map_err(|error| invalid_json("date", error.to_string()));
    }
    let text = value.as_str().ok_or_else(|| {
        invalid_json("date", format!("expected integer or date string, got {value}"))
    })?;
    let date = NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map_err(|error| invalid_json("date", format!("invalid date `{text}`: {error}")))?;
    let unix_days = date
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"))
        .num_days();
    i32::try_from(unix_days - i64::from(PG_EPOCH_DAYS_FROM_UNIX))
        .map_err(|error| invalid_json("date", error.to_string()))
}

fn parse_timestamp_micros(value: &Value) -> Result<i64, SortKeyError> {
    if let Some(n) = try_parse_i64(value) {
        return Ok(n);
    }
    let text = value.as_str().ok_or_else(|| {
        invalid_json(
            "timestamp",
            format!("expected integer or timestamp string, got {value}"),
        )
    })?;
    // `timestamp without time zone` literals are treated as UTC wall times for
    // Sort Key V1 encoding so flush JSON and query constants stay comparable.
    parse_temporal_string_to_pg_micros(text, "timestamp")
}

fn parse_timestamptz_micros(value: &Value) -> Result<i64, SortKeyError> {
    if let Some(n) = try_parse_i64(value) {
        return Ok(n);
    }
    let text = value.as_str().ok_or_else(|| {
        invalid_json(
            "timestamptz",
            format!("expected integer or timestamptz string, got {value}"),
        )
    })?;
    parse_temporal_string_to_pg_micros(text, "timestamptz")
}

fn parse_temporal_string_to_pg_micros(
    text: &str,
    expected: &'static str,
) -> Result<i64, SortKeyError> {
    let unix_micros = DateTime::parse_from_rfc3339(text)
        .map(|timestamp| timestamp.with_timezone(&Utc).timestamp_micros())
        .or_else(|_| {
            DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%z")
                .or_else(|_| DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%z"))
                .or_else(|_| DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z"))
                .or_else(|_| DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%#z"))
                .map(|timestamp| timestamp.with_timezone(&Utc).timestamp_micros())
        })
        .or_else(|_| {
            NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S"))
                .map(|naive| naive.and_utc().timestamp_micros())
        })
        .map_err(|error| {
            invalid_json(
                expected,
                format!("unsupported temporal literal `{text}`: {error}"),
            )
        })?;
    Ok(unix_micros - PG_EPOCH_MICROS_FROM_UNIX)
}

fn try_parse_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|n| i64::try_from(n).ok()))
        .or_else(|| value.as_str().and_then(|text| text.parse::<i64>().ok()))
}

fn parse_i64(value: &Value, expected: &'static str) -> Result<i64, SortKeyError> {
    try_parse_i64(value)
        .ok_or_else(|| invalid_json(expected, format!("expected integer, got {value}")))
}

fn decode_one<T: storekey::Decode>(bytes: &[u8]) -> Result<T, SortKeyError> {
    storekey::decode(Cursor::new(bytes)).map_err(|error| SortKeyError::Decode(error.to_string()))
}

fn invalid_json(expected: &'static str, detail: impl Into<String>) -> SortKeyError {
    SortKeyError::InvalidJson {
        expected,
        detail: detail.into(),
    }
}
