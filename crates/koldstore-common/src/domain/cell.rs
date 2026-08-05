//! Typed cell and row-image values for hot/cold merge payloads.
//!
//! Prefer these over `serde_json::Value` on scan, flush, and resolve paths so
//! backends avoid per-row JSON allocate/parse. JSON conversion stays at SQL /
//! admin boundaries (`JsonB` operators, change-feed contracts).

use std::collections::HashMap;
use std::ops::Index;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Number, Value};

use crate::{KoldstoreError, Result};

/// One typed column value in a hot or cold row image.
///
/// Matches the flush/Parquet cell vocabulary so decode, merge, and encode share
/// one representation without JSON round-trips.
#[derive(Debug, Clone, PartialEq)]
pub enum CellValue {
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
    /// `timestamptz` stored as PostgreSQL-epoch UTC microseconds.
    TimestamptzMicros(i64),
}

impl CellValue {
    /// Returns true for SQL NULL.
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Returns a boolean when this cell is [`Self::Bool`].
    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    /// Returns an integer when this cell is an integer variant (widened to `i64`).
    #[must_use]
    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int16(value) => Some(*value as i64),
            Self::Int32(value) => Some(*value as i64),
            Self::Int64(value) | Self::TimestamptzMicros(value) => Some(*value),
            _ => None,
        }
    }

    /// Returns a float when this cell is a floating-point variant (widened to `f64`).
    #[must_use]
    pub const fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float32(value) => Some(*value as f64),
            Self::Float64(value) => Some(*value),
            _ => None,
        }
    }

    /// Returns text when this cell is [`Self::Utf8`].
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Utf8(value) => Some(value.as_str()),
            _ => None,
        }
    }

    /// Lossless JSON view for SQL/admin boundaries.
    #[must_use]
    pub fn to_json(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(value) => Value::Bool(*value),
            Self::Int16(value) => Value::Number((*value).into()),
            Self::Int32(value) => Value::Number((*value).into()),
            Self::Int64(value) | Self::TimestamptzMicros(value) => Value::Number((*value).into()),
            Self::Float32(value) => Number::from_f64(f64::from(*value))
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Self::Float64(value) => Number::from_f64(*value)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Self::Utf8(value) => Value::String(value.clone()),
        }
    }

    /// Builds a cell from JSON produced at a SQL/admin boundary.
    ///
    /// Numbers prefer the narrowest integer that fits; otherwise `float64`.
    /// Objects and arrays are compacted into [`Self::Utf8`] so jsonb/array
    /// payloads remain round-trippable as text for Parquet Utf8 storage.
    #[must_use]
    pub fn from_json(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(flag) => Self::Bool(*flag),
            Value::Number(number) => {
                if let Some(n) = number.as_i64() {
                    if let Ok(n) = i16::try_from(n) {
                        Self::Int16(n)
                    } else if let Ok(n) = i32::try_from(n) {
                        Self::Int32(n)
                    } else {
                        Self::Int64(n)
                    }
                } else if let Some(n) = number.as_u64() {
                    if let Ok(n) = i16::try_from(n) {
                        Self::Int16(n)
                    } else if let Ok(n) = i32::try_from(n) {
                        Self::Int32(n)
                    } else if let Ok(n) = i64::try_from(n) {
                        Self::Int64(n)
                    } else {
                        Self::Utf8(n.to_string())
                    }
                } else if let Some(n) = number.as_f64() {
                    Self::Float64(n)
                } else {
                    Self::Utf8(number.to_string())
                }
            }
            Value::String(text) => Self::Utf8(text.clone()),
            Value::Array(_) | Value::Object(_) => Self::Utf8(value.to_string()),
        }
    }

    /// Compact display used by residual filter matching (SPI text equality).
    #[must_use]
    pub fn display_text(&self) -> String {
        match self {
            Self::Null => "null".to_string(),
            Self::Bool(value) => value.to_string(),
            Self::Int16(value) => value.to_string(),
            Self::Int32(value) => value.to_string(),
            Self::Int64(value) | Self::TimestamptzMicros(value) => value.to_string(),
            Self::Float32(value) => value.to_string(),
            Self::Float64(value) => value.to_string(),
            Self::Utf8(value) => value.clone(),
        }
    }
}

/// Application column payload for a hot or cold row.
///
/// Cells are keyed by catalog column name. Prefer building from typed decode
/// paths; use [`Self::from_json`] / [`Self::to_json`] only at JSON boundaries.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RowImage {
    cells: HashMap<String, CellValue>,
}

impl RowImage {
    /// Empty image (no application columns).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds an image with pre-sized capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            cells: HashMap::with_capacity(capacity),
        }
    }

    /// Returns true when no cells are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Number of stored cells.
    #[must_use]
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Borrows a cell by catalog name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&CellValue> {
        self.cells.get(name)
    }

    /// Inserts or replaces a cell.
    pub fn insert(&mut self, name: impl Into<String>, value: CellValue) -> Option<CellValue> {
        self.cells.insert(name.into(), value)
    }

    /// Removes a cell by name.
    pub fn remove(&mut self, name: &str) -> Option<CellValue> {
        self.cells.remove(name)
    }

    /// Returns true when `name` is present.
    #[must_use]
    pub fn contains_key(&self, name: &str) -> bool {
        self.cells.contains_key(name)
    }

    /// Iterates name/value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &CellValue)> {
        self.cells.iter()
    }

    /// Mutable map access for rename remapping on cold decode.
    #[must_use]
    pub fn cells_mut(&mut self) -> &mut HashMap<String, CellValue> {
        &mut self.cells
    }

    /// Builds from a JSON object (or null → empty).
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is neither null nor an object.
    pub fn from_json(value: &Value) -> Result<Self> {
        match value {
            Value::Null => Ok(Self::new()),
            Value::Object(map) => {
                let mut cells = HashMap::with_capacity(map.len());
                for (name, cell) in map {
                    cells.insert(name.clone(), CellValue::from_json(cell));
                }
                Ok(Self { cells })
            }
            other => Err(KoldstoreError::InvalidIdentifier {
                kind: "row_image",
                value: other.to_string(),
            }),
        }
    }

    /// Convenience for tests and JSON fixtures.
    #[must_use]
    pub fn from_json_value(value: Value) -> Self {
        Self::from_json(&value).unwrap_or_default()
    }

    /// JSON object view for SQL/admin boundaries.
    #[must_use]
    pub fn to_json(&self) -> Value {
        let mut map = Map::with_capacity(self.cells.len());
        for (name, value) in &self.cells {
            map.insert(name.clone(), value.to_json());
        }
        Value::Object(map)
    }
}

impl Serialize for RowImage {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_json().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RowImage {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        Self::from_json(&value).map_err(serde::de::Error::custom)
    }
}

impl Index<&str> for RowImage {
    type Output = CellValue;

    fn index(&self, name: &str) -> &Self::Output {
        &self.cells[name]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trips_object_images() {
        let image = RowImage::from_json_value(json!({"id": 7, "body": "hot"}));
        assert_eq!(image["id"].as_i64(), Some(7));
        assert_eq!(image["body"].as_str(), Some("hot"));
        assert_eq!(image.to_json(), json!({"id": 7, "body": "hot"}));
    }

    #[test]
    fn null_json_becomes_empty_image() {
        assert!(RowImage::from_json(&Value::Null).unwrap().is_empty());
    }
}
