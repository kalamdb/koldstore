//! Pure decoders for catalog JSON payloads.

use serde::Deserialize;

/// PostgreSQL relation identity resolved from `pg_class`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RelationContext {
    /// Schema name.
    pub namespace: String,
    /// Relation name.
    pub name: String,
}

/// Storage context required to publish a flush segment.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct FlushStorageContext {
    /// Object-store base path.
    pub base_path: String,
    /// Active KoldStore schema version.
    #[serde(deserialize_with = "deserialize_schema_version")]
    pub schema_version: i32,
    /// Configured Parquet compression codec.
    pub compression: String,
}

/// Decodes a relation context JSON payload.
///
/// # Errors
///
/// Returns an error when required fields are missing or have the wrong type.
pub fn relation_context(value: &serde_json::Value) -> Result<RelationContext, String> {
    serde_json::from_value(value.clone()).map_err(|error| error.to_string())
}

/// Decodes a flush storage context JSON payload.
///
/// # Errors
///
/// Returns an error when required fields are missing or have the wrong type.
pub fn flush_storage_context(value: &serde_json::Value) -> Result<FlushStorageContext, String> {
    serde_json::from_value(value.clone()).map_err(|error| error.to_string())
}

fn deserialize_schema_version<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = i64::deserialize(deserializer)?;
    i32::try_from(value).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::flush_storage_context;

    #[test]
    fn flush_storage_context_requires_compression_codec() {
        let value = serde_json::json!({
            "base_path": "/tmp/koldstore",
            "schema_version": 3,
            "compression": "zstd"
        });

        let context = flush_storage_context(&value).unwrap();
        assert_eq!(context.base_path, "/tmp/koldstore");
        assert_eq!(context.schema_version, 3);
        assert_eq!(context.compression, "zstd");
    }
}
