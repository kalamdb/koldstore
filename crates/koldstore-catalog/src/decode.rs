//! Pure decoders for catalog JSON payloads.

/// PostgreSQL relation identity resolved from `pg_class`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationContext {
    /// Schema name.
    pub namespace: String,
    /// Relation name.
    pub name: String,
}

/// Storage context required to publish a flush segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushStorageContext {
    /// Object-store base path.
    pub base_path: String,
    /// Active KoldStore schema version.
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
    Ok(RelationContext {
        namespace: required_string(value, "namespace")?.to_string(),
        name: required_string(value, "name")?.to_string(),
    })
}

/// Decodes a flush storage context JSON payload.
///
/// # Errors
///
/// Returns an error when required fields are missing or have the wrong type.
pub fn flush_storage_context(value: &serde_json::Value) -> Result<FlushStorageContext, String> {
    let schema_version = required_i32(value, "schema_version")?;
    Ok(FlushStorageContext {
        base_path: required_string(value, "base_path")?.to_string(),
        schema_version,
        compression: required_string(value, "compression")?.to_string(),
    })
}

fn required_string<'a>(value: &'a serde_json::Value, field: &str) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("missing string field `{field}`"))
}

fn required_i32(value: &serde_json::Value, field: &str) -> Result<i32, String> {
    let raw = value
        .get(field)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| format!("missing integer field `{field}`"))?;
    i32::try_from(raw).map_err(|error| error.to_string())
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
