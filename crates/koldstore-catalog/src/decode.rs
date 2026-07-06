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
}

/// Decodes a relation context JSON payload.
///
/// # Errors
///
/// Returns an error when required fields are missing or have the wrong type.
pub fn relation_context(value: &serde_json::Value) -> Result<RelationContext, String> {
    Ok(RelationContext {
        namespace: json_string(value, "namespace")?,
        name: json_string(value, "name")?,
    })
}

/// Decodes a flush storage context JSON payload.
///
/// # Errors
///
/// Returns an error when required fields are missing or have the wrong type.
pub fn flush_storage_context(value: &serde_json::Value) -> Result<FlushStorageContext, String> {
    Ok(FlushStorageContext {
        base_path: json_string(value, "base_path")?,
        schema_version: json_i64(value, "schema_version")? as i32,
    })
}

/// Decodes a required string field from a JSON object.
///
/// # Errors
///
/// Returns an error when the field is missing or not a string.
pub fn json_string(value: &serde_json::Value, key: &str) -> Result<String, String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| format!("missing string field `{key}`"))
}

/// Decodes a required integer field from a JSON object.
///
/// # Errors
///
/// Returns an error when the field is missing or not an integer.
pub fn json_i64(value: &serde_json::Value, key: &str) -> Result<i64, String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| format!("missing integer field `{key}`"))
}
