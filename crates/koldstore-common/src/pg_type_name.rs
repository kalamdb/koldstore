//! PostgreSQL catalog type-name normalization.
//!
//! This module only normalizes raw catalog strings. For typed parsing use
//! [`koldstore_schema::PgType::from_postgres_name`].

/// Normalizes PostgreSQL catalog type text to a canonical MVP spelling.
#[must_use]
pub fn canonical_postgres_type_name(type_name: &str) -> String {
    let mut normalized = type_name
        .trim()
        .to_ascii_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if normalized == "timestamp with time zone" {
        return "timestamptz".to_string();
    }
    if normalized.starts_with("timestamp(") && normalized.ends_with(" with time zone") {
        return "timestamptz".to_string();
    }
    if let Some((prefix, suffix)) = normalized.split_once('(') {
        if suffix.ends_with(')') {
            normalized = prefix.trim().to_string();
        }
    }
    match normalized.as_str() {
        "boolean" => "bool".to_string(),
        "smallint" => "int2".to_string(),
        "integer" | "int" => "int4".to_string(),
        "bigint" => "int8".to_string(),
        "real" => "float4".to_string(),
        "double precision" => "float8".to_string(),
        "character varying" => "varchar".to_string(),
        "timestamp with time zone" => "timestamptz".to_string(),
        _ => normalized,
    }
}
