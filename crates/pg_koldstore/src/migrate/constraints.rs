//! Migration validation.

/// Returns true when the table has a primary-key shape pg-koldstore can manage.
#[must_use]
pub fn primary_key_shape_supported(columns: &[&str]) -> bool {
    !columns.is_empty() && columns.iter().all(|column| !column.trim().is_empty())
}

/// Migration validation outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationValidation {
    /// Supported primary-key columns.
    pub primary_key: Vec<String>,
    /// Whether FK semantics are accepted as hot-only.
    pub allow_fk_hot_only: bool,
    /// Indexed columns considered for cold stats.
    pub indexed_columns: Vec<String>,
}

/// Returns whether FK configuration can be migrated.
#[must_use]
pub const fn fk_policy_allowed(has_fk: bool, flush_enabled: bool, allow_fk_hot_only: bool) -> bool {
    !has_fk || !flush_enabled || allow_fk_hot_only
}

/// Returns whether a column type is supported by the MVP type matrix.
#[must_use]
pub fn type_supported(type_name: &str) -> bool {
    matches!(
        type_name,
        "boolean"
            | "smallint"
            | "integer"
            | "bigint"
            | "real"
            | "double precision"
            | "text"
            | "uuid"
            | "jsonb"
            | "timestamp with time zone"
    )
}
