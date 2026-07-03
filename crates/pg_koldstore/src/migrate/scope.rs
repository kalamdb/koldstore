//! User-scope migration helpers.

/// System-added scope column name.
pub const SYSTEM_SCOPE_COLUMN: &str = "_user_id";

/// Resolves the effective scope column for a user-scoped table.
#[must_use]
pub fn effective_scope_column(table_type: &str, app_scope_column: Option<&str>) -> Option<String> {
    if table_type == "user" {
        Some(app_scope_column.unwrap_or(SYSTEM_SCOPE_COLUMN).to_string())
    } else {
        None
    }
}
