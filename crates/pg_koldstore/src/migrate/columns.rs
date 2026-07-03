//! System column definitions.

/// Required system columns.
pub const REQUIRED_SYSTEM_COLUMNS: &[&str] = &["_seq", "_commit_seq", "_deleted"];

/// Returns all system columns for a migration.
#[must_use]
pub fn system_columns(user_scoped_without_app_column: bool) -> Vec<&'static str> {
    let mut columns = REQUIRED_SYSTEM_COLUMNS.to_vec();
    if user_scoped_without_app_column {
        columns.push("_user_id");
    }
    columns
}
