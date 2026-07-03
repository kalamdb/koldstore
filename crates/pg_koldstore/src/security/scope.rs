//! User-scope enforcement.

/// Normalizes a user scope string.
#[must_use]
pub fn normalize_scope(value: &str) -> String {
    value.trim().to_string()
}

/// Requires a user scope and returns the normalized value.
///
/// # Errors
///
/// Returns an error when the scope is missing or empty.
pub fn require_user_scope(value: Option<&str>) -> Result<String, String> {
    let Some(value) = value.map(normalize_scope).filter(|value| !value.is_empty()) else {
        return Err("koldstore.user_id is not set".to_string());
    };
    Ok(value)
}

/// Returns whether a row scope matches the active session scope.
#[must_use]
pub fn scope_matches(active_scope: &str, row_scope: &str) -> bool {
    normalize_scope(active_scope) == normalize_scope(row_scope)
}
