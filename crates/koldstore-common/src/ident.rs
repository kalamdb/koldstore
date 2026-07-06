//! PostgreSQL identifier validation and quoting.

/// Returns true when `value` is a safe unquoted SQL identifier.
#[must_use]
pub fn is_safe_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

/// Quotes a validated SQL identifier.
///
/// # Panics
///
/// Panics when `value` is not a safe identifier.
#[must_use]
pub fn quote_ident(value: &str) -> String {
    assert!(
        is_safe_identifier(value),
        "quote_ident requires a safe identifier: {value}"
    );
    format!("\"{value}\"")
}
