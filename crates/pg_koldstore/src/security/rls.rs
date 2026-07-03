//! RLS classification and fail-closed boundaries.

/// Returns the fail-closed error message for unsupported cold RLS.
#[must_use]
pub const fn unsupported_rls_error() -> &'static str {
    "koldstore cannot enforce this RLS policy on cold rows"
}

/// Enforces an RLS/security qual or fails closed.
///
/// # Errors
///
/// Returns an error when the qual cannot be enforced on cold rows.
pub fn enforce_or_fail_closed(can_enforce: bool) -> Result<(), &'static str> {
    if can_enforce {
        Ok(())
    } else {
        Err(unsupported_rls_error())
    }
}
