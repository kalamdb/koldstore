//! Background worker registration boundary.

/// Returns whether built-in worker registration requires preload.
#[must_use]
pub const fn requires_shared_preload() -> bool {
    true
}
