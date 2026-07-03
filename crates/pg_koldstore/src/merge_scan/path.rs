//! CustomPath construction glue.

/// Custom scan provider name.
pub const CUSTOM_PATH_NAME: &str = "KoldstoreMergeScan";

/// Returns whether heap-only final paths must be replaced for a managed relation.
#[must_use]
pub const fn replace_heap_final_path(is_managed: bool) -> bool {
    is_managed
}
