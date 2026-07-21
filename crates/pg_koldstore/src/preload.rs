//! Shared-preload gate for merge-scan correctness.
//!
//! KoldMergeScan planner hooks are installed in `_PG_init`. Without
//! `shared_preload_libraries = 'koldstore'`, those hooks exist only in backends
//! that happened to load the `.so`, so fresh sessions silently read heap-only
//! rows after flush. This module fails closed at load and at `manage_table`.

use std::sync::atomic::{AtomicBool, Ordering};

/// Set when `_PG_init` runs under `shared_preload_libraries`.
static LOADED_VIA_SHARED_PRELOAD: AtomicBool = AtomicBool::new(false);

const PRELOAD_REQUIRED_MSG: &str = "\
koldstore must be loaded via shared_preload_libraries. \
Add koldstore to shared_preload_libraries and restart PostgreSQL before \
CREATE EXTENSION or manage_table. session_preload_libraries is not sufficient \
(merge-scan hooks and background workers require shared preload).";

/// Records that this process loaded koldstore during shared preload.
pub fn mark_loaded_via_shared_preload() {
    LOADED_VIA_SHARED_PRELOAD.store(true, Ordering::Relaxed);
}

/// Returns whether `_PG_init` ran under shared preload in this process.
#[must_use]
pub fn loaded_via_shared_preload() -> bool {
    LOADED_VIA_SHARED_PRELOAD.load(Ordering::Relaxed)
}

/// Errors unless koldstore was shared-preloaded into this process.
///
/// Call from `manage_table` (and similar tiered-data entrypoints) so operators
/// get a loud failure instead of incomplete SELECT results later.
pub fn require_shared_preload() {
    if loaded_via_shared_preload() {
        return;
    }
    // Defense in depth: GUC list can still list koldstore after a mis-ordered
    // LOAD in odd test setups, but only the shared-preload flag is authoritative.
    pgrx::error!("{PRELOAD_REQUIRED_MSG}");
}

/// Returns the operator-facing preload requirement message.
#[must_use]
pub fn preload_required_message() -> &'static str {
    PRELOAD_REQUIRED_MSG
}

/// Builds `koldstore.preload_status()` JSON for operators and monitoring.
#[must_use]
pub fn preload_status_json() -> serde_json::Value {
    let shared_preload_lists_koldstore = shared_preload_lists_koldstore();
    serde_json::json!({
        "library": "koldstore",
        "shared_preload": shared_preload_lists_koldstore,
        "loaded_via_shared_preload": loaded_via_shared_preload(),
        "enable_merge_scan": crate::guc::enable_merge_scan(),
    })
}

fn shared_preload_lists_koldstore() -> bool {
    // SHOW shared_preload_libraries via GUC when available.
    let setting = unsafe {
        let name = c"shared_preload_libraries";
        let value = pgrx::pg_sys::GetConfigOption(name.as_ptr(), true, false);
        if value.is_null() {
            return false;
        }
        std::ffi::CStr::from_ptr(value)
            .to_string_lossy()
            .to_string()
    };
    setting
        .split(',')
        .map(str::trim)
        .any(|entry| entry == "koldstore")
}

#[cfg(feature = "pg")]
#[pgrx::pg_extern(
    name = "preload_status",
    schema = "koldstore",
    immutable,
    parallel_safe
)]
fn preload_status_pg() -> pgrx::JsonB {
    pgrx::JsonB(preload_status_json())
}
