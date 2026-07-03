//! Native Custom Scan shim bindings.

use std::ffi::CStr;
use std::os::raw::c_char;

unsafe extern "C" {
    fn koldstore_register_custom_scan();
    fn koldstore_custom_scan_callback_count() -> usize;
    fn koldstore_custom_scan_callback_name(index: usize) -> *const c_char;
}

/// Registers the native Custom Scan shim.
pub fn register_native_custom_scan() {
    unsafe { koldstore_register_custom_scan() };
}

/// Returns the number of native Custom Scan callback slots.
#[must_use]
pub fn native_callback_count() -> usize {
    unsafe { koldstore_custom_scan_callback_count() }
}

/// Returns one native Custom Scan callback name.
#[must_use]
pub fn native_callback_name(index: usize) -> Option<&'static str> {
    let ptr = unsafe { koldstore_custom_scan_callback_name(index) };
    if ptr.is_null() {
        return None;
    }

    unsafe { CStr::from_ptr(ptr) }.to_str().ok()
}

/// Returns the native Custom Scan callback names in registration order.
#[must_use]
pub fn native_callback_names() -> Vec<&'static str> {
    (0..native_callback_count())
        .filter_map(native_callback_name)
        .collect()
}
