//! Shared PostgreSQL `List` and `String` helpers for CustomPath/CustomScan private data.
//!
//! Owns null-safe list indexing and `pstrdup`+`makeString` so path and scan private
//! serializers do not each invent dangling-`CString` patterns.

#![allow(unsafe_op_in_unsafe_fn)]

use std::ffi::CString;
use std::os::raw::c_void;

use pgrx::pg_sys;

/// Returns list length, or `0` when `list` is null.
#[must_use]
pub(super) unsafe fn list_len(list: *mut pg_sys::List) -> i32 {
    if list.is_null() {
        0
    } else {
        (*list).length
    }
}

/// Returns the nth pointer element, or null when out of range / missing elements.
#[must_use]
pub(super) unsafe fn list_nth_ptr(list: *mut pg_sys::List, index: i32) -> *mut c_void {
    if list.is_null() || index < 0 || index >= (*list).length || (*list).elements.is_null() {
        return std::ptr::null_mut();
    }
    (*(*list).elements.add(index as usize)).ptr_value
}

/// Reads a `T_Integer` at `index`; missing/invalid → `None`.
#[must_use]
pub(super) unsafe fn list_integer_at(list: *mut pg_sys::List, index: i32) -> Option<i32> {
    if list_len(list) <= index {
        return None;
    }
    let marker = list_nth_ptr(list, index).cast::<pg_sys::Integer>();
    if marker.is_null() || (*marker).type_ != pg_sys::NodeTag::T_Integer {
        return None;
    }
    Some((*marker).ival)
}

/// Reads a `T_String` at `index`; missing/invalid/null sval → `None`.
#[must_use]
pub(super) unsafe fn list_cstring_at(list: *mut pg_sys::List, index: i32) -> Option<String> {
    if list_len(list) <= index {
        return None;
    }
    let string_node = list_nth_ptr(list, index).cast::<pg_sys::String>();
    if string_node.is_null()
        || (*string_node).type_ != pg_sys::NodeTag::T_String
        || (*string_node).sval.is_null()
    {
        return None;
    }
    Some(
        std::ffi::CStr::from_ptr((*string_node).sval)
            .to_string_lossy()
            .into_owned(),
    )
}

/// Builds a PostgreSQL `String` node whose payload lives in `CurrentMemoryContext`.
///
/// `makeString` stores the pointer as-is; always `pstrdup` so Rust `CString`
/// drop cannot leave a dangling `sval`.
#[must_use]
pub(super) unsafe fn make_pg_string(value: &str) -> *mut pg_sys::String {
    let c_value = match CString::new(value) {
        Ok(value) => value,
        Err(_) => CString::new("").expect("empty string has no interior NUL"),
    };
    pg_sys::makeString(pg_sys::pstrdup(c_value.as_ptr()))
}

/// True when a private descending flag Integer is non-zero.
///
/// Missing private data means ASC (fail-closed after ordered LIMIT regressions).
#[must_use]
pub(super) fn order_descending_flag(value: Option<i32>) -> bool {
    value.is_some_and(|v| v != 0)
}
