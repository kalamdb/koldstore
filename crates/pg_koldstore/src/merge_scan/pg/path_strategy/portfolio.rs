//! Installs KoldMergeScan `CustomPath` nodes onto a `RelOptInfo`.
//!
//! Task 1.3 behavior: still emits a single [`KoldPathStrategy::GeneralMerge`]
//! wrapper around the cheapest hot child. Multi-path ordered portfolio lands
//! in Task 1.4. `scope_key` is plumbed as `""` for forward compatibility.

#![allow(unsafe_op_in_unsafe_fn)]

use std::ffi::CString;
use std::os::raw::{c_char, c_void};

use koldstore_merge::scan::KoldPathStrategy;
use pgrx::pg_sys;

use super::cost::{catalog_startup_bias, general_merge_total_cost};

/// Private-list tag for [`KoldPathStrategy::ExactPrimaryKey`].
pub(crate) const STRATEGY_TAG_EXACT_PRIMARY_KEY: i32 = 1;
/// Private-list tag for [`KoldPathStrategy::ProvenHotOnly`].
pub(crate) const STRATEGY_TAG_PROVEN_HOT_ONLY: i32 = 2;
/// Private-list tag for [`KoldPathStrategy::UnorderedHotFirst`].
pub(crate) const STRATEGY_TAG_UNORDERED_HOT_FIRST: i32 = 3;
/// Private-list tag for [`KoldPathStrategy::OrderedProgressive`].
pub(crate) const STRATEGY_TAG_ORDERED_PROGRESSIVE: i32 = 4;
/// Private-list tag for [`KoldPathStrategy::GeneralMerge`].
pub(crate) const STRATEGY_TAG_GENERAL_MERGE: i32 = 5;

const PATH_PRIVATE_STRATEGY_INDEX: i32 = 0;
const PATH_PRIVATE_SCOPE_KEY_INDEX: i32 = 1;

/// Maps a strategy discriminant to the integer stored in `custom_private`.
///
/// Ordered specs are not fully serialized yet; Task 1.4 extends private data.
#[must_use]
pub(crate) fn path_strategy_tag(strategy: &KoldPathStrategy) -> i32 {
    match strategy {
        KoldPathStrategy::ExactPrimaryKey => STRATEGY_TAG_EXACT_PRIMARY_KEY,
        KoldPathStrategy::ProvenHotOnly => STRATEGY_TAG_PROVEN_HOT_ONLY,
        KoldPathStrategy::UnorderedHotFirst => STRATEGY_TAG_UNORDERED_HOT_FIRST,
        KoldPathStrategy::OrderedProgressive(_) => STRATEGY_TAG_ORDERED_PROGRESSIVE,
        KoldPathStrategy::GeneralMerge => STRATEGY_TAG_GENERAL_MERGE,
    }
}

/// Reconstructs a coarse strategy from path private data (ordered details TBD).
#[must_use]
pub(crate) fn path_strategy_tag_from_private(private: *mut pg_sys::List) -> i32 {
    unsafe {
        if list_len(private) <= PATH_PRIVATE_STRATEGY_INDEX {
            return STRATEGY_TAG_GENERAL_MERGE;
        }
        let marker =
            list_nth_ptr(private, PATH_PRIVATE_STRATEGY_INDEX).cast::<pg_sys::Integer>();
        if marker.is_null() || (*marker).type_ != pg_sys::NodeTag::T_Integer {
            return STRATEGY_TAG_GENERAL_MERGE;
        }
        (*marker).ival
    }
}

/// Encodes strategy identity and single-scope key for a CustomPath.
///
/// `scope_key` defaults to `""` today; one query binds one scope later.
pub(crate) unsafe fn serialize_path_strategy_private(
    strategy: &KoldPathStrategy,
    scope_key: &str,
) -> *mut pg_sys::List {
    let tag = pg_sys::makeInteger(path_strategy_tag(strategy));
    let scope = match CString::new(scope_key) {
        Ok(value) => value,
        Err(_) => CString::new("").expect("empty scope is valid"),
    };
    // makeString copies into PostgreSQL memory.
    let scope_node = pg_sys::makeString(scope.as_ptr() as *mut c_char);
    let private = pg_sys::lappend(std::ptr::null_mut(), tag.cast::<c_void>());
    pg_sys::lappend(private, scope_node.cast::<c_void>())
}

/// Reads the scope key from path private data; missing/invalid → `""`.
#[must_use]
pub(crate) unsafe fn scope_key_from_path_private(private: *mut pg_sys::List) -> String {
    if list_len(private) <= PATH_PRIVATE_SCOPE_KEY_INDEX {
        return String::new();
    }
    let string_node =
        list_nth_ptr(private, PATH_PRIVATE_SCOPE_KEY_INDEX).cast::<pg_sys::String>();
    if string_node.is_null()
        || (*string_node).type_ != pg_sys::NodeTag::T_String
        || (*string_node).sval.is_null()
    {
        return String::new();
    }
    std::ffi::CStr::from_ptr((*string_node).sval)
        .to_string_lossy()
        .into_owned()
}

/// Installs a single general-merge KoldMergeScan path and clears heap finals.
///
/// # Safety
/// `rel` and `hot_child` must be live planner pointers; `methods` must outlive
/// the path (static `PATH_METHODS`).
pub(crate) unsafe fn install_general_merge_path(
    rel: *mut pg_sys::RelOptInfo,
    hot_child: *mut pg_sys::Path,
    segment_count: usize,
    methods: *const pg_sys::CustomPathMethods,
) {
    if rel.is_null() || hot_child.is_null() || methods.is_null() {
        return;
    }

    let startup_cost = (*hot_child).startup_cost + catalog_startup_bias();
    let total_cost = general_merge_total_cost((*hot_child).total_cost, segment_count);

    let custom_path =
        pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;
    if custom_path.is_null() {
        return;
    }

    (*custom_path).path.type_ = pg_sys::NodeTag::T_CustomPath;
    (*custom_path).path.pathtype = pg_sys::NodeTag::T_CustomScan;
    (*custom_path).path.parent = rel;
    (*custom_path).path.pathtarget = (*rel).reltarget;
    (*custom_path).path.param_info = (*hot_child).param_info;
    (*custom_path).path.rows = (*hot_child).rows;
    (*custom_path).path.startup_cost = startup_cost;
    (*custom_path).path.total_cost = total_cost;
    (*custom_path).path.parallel_safe = false;
    (*custom_path).custom_paths = pg_sys::lappend(std::ptr::null_mut(), hot_child.cast::<c_void>());
    (*custom_path).custom_private =
        serialize_path_strategy_private(&KoldPathStrategy::GeneralMerge, "");
    (*custom_path).methods = methods;

    // Managed reads must expose only KoldMergeScan as a final path. Clear both
    // `pathlist` and `partial_pathlist`: PostgreSQL builds Gather / Gather Merge
    // *after* this hook from leftover partials.
    (*rel).pathlist = std::ptr::null_mut();
    (*rel).partial_pathlist = std::ptr::null_mut();
    (*rel).pathlist = pg_sys::lappend(std::ptr::null_mut(), (&raw mut (*custom_path).path).cast());
}

/// Returns the cheapest non-custom path from a relation pathlist.
pub(crate) unsafe fn find_cheapest_path(
    pathlist: *mut pg_sys::List,
) -> Option<*mut pg_sys::Path> {
    let len = list_len(pathlist);
    let mut best: *mut pg_sys::Path = std::ptr::null_mut();
    let mut best_cost = f64::INFINITY;
    for idx in 0..len {
        let path = list_nth_ptr(pathlist, idx) as *mut pg_sys::Path;
        if path.is_null() {
            continue;
        }
        if (*path).type_ == pg_sys::NodeTag::T_CustomPath {
            continue;
        }
        if (*path).total_cost < best_cost {
            best_cost = (*path).total_cost;
            best = path;
        }
    }
    if best.is_null() {
        None
    } else {
        Some(best)
    }
}

unsafe fn list_len(list: *mut pg_sys::List) -> i32 {
    if list.is_null() {
        0
    } else {
        (*list).length
    }
}

unsafe fn list_nth_ptr(list: *mut pg_sys::List, index: i32) -> *mut c_void {
    if list.is_null() || index < 0 || index >= (*list).length {
        return std::ptr::null_mut();
    }
    let elements = (*list).elements;
    (*elements.offset(index as isize)).ptr_value
}
