//! Installs KoldMergeScan `CustomPath` nodes onto a `RelOptInfo`.
//!
//! Owns strategy selection and `add_path` portfolio installation. Locked
//! plan-time early returns (empty manifest, `cold_side_proven_empty`) stay in
//! `set_rel_pathlist` before this module runs.
//!
//! `scope_key` defaults to [`koldstore_common::DEFAULT_SCOPE_KEY`] (`""`) for
//! single-scope shared tables. Ordered paths copy the hot child's `pathkeys`
//! so PostgreSQL can avoid an external `Sort`; emit uses ordered progressive
//! merge in `execute`.

#![allow(unsafe_op_in_unsafe_fn)]

use std::os::raw::c_void;

use koldstore_merge::scan::{
    classify_path_strategy, KoldPathStrategy, OrderColumnSupport, OrderedPathSpec, StrategyRequest,
};
use pgrx::pg_sys;

use super::super::pg_list::{
    list_cstring_at, list_integer_at, list_len, list_nth_ptr, make_pg_string, order_descending_flag,
};
use super::cost::{catalog_startup_bias, general_merge_total_cost};

/// Private-list tag for [`KoldPathStrategy::ExactPrimaryKey`].
pub(crate) const STRATEGY_TAG_EXACT_PRIMARY_KEY: i32 = 1;
/// Private-list tag for [`KoldPathStrategy::UnorderedHotFirst`].
pub(crate) const STRATEGY_TAG_UNORDERED_HOT_FIRST: i32 = 3;
/// Private-list tag for [`KoldPathStrategy::OrderedProgressive`].
pub(crate) const STRATEGY_TAG_ORDERED_PROGRESSIVE: i32 = 4;
/// Private-list tag for [`KoldPathStrategy::GeneralMerge`].
pub(crate) const STRATEGY_TAG_GENERAL_MERGE: i32 = 5;

const PATH_PRIVATE_STRATEGY_INDEX: i32 = 0;
const PATH_PRIVATE_SCOPE_KEY_INDEX: i32 = 1;
const PATH_PRIVATE_SORT_ORDER_ID_INDEX: i32 = 2;
const PATH_PRIVATE_LEADING_COLUMN_ID_INDEX: i32 = 3;
const PATH_PRIVATE_ORDER_DESCENDING_INDEX: i32 = 4;

/// Inputs for building the cold-capable path portfolio.
#[derive(Debug, Clone)]
pub(crate) struct PortfolioInstallArgs {
    /// Scan RTE index (`varno`) for pathkey Var matching.
    pub scanrelid: pg_sys::Index,
    /// Primary-key attnums (column ids).
    pub primary_key_attnums: Vec<i16>,
    /// Configured immutable segment-order column, if any.
    pub segment_order_attnum: Option<i16>,
    /// True when baserestrict quals are exact equality on every PK column.
    pub exact_full_primary_key_equality: bool,
    /// Published cold segment count for costing.
    pub segment_count: usize,
    /// Single-scope key; default `""`.
    pub scope_key: String,
}

/// Maps a strategy discriminant to the integer stored in `custom_private`.
#[must_use]
pub(crate) fn path_strategy_tag(strategy: &KoldPathStrategy) -> i32 {
    match strategy {
        KoldPathStrategy::ExactPrimaryKey => STRATEGY_TAG_EXACT_PRIMARY_KEY,
        KoldPathStrategy::UnorderedHotFirst => STRATEGY_TAG_UNORDERED_HOT_FIRST,
        KoldPathStrategy::OrderedProgressive(_) => STRATEGY_TAG_ORDERED_PROGRESSIVE,
        KoldPathStrategy::GeneralMerge => STRATEGY_TAG_GENERAL_MERGE,
    }
}

/// Human-readable strategy label for EXPLAIN.
#[must_use]
pub(crate) fn strategy_explain_label(tag: i32) -> &'static str {
    match tag {
        STRATEGY_TAG_EXACT_PRIMARY_KEY => "Exact Primary Key",
        STRATEGY_TAG_UNORDERED_HOT_FIRST => "Unordered Hot First",
        STRATEGY_TAG_ORDERED_PROGRESSIVE => "Ordered Progressive",
        _ => "General Merge",
    }
}

/// Reconstructs a coarse strategy tag from path private data.
#[must_use]
pub(crate) fn path_strategy_tag_from_private(private: *mut pg_sys::List) -> i32 {
    unsafe {
        list_integer_at(private, PATH_PRIVATE_STRATEGY_INDEX).unwrap_or(STRATEGY_TAG_GENERAL_MERGE)
    }
}

/// Encodes strategy identity, scope, and ordered-spec fields for a CustomPath.
pub(crate) unsafe fn serialize_path_strategy_private(
    strategy: &KoldPathStrategy,
    scope_key: &str,
    order_descending: bool,
) -> *mut pg_sys::List {
    let (sort_order_id, leading_column_id) = match strategy {
        KoldPathStrategy::OrderedProgressive(spec) => {
            (spec.sort_order_id, i32::from(spec.leading_column_id))
        }
        _ => (0, 0),
    };
    let tag = pg_sys::makeInteger(path_strategy_tag(strategy));
    let scope_node = make_pg_string(scope_key);
    let sort_order = pg_sys::makeInteger(sort_order_id);
    let leading = pg_sys::makeInteger(leading_column_id);
    let descending = pg_sys::makeInteger(i32::from(order_descending));
    let mut private = pg_sys::lappend(std::ptr::null_mut(), tag.cast::<c_void>());
    private = pg_sys::lappend(private, scope_node.cast::<c_void>());
    private = pg_sys::lappend(private, sort_order.cast::<c_void>());
    private = pg_sys::lappend(private, leading.cast::<c_void>());
    pg_sys::lappend(private, descending.cast::<c_void>())
}

/// Reads the scope key from path private data; missing/invalid → `""`.
#[must_use]
pub(crate) unsafe fn scope_key_from_path_private(private: *mut pg_sys::List) -> String {
    list_cstring_at(private, PATH_PRIVATE_SCOPE_KEY_INDEX).unwrap_or_default()
}

/// Reads ordered leading column id from path private data (0 if absent).
#[must_use]
pub(crate) unsafe fn leading_column_id_from_path_private(private: *mut pg_sys::List) -> i16 {
    list_integer_at(private, PATH_PRIVATE_LEADING_COLUMN_ID_INDEX).unwrap_or(0) as i16
}

/// Reads sort_order_id from path private data (0 if absent).
#[must_use]
pub(crate) unsafe fn sort_order_id_from_path_private(private: *mut pg_sys::List) -> i32 {
    list_integer_at(private, PATH_PRIVATE_SORT_ORDER_ID_INDEX).unwrap_or(0)
}

/// Installs the KoldMergeScan path portfolio and clears bare heap finals.
///
/// Always offers a non-ordering fallback around the cheapest hot child, plus
/// an ordered progressive wrapper for each native path whose leading pathkey
/// matches the primary key or configured segment-order column.
///
/// # Safety
/// `rel` must be a live planner relation; `methods` must outlive installed paths.
pub(crate) unsafe fn install_path_portfolio(
    rel: *mut pg_sys::RelOptInfo,
    args: &PortfolioInstallArgs,
    methods: *const pg_sys::CustomPathMethods,
) {
    if rel.is_null() || methods.is_null() {
        return;
    }

    let natives = collect_native_paths((*rel).pathlist);
    if natives.is_empty() {
        return;
    }

    let cheapest = natives
        .iter()
        .copied()
        .min_by(|left, right| unsafe { (**left).total_cost.total_cmp(&(**right).total_cost) })
        .expect("natives non-empty");

    // Drop bare heap finals before add_path so only KoldMergeScan remains.
    (*rel).pathlist = std::ptr::null_mut();
    (*rel).partial_pathlist = std::ptr::null_mut();

    let fallback_strategy = fallback_strategy_for_cheapest(args);
    add_custom_wrapper(CustomWrapperArgs {
        rel,
        hot_child: cheapest,
        strategy: &fallback_strategy,
        scope_key: &args.scope_key,
        segment_count: args.segment_count,
        copy_pathkeys: false,
        order_descending: false,
        methods,
    });

    for hot_child in natives {
        let Some(order_support) = leading_order_support(hot_child, args.scanrelid, args) else {
            continue;
        };
        let leading = match order_support {
            OrderColumnSupport::PrimaryKey => args.primary_key_attnums[0],
            OrderColumnSupport::SegmentOrder => {
                args.segment_order_attnum.expect("segment order matched")
            }
            OrderColumnSupport::MutableOrUnsupported => continue,
        };
        let strategy = KoldPathStrategy::OrderedProgressive(OrderedPathSpec::with_scope_key(
            i32::from(leading),
            leading,
            args.primary_key_attnums.clone(),
            args.scope_key.clone(),
        ));
        add_custom_wrapper(CustomWrapperArgs {
            rel,
            hot_child,
            strategy: &strategy,
            scope_key: &args.scope_key,
            segment_count: args.segment_count,
            copy_pathkeys: true,
            order_descending: path_leading_descending(hot_child),
            methods,
        });
    }
}

/// Bundled inputs for installing one KoldMergeScan `CustomPath`.
struct CustomWrapperArgs<'a> {
    rel: *mut pg_sys::RelOptInfo,
    hot_child: *mut pg_sys::Path,
    strategy: &'a KoldPathStrategy,
    scope_key: &'a str,
    segment_count: usize,
    copy_pathkeys: bool,
    order_descending: bool,
    methods: *const pg_sys::CustomPathMethods,
}

unsafe fn add_custom_wrapper(args: CustomWrapperArgs<'_>) {
    let CustomWrapperArgs {
        rel,
        hot_child,
        strategy,
        scope_key,
        segment_count,
        copy_pathkeys,
        order_descending,
        methods,
    } = args;
    let custom_path =
        pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;
    if custom_path.is_null() {
        return;
    }

    let startup_cost = (*hot_child).startup_cost + catalog_startup_bias();
    let total_cost = general_merge_total_cost((*hot_child).total_cost, segment_count);

    (*custom_path).path.type_ = pg_sys::NodeTag::T_CustomPath;
    (*custom_path).path.pathtype = pg_sys::NodeTag::T_CustomScan;
    (*custom_path).path.parent = rel;
    (*custom_path).path.pathtarget = (*rel).reltarget;
    (*custom_path).path.param_info = (*hot_child).param_info;
    (*custom_path).path.rows = (*hot_child).rows;
    (*custom_path).path.startup_cost = startup_cost;
    (*custom_path).path.total_cost = total_cost;
    (*custom_path).path.parallel_safe = false;
    if copy_pathkeys {
        (*custom_path).path.pathkeys = (*hot_child).pathkeys;
    } else {
        (*custom_path).path.pathkeys = std::ptr::null_mut();
    }
    (*custom_path).custom_paths = pg_sys::lappend(std::ptr::null_mut(), hot_child.cast::<c_void>());
    (*custom_path).custom_private =
        serialize_path_strategy_private(strategy, scope_key, order_descending);
    (*custom_path).methods = methods;

    pg_sys::add_path(rel, (&raw mut (*custom_path).path).cast());
}

fn fallback_strategy_for_cheapest(args: &PortfolioInstallArgs) -> KoldPathStrategy {
    let request = StrategyRequest::new(
        args.exact_full_primary_key_equality,
        None,
        0,
        0,
        args.primary_key_attnums.clone(),
    );
    match classify_path_strategy(&request) {
        KoldPathStrategy::OrderedProgressive(_) => KoldPathStrategy::GeneralMerge,
        other => other,
    }
}

unsafe fn leading_order_support(
    path: *mut pg_sys::Path,
    scanrelid: pg_sys::Index,
    args: &PortfolioInstallArgs,
) -> Option<OrderColumnSupport> {
    if path.is_null() || (*path).pathkeys.is_null() {
        return None;
    }
    if !args.primary_key_attnums.is_empty()
        && path_leads_with_attnum(path, scanrelid, args.primary_key_attnums[0])
    {
        return Some(OrderColumnSupport::PrimaryKey);
    }
    if let Some(attnum) = args.segment_order_attnum {
        if path_leads_with_attnum(path, scanrelid, attnum) {
            return Some(OrderColumnSupport::SegmentOrder);
        }
    }
    None
}

/// True when the path's leading pathkey equivalence class contains `relid.attnum`.
unsafe fn path_leads_with_attnum(
    path: *mut pg_sys::Path,
    relid: pg_sys::Index,
    attnum: i16,
) -> bool {
    let pathkeys = (*path).pathkeys;
    if list_len(pathkeys) < 1 {
        return false;
    }
    let pathkey = list_nth_ptr(pathkeys, 0).cast::<pg_sys::PathKey>();
    if pathkey.is_null() {
        return false;
    }
    let eclass = (*pathkey).pk_eclass;
    if eclass.is_null() {
        return false;
    }
    let members = (*eclass).ec_members;
    let len = list_len(members);
    for idx in 0..len {
        let member = list_nth_ptr(members, idx).cast::<pg_sys::EquivalenceMember>();
        if member.is_null() {
            continue;
        }
        let expr = (*member).em_expr.cast::<pg_sys::Node>();
        if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_Var {
            continue;
        }
        let var = expr.cast::<pg_sys::Var>();
        let scanrelid = i32::try_from(relid).unwrap_or(i32::MAX);
        if (*var).varno == scanrelid && (*var).varattno == attnum && (*var).varlevelsup == 0 {
            return true;
        }
    }
    false
}

/// True when the hot child produces DESC order for its leading pathkey.
///
/// Prefer [`IndexPath::indexscandir`] when the child is an index path: that is
/// the direction PostgreSQL will actually scan. Fall back to the leading
/// pathkey's btree strategy. Missing metadata defaults to ASC — fail-open to
/// DESC incorrectly skips lower cold keys on `ORDER BY … ASC LIMIT`.
unsafe fn path_leading_descending(path: *mut pg_sys::Path) -> bool {
    if path.is_null() {
        return false;
    }
    if (*path).type_ == pg_sys::NodeTag::T_IndexPath {
        let index_path = path.cast::<pg_sys::IndexPath>();
        if !index_path.is_null() {
            match (*index_path).indexscandir {
                pg_sys::ScanDirection::BackwardScanDirection => return true,
                pg_sys::ScanDirection::ForwardScanDirection => return false,
                _ => {}
            }
        }
    }
    if (*path).pathkeys.is_null() || list_len((*path).pathkeys) < 1 {
        return false;
    }
    let pathkey = list_nth_ptr((*path).pathkeys, 0).cast::<pg_sys::PathKey>();
    if pathkey.is_null() {
        return false;
    }
    ((*pathkey).pk_strategy as u32) == pg_sys::BTGreaterStrategyNumber
}

/// Reads ordered ASC/DESC marker from path private (`true` = DESC).
///
/// Missing/invalid private data defaults to ASC (see [`path_leading_descending`]).
#[must_use]
pub(crate) unsafe fn order_descending_from_path_private(private: *mut pg_sys::List) -> bool {
    order_descending_flag(list_integer_at(
        private,
        PATH_PRIVATE_ORDER_DESCENDING_INDEX,
    ))
}

/// Returns all non-custom paths from a relation pathlist.
pub(crate) unsafe fn collect_native_paths(pathlist: *mut pg_sys::List) -> Vec<*mut pg_sys::Path> {
    let len = list_len(pathlist);
    let mut out = Vec::with_capacity(len as usize);
    for idx in 0..len {
        let path = list_nth_ptr(pathlist, idx).cast::<pg_sys::Path>();
        if path.is_null() || (*path).type_ == pg_sys::NodeTag::T_CustomPath {
            continue;
        }
        out.push(path);
    }
    out
}
