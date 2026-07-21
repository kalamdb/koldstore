//! PostgreSQL CustomScan wiring for managed hot/cold reads.
//!
//! `KoldMergeScan` is a merge coordinator over:
//! - a native PostgreSQL hot child plan (`custom_paths` / `custom_plans`)
//! - streaming cold Parquet reads
//! - an immediate mirror overlay for unflushed inserts/updates/tombstones
#![allow(unsafe_op_in_unsafe_fn)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};

use koldstore_merge::scan::{
    execute_merge_scan, MergeScanPlan, MirrorOverlayStrategy, CUSTOM_PATH_NAME,
};
use pgrx::pg_sys;

mod cold;
mod emit;
mod hot;
mod literals;
mod mirror;
mod profile;
mod qual;
mod tuple;

use cold::{load_cold_rows_for_merge, planned_cold_read_profile};
use emit::materialize_scan_row_from_image;
use hot::{load_hot_rows_for_merge, load_hot_rows_native};
use mirror::{load_mirror_tombstone_overlay, MirrorOverlay};
use profile::{ColdReadProfile, EmitPath};
use qual::{required_scan_projection, residual_filters};
use tuple::{slot_attribute_count, store_materialized_row, MaterializedRow, ScanMemory};

const CUSTOM_SCAN_NAME: &[u8] = b"KoldMergeScan\0";

/// Catalog lookup + merge overlay overhead added on top of the hot child cost.
const CATALOG_LOOKUP_COST: f64 = 10.0;
const MERGE_OVERLAY_COST: f64 = 5.0;
/// Per active cold segment estimate used at plan time (local catalog only).
const COLD_SEGMENT_COST: f64 = 25.0;

thread_local! {
    static SCAN_STATES: RefCell<HashMap<usize, ScanExecutionState>> = RefCell::new(HashMap::new());
    static DISABLE_HOOK: RefCell<bool> = const { RefCell::new(false) };
}

#[derive(Debug)]
enum ScanEmitMode {
    /// Hot-only: pull tuples from the native child plan one at a time.
    HotChild,
    /// Merged/materialized buffer (mirror-filtered). Parent LIMIT stops pulling.
    Buffer {
        rows: Vec<MaterializedRow>,
        next: usize,
        slot_indexes: Vec<usize>,
        tuple_width: usize,
    },
}

impl ScanEmitMode {
    fn buffer(rows: Vec<MaterializedRow>, projection: &qual::ScanProjection<'_>) -> Self {
        Self::Buffer {
            rows,
            next: 0,
            slot_indexes: projection
                .columns
                .iter()
                .map(|column| column.slot_index)
                .collect(),
            tuple_width: projection.tuple_width,
        }
    }
}

#[derive(Debug)]
struct ScanExecutionState {
    mode: ScanEmitMode,
    cold_profile: ColdReadProfile,
    mirror_tombstones: usize,
    mirror_live_overrides: usize,
    hot_plan_label: String,
    emit_path: EmitPath,
    hot_rows: usize,
    result_rows: usize,
    /// Owns all Datums in buffered rows; deleted on EndCustomScan.
    _memory: ScanMemory,
}

impl ScanExecutionState {
    unsafe fn store_next_buffered_row(&mut self, slot: *mut pg_sys::TupleTableSlot) -> bool {
        let ScanEmitMode::Buffer {
            rows,
            next,
            slot_indexes,
            tuple_width,
        } = &mut self.mode
        else {
            return false;
        };
        let Some(row) = rows.get(*next) else {
            return false;
        };
        *next += 1;
        store_materialized_row(slot, row, slot_indexes, *tuple_width);
        true
    }
}

static mut PREVIOUS_SET_REL_PATHLIST_HOOK: pg_sys::set_rel_pathlist_hook_type = None;

static mut PATH_METHODS: pg_sys::CustomPathMethods = pg_sys::CustomPathMethods {
    CustomName: CUSTOM_SCAN_NAME.as_ptr().cast::<c_char>(),
    PlanCustomPath: Some(plan_custom_path),
    ReparameterizeCustomPathByChild: None,
};

static mut SCAN_METHODS: pg_sys::CustomScanMethods = pg_sys::CustomScanMethods {
    CustomName: CUSTOM_SCAN_NAME.as_ptr().cast::<c_char>(),
    CreateCustomScanState: Some(create_custom_scan_state),
};

static mut EXEC_METHODS: pg_sys::CustomExecMethods = pg_sys::CustomExecMethods {
    CustomName: CUSTOM_SCAN_NAME.as_ptr().cast::<c_char>(),
    BeginCustomScan: Some(begin_custom_scan),
    ExecCustomScan: Some(exec_custom_scan),
    EndCustomScan: Some(end_custom_scan),
    ReScanCustomScan: Some(rescan_custom_scan),
    MarkPosCustomScan: None,
    RestrPosCustomScan: None,
    EstimateDSMCustomScan: None,
    InitializeDSMCustomScan: None,
    ReInitializeDSMCustomScan: None,
    InitializeWorkerCustomScan: None,
    // Must not share EndCustomScan: ExecutorFinish always calls Shutdown first.
    // Dropping scan state there leaves EXPLAIN ANALYZE reading freed child planstate.
    ShutdownCustomScan: None,
    ExplainCustomScan: Some(explain_custom_scan),
};

/// Registers KoldMergeScan with PostgreSQL and installs the planner hook.
pub fn register_custom_scan_hooks() {
    unsafe {
        pg_sys::RegisterCustomScanMethods(&raw const SCAN_METHODS);
        PREVIOUS_SET_REL_PATHLIST_HOOK = pg_sys::set_rel_pathlist_hook;
        pg_sys::set_rel_pathlist_hook = Some(set_rel_pathlist);
    }
}

/// Runs extension-internal SQL without injecting KoldMergeScan paths.
pub fn with_custom_scan_disabled<T>(f: impl FnOnce() -> T) -> T {
    with_hook_disabled(f)
}

/// Disables the planner hook for nested catalog/SPI work.
pub(crate) fn with_hook_disabled<T>(f: impl FnOnce() -> T) -> T {
    DISABLE_HOOK.with(|disabled| {
        let was_disabled = *disabled.borrow();
        *disabled.borrow_mut() = true;
        let result = f();
        *disabled.borrow_mut() = was_disabled;
        result
    })
}

/// Returns whether `koldstore.schemas` is present in the catalogs.
///
/// Planner hooks must not SPI-query the managed catalog while CREATE EXTENSION
/// (or DROP) is still building it. Syscache avoids nested planning.
fn managed_catalog_ready() -> bool {
    unsafe {
        let namespace = pgrx::pg_sys::get_namespace_oid(c"koldstore".as_ptr(), true);
        if namespace == pgrx::pg_sys::InvalidOid {
            return false;
        }
        pgrx::pg_sys::get_relname_relid(c"schemas".as_ptr(), namespace) != pgrx::pg_sys::InvalidOid
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn set_rel_pathlist(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    rti: pg_sys::Index,
    rte: *mut pg_sys::RangeTblEntry,
) {
    if let Some(previous) = PREVIOUS_SET_REL_PATHLIST_HOOK {
        previous(root, rel, rti, rte);
    }

    if root.is_null() || rel.is_null() || rte.is_null() {
        return;
    }
    if DISABLE_HOOK.with(|disabled| *disabled.borrow()) {
        return;
    }
    if (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
        return;
    }
    if (*root).parse.is_null() {
        return;
    }
    if (*(*root).parse).commandType != pg_sys::CmdType::CMD_SELECT {
        return;
    }
    if !managed_catalog_ready() {
        return;
    }

    let table_oid = (*rte).relid;
    let managed = with_hook_disabled(|| crate::catalog::cache::is_managed_relation(table_oid));
    if !managed {
        return;
    }

    // Always inject KoldMergeScan for managed SELECTs. When enable_merge_scan is
    // off, BeginCustomScan errors instead of silently reading heap-only.
    let Some(hot_child) = find_cheapest_path((*rel).pathlist) else {
        return;
    };

    let segment_count = with_hook_disabled(|| {
        crate::catalog::cache::cached_manifest_segment_stats(table_oid, &[])
            .ok()
            .flatten()
            .map(|stats| stats.segments.len())
            .unwrap_or(0)
    });
    let cold_cost = segment_count as f64 * COLD_SEGMENT_COST;
    let startup_cost = (*hot_child).startup_cost + CATALOG_LOOKUP_COST;
    let total_cost = (*hot_child).total_cost + CATALOG_LOOKUP_COST + cold_cost + MERGE_OVERLAY_COST;

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
    (*custom_path).methods = &raw const PATH_METHODS;

    // Managed reads must expose only KoldMergeScan as a final path. Clear both
    // `pathlist` and `partial_pathlist`: PostgreSQL builds Gather / Gather Merge
    // *after* this hook from leftover partials. Leaving heap IndexScan partials
    // lets `ORDER BY … LIMIT` prefer a hot-heap-only plan that omits cold rows
    // after flush (visible as count(*) > 0 but ordered SELECT returning empty).
    (*rel).pathlist = std::ptr::null_mut();
    (*rel).partial_pathlist = std::ptr::null_mut();
    (*rel).pathlist = pg_sys::lappend(std::ptr::null_mut(), (&raw mut (*custom_path).path).cast());
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn plan_custom_path(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    tlist: *mut pg_sys::List,
    clauses: *mut pg_sys::List,
    custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
    let scan =
        pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScan>()) as *mut pg_sys::CustomScan;
    if scan.is_null() {
        return std::ptr::null_mut();
    }

    let scanrelid = if rel.is_null() { 0 } else { (*rel).relid };
    let table_oid = resolve_rte_oid(_root, scanrelid).unwrap_or(pg_sys::InvalidOid);
    let primary_key = with_hook_disabled(|| {
        crate::catalog::cache::managed_table_snapshot(table_oid)
            .ok()
            .flatten()
            .map(|snapshot| snapshot.primary_key_columns.clone())
            .unwrap_or_default()
    });
    let mut merge_plan = MergeScanPlan::new(table_oid.to_u32(), primary_key);
    merge_plan.scanrelid = scanrelid;
    merge_plan.overlay_strategy = MirrorOverlayStrategy::MirrorMask;
    let private = merge_plan.serialize().unwrap_or_default();

    (*scan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
    (*scan).scan.plan.startup_cost = (*best_path).path.startup_cost;
    (*scan).scan.plan.total_cost = (*best_path).path.total_cost;
    (*scan).scan.plan.plan_rows = (*best_path).path.rows;
    (*scan).scan.plan.targetlist = tlist;
    let actual_clauses = pg_sys::extract_actual_clauses(clauses, false);
    (*scan).scan.plan.qual = actual_clauses;
    (*scan).scan.scanrelid = scanrelid;
    (*scan).flags = (*best_path).flags;
    (*scan).custom_plans = custom_plans;
    // Do not alias `qual` here: Postgres frees `custom_exprs` and `qual` separately.
    (*scan).custom_exprs = std::ptr::null_mut();
    (*scan).custom_private = serialize_custom_private(&private);
    (*scan).custom_scan_tlist = std::ptr::null_mut();
    (*scan).custom_relids = std::ptr::null_mut();
    (*scan).methods = &raw const SCAN_METHODS;

    scan.cast::<pg_sys::Plan>()
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn create_custom_scan_state(
    _cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    let state = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScanState>())
        as *mut pg_sys::CustomScanState;
    if state.is_null() {
        return std::ptr::null_mut();
    }
    (*state).ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
    (*state).methods = &raw const EXEC_METHODS;
    #[cfg(not(feature = "pg15"))]
    {
        (*state).slotOps = &raw const pg_sys::TTSOpsVirtual;
    }
    state.cast::<pg_sys::Node>()
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn begin_custom_scan(
    node: *mut pg_sys::CustomScanState,
    estate: *mut pg_sys::EState,
    eflags: c_int,
) {
    if node.is_null() || (*node).ss.ss_currentRelation.is_null() {
        return;
    }
    if !crate::guc::enable_merge_scan() {
        pgrx::error!(
            "{CUSTOM_PATH_NAME} is required for managed-table SELECT; \
             koldstore.enable_merge_scan is off"
        );
    }

    let table_oid = (*(*node).ss.ss_currentRelation).rd_id;
    let relation_owner = (*(*node).ss.ss_currentRelation)
        .rd_rel
        .as_ref()
        .map_or(pg_sys::InvalidOid, |relation| relation.relowner);
    let plan = (*node).ss.ps.plan;
    let targetlist = if plan.is_null() {
        std::ptr::null_mut()
    } else {
        (*plan).targetlist
    };
    let qual = if plan.is_null() {
        std::ptr::null_mut()
    } else {
        (*plan).qual
    };
    let params = if estate.is_null() {
        std::ptr::null_mut()
    } else {
        (*estate).es_param_list_info
    };

    let _planned = deserialize_custom_private(plan);

    let (relation, catalog, snapshot) = with_hook_disabled(|| {
        let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
        let catalog = crate::catalog::cache::cached_migration_catalog(table_oid)?;
        let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
        Ok::<_, String>((relation, catalog, snapshot))
    })
    .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} catalog lookup failed: {error}"));

    let scanrelid = plan
        .cast::<pg_sys::CustomScan>()
        .as_ref()
        .map_or(0, |scan| scan.scan.scanrelid);
    let tuple_width = unsafe { slot_attribute_count((*node).ss.ss_ScanTupleSlot) }
        .unwrap_or_else(|| pgrx::error!("{CUSTOM_PATH_NAME} scan tuple descriptor is unavailable"));
    let scan_projection = unsafe {
        required_scan_projection(
            table_oid,
            scanrelid,
            targetlist,
            qual,
            &catalog.columns,
            tuple_width,
        )
    }
    .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} projection failed: {error}"));
    let residual =
        unsafe { residual_filters(table_oid, scanrelid, qual, &catalog.columns, params) };
    // Source reads may only push immutable primary-key equality. All other
    // predicates, especially RLS/security quals, run after winner resolution.
    let primary_keys = snapshot
        .primary_key_columns
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    let pk_equality = residual
        .hot_equality
        .iter()
        .filter(|filter| primary_keys.contains(filter.column.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let image_columns = scan_projection.catalog_columns();

    let (mut cold_profile, cold_rows) = match load_cold_rows_for_merge(
        table_oid,
        scanrelid,
        &snapshot,
        catalog.as_ref(),
        qual,
        &image_columns,
        params,
    ) {
        Ok(result) => result,
        Err(error) => pgrx::error!("{CUSTOM_PATH_NAME} cold read failed: {error}"),
    };

    // Only load mirror tombstones when cold rows could still be visible.
    // Hot-only scans skip the overlay entirely (critical for pre-flush PK lookups).
    let overlay = if cold_rows.is_empty() {
        MirrorOverlay::default()
    } else {
        match load_mirror_tombstone_overlay(
            &snapshot.mirror_relation,
            &snapshot.primary_key_columns,
            &pk_equality,
        ) {
            Ok(overlay) => overlay,
            Err(error) => pgrx::error!("{CUSTOM_PATH_NAME} mirror overlay failed: {error}"),
        }
    };

    let cold_rows = filter_cold_rows_with_overlay(cold_rows, &overlay);
    // PostgreSQL does not initialize `custom_plans` for us; do it only on the
    // hot-only path so cold merge does not pay for an unused heap child.
    if cold_rows.is_empty() && cold_profile.segments.is_empty() {
        initialize_custom_plan_children(node, estate, eflags);
    }
    let hot_plan_label = hot_child_explain_label(node);
    let has_hot_child = hot_child_planstate(node).is_some();

    let mut memory = ScanMemory::create("KoldMergeScan");

    // Hot-only fast path: stream the native child when present and cold is empty.
    let (mode, emit_path, hot_rows_count) = if cold_rows.is_empty()
        && cold_profile.segments.is_empty()
        && has_hot_child
    {
        (ScanEmitMode::HotChild, EmitPath::HotChild, 0)
    } else if cold_rows.is_empty() && cold_profile.segments.is_empty() {
        match crate::catalog::owner::with_relation_owner_for_merge(relation_owner, || {
            load_hot_rows_native(
                &relation,
                &pk_equality,
                &image_columns,
                &scan_projection,
                &mut memory,
            )
        }) {
            Ok(rows) => {
                let hot_count = rows.len();
                (
                    ScanEmitMode::buffer(rows, &scan_projection),
                    EmitPath::HotNative,
                    hot_count,
                )
            }
            Err(error) => pgrx::error!("{CUSTOM_PATH_NAME} hot-only read failed: {error}"),
        }
    } else if !cold_rows.is_empty()
        && hot_equality_covers_primary_key(&pk_equality, &snapshot.primary_key_columns)
    {
        // PK point-lookup fast path: probe hot with native Datums (no to_jsonb).
        // Hot always wins for the same PK, so a hit skips cold merge entirely.
        // A miss materializes cold winners without the JSON hot merge path.
        match crate::catalog::owner::with_relation_owner_for_merge(relation_owner, || {
            load_hot_rows_native(
                &relation,
                &pk_equality,
                &image_columns,
                &scan_projection,
                &mut memory,
            )
        }) {
            Ok(rows) if !rows.is_empty() => {
                let hot_count = rows.len();
                (
                    ScanEmitMode::buffer(rows, &scan_projection),
                    EmitPath::HotNative,
                    hot_count,
                )
            }
            Ok(_) => {
                let merged = match execute_merge_scan(Vec::new(), cold_rows) {
                    Ok(result) => result,
                    Err(error) => {
                        pgrx::error!("{CUSTOM_PATH_NAME} cold-native merge failed: {error}")
                    }
                };
                let rows = unsafe {
                    memory.switch(|| {
                        merged
                            .rows
                            .iter()
                            .map(|row| {
                                materialize_scan_row_from_image(&row.row_image, &scan_projection)
                            })
                            .collect::<Result<Vec<_>, _>>()
                    })
                }
                .unwrap_or_else(|error| {
                    pgrx::error!("{CUSTOM_PATH_NAME} cold-native emit failed: {error}")
                });
                (
                    ScanEmitMode::buffer(rows, &scan_projection),
                    EmitPath::ColdNative,
                    0,
                )
            }
            Err(error) => pgrx::error!("{CUSTOM_PATH_NAME} hot probe failed: {error}"),
        }
    } else {
        let hot_rows =
            match crate::catalog::owner::with_relation_owner_for_merge(relation_owner, || {
                load_hot_rows_for_merge(&relation, &snapshot, &pk_equality, &image_columns)
            }) {
                Ok(rows) => rows,
                Err(error) => pgrx::error!("{CUSTOM_PATH_NAME} hot read failed: {error}"),
            };
        let hot_count = hot_rows.len();
        let merged = match execute_merge_scan(hot_rows, cold_rows) {
            Ok(result) => result,
            Err(error) => pgrx::error!("{CUSTOM_PATH_NAME} merge failed: {error}"),
        };

        let rows = unsafe {
            memory.switch(|| {
                merged
                    .rows
                    .iter()
                    .map(|row| materialize_scan_row_from_image(&row.row_image, &scan_projection))
                    .collect::<Result<Vec<_>, _>>()
            })
        }
        .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} emit failed: {error}"));
        (
            ScanEmitMode::buffer(rows, &scan_projection),
            EmitPath::MergeBuffer,
            hot_count,
        )
    };

    cold_profile.segments_opened = cold_profile.segments.len();
    let result_rows = match &mode {
        ScanEmitMode::HotChild => 0,
        ScanEmitMode::Buffer { rows, .. } => rows.len(),
    };

    SCAN_STATES.with(|states| {
        states.borrow_mut().insert(
            node as usize,
            ScanExecutionState {
                mode,
                cold_profile,
                mirror_tombstones: overlay.tombstones,
                mirror_live_overrides: overlay.live_overrides,
                hot_plan_label,
                emit_path,
                hot_rows: hot_rows_count,
                result_rows,
                _memory: memory,
            },
        );
    });
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn exec_custom_scan(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    if node.is_null() {
        return std::ptr::null_mut();
    }
    let slot = (*node).ss.ps.ps_ResultTupleSlot;
    if slot.is_null() {
        return std::ptr::null_mut();
    }

    // Cooperative cancel between streamed rows.
    pgrx::check_for_interrupts!();

    let use_child = SCAN_STATES.with(|states| {
        states
            .borrow()
            .get(&(node as usize))
            .is_some_and(|scan| matches!(scan.mode, ScanEmitMode::HotChild))
    });

    if use_child {
        return exec_hot_child_slot(node, slot);
    }

    // Buffered merge rows are base-relation scan tuples. ExecScan applies the
    // ExprState compiled from plan.qual (including RLS/security quals), counts
    // rejected rows, and projects into ps_ResultTupleSlot.
    pg_sys::ExecScan(
        &raw mut (*node).ss,
        Some(next_buffered_scan_tuple),
        Some(recheck_buffered_scan_tuple),
    )
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn next_buffered_scan_tuple(
    scan_state: *mut pg_sys::ScanState,
) -> *mut pg_sys::TupleTableSlot {
    if scan_state.is_null() {
        return std::ptr::null_mut();
    }
    let node = scan_state.cast::<pg_sys::CustomScanState>();
    let slot = (*scan_state).ss_ScanTupleSlot;
    if slot.is_null() {
        return std::ptr::null_mut();
    }

    let stored = SCAN_STATES.with(|states| {
        let mut states = states.borrow_mut();
        let scan = states.get_mut(&(node as usize))?;
        Some(scan.store_next_buffered_row(slot))
    });

    if stored == Some(true) {
        slot
    } else {
        std::ptr::null_mut()
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn recheck_buffered_scan_tuple(
    _scan_state: *mut pg_sys::ScanState,
    _slot: *mut pg_sys::TupleTableSlot,
) -> bool {
    true
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn end_custom_scan(node: *mut pg_sys::CustomScanState) {
    if !node.is_null() {
        SCAN_STATES.with(|states| {
            if let Some(scan) = states.borrow_mut().remove(&(node as usize)) {
                if should_keep_explain_profile(node) {
                    profile::remember_explain_profile(
                        node as usize,
                        profile::ExplainScanMeta {
                            cold_profile: scan.cold_profile,
                            hot_plan_label: scan.hot_plan_label,
                            mirror_tombstones: scan.mirror_tombstones,
                            mirror_live_overrides: scan.mirror_live_overrides,
                            emit_path: scan.emit_path,
                            hot_rows: scan.hot_rows,
                            result_rows: scan.result_rows,
                        },
                    );
                }
                // `scan` drops here: releases ScanMemory / buffered Datums.
            }
        });
        end_custom_plan_children(node);
    }
}

/// Initializes planned native children selected for hot-only delegation.
///
/// PostgreSQL leaves `custom_plans` initialization to the custom provider.
///
/// # Safety
///
/// `node`, `estate`, and every entry in `custom_plans` must belong to the active
/// executor invocation. The caller must invoke this at most once per node.
unsafe fn initialize_custom_plan_children(
    node: *mut pg_sys::CustomScanState,
    estate: *mut pg_sys::EState,
    eflags: c_int,
) {
    if node.is_null() || estate.is_null() || !(*node).custom_ps.is_null() {
        return;
    }
    let plan = (*node).ss.ps.plan;
    if plan.is_null() {
        return;
    }
    let custom_scan = plan.cast::<pg_sys::CustomScan>();
    let planned_children = (*custom_scan).custom_plans;
    for index in 0..list_len(planned_children) {
        let child_plan = list_nth_ptr(planned_children, index).cast::<pg_sys::Plan>();
        if child_plan.is_null() {
            continue;
        }
        let child_state = pg_sys::ExecInitNode(child_plan, estate, eflags);
        if !child_state.is_null() {
            (*node).custom_ps = pg_sys::lappend((*node).custom_ps, child_state.cast::<c_void>());
        }
    }
}

/// Ends native children initialized by [`initialize_custom_plan_children`].
///
/// # Safety
///
/// `node.custom_ps` must contain only live `PlanState` pointers owned by `node`.
unsafe fn end_custom_plan_children(node: *mut pg_sys::CustomScanState) {
    let children = (*node).custom_ps;
    for index in 0..list_len(children) {
        let child = list_nth_ptr(children, index).cast::<pg_sys::PlanState>();
        if !child.is_null() {
            pg_sys::ExecEndNode(child);
        }
    }
    (*node).custom_ps = std::ptr::null_mut();
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn rescan_custom_scan(node: *mut pg_sys::CustomScanState) {
    if node.is_null() {
        return;
    }
    pg_sys::ExecScanReScan(&raw mut (*node).ss);
    if let Some(child) = hot_child_planstate(node) {
        pg_sys::ExecReScan(child);
    }
    SCAN_STATES.with(|states| {
        if let Some(scan) = states.borrow_mut().get_mut(&(node as usize)) {
            if let ScanEmitMode::Buffer { next, .. } = &mut scan.mode {
                *next = 0;
            }
        }
    });
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn explain_custom_scan(
    node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    if node.is_null() || es.is_null() {
        return;
    }

    let profile = SCAN_STATES.with(|states| {
        states.borrow().get(&(node as usize)).map(|scan| {
            (
                scan.cold_profile.clone(),
                scan.hot_plan_label.clone(),
                scan.mirror_tombstones,
                scan.mirror_live_overrides,
                scan.emit_path,
                scan.hot_rows,
                scan.result_rows,
            )
        })
    });
    let (profile, hot_label, tombstones, live_overrides, emit_path, hot_rows, result_rows) =
        match profile {
            Some(meta) => meta,
            None => match profile::saved_explain_profile(node as usize) {
                Some(meta) => (
                    meta.cold_profile,
                    meta.hot_plan_label,
                    meta.mirror_tombstones,
                    meta.mirror_live_overrides,
                    meta.emit_path,
                    meta.hot_rows,
                    meta.result_rows,
                ),
                None => {
                    // EXPLAIN without ANALYZE (or missing saved meta): plan-time cold profile only.
                    // Do not touch custom_ps here — children may already be shut down.
                    let profile = match resolve_table_oid(node).and_then(planned_cold_read_profile)
                    {
                        Ok(profile) => profile,
                        Err(error) => {
                            profile::explain_property(
                                es,
                                "Cold storage",
                                &format!("unavailable: {error}"),
                            );
                            return;
                        }
                    };
                    (profile, String::new(), 0, 0, EmitPath::default(), 0, 0)
                }
            },
        };

    if !hot_label.is_empty() {
        profile::explain_property(es, "Hot Plan", &hot_label);
    }
    profile::explain_property(es, "Mirror Tombstones", &tombstones.to_string());
    profile::explain_property(es, "Mirror Overrides", &live_overrides.to_string());
    profile::explain_cold_read_profile(es, &profile, emit_path, hot_rows, result_rows);

    if (*es).analyze {
        profile::forget_explain_profile(node as usize);
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
    if list.is_null() || index < 0 || index >= (*list).length || (*list).elements.is_null() {
        return std::ptr::null_mut();
    }
    (*(*list).elements.add(index as usize)).ptr_value
}

unsafe fn exec_proc_node(node: *mut pg_sys::PlanState) -> *mut pg_sys::TupleTableSlot {
    let Some(exec) = (*node).ExecProcNode else {
        return std::ptr::null_mut();
    };
    exec(node)
}

unsafe fn exec_copy_slot(dst: *mut pg_sys::TupleTableSlot, src: *mut pg_sys::TupleTableSlot) {
    if let Some(ops) = (*dst).tts_ops.as_ref() {
        if let Some(copy) = ops.copyslot {
            copy(dst, src);
            return;
        }
    }
    if let Some(ops) = (*src).tts_ops.as_ref() {
        if let Some(copy) = ops.copyslot {
            copy(dst, src);
        }
    }
}

unsafe fn exec_hot_child_slot(
    node: *mut pg_sys::CustomScanState,
    result_slot: *mut pg_sys::TupleTableSlot,
) -> *mut pg_sys::TupleTableSlot {
    let Some(child) = hot_child_planstate(node) else {
        return std::ptr::null_mut();
    };
    let child_slot = exec_proc_node(child);
    if child_slot.is_null() {
        return std::ptr::null_mut();
    }
    if (*child_slot).tts_nvalid == 0
        && ((*child_slot).tts_flags & pg_sys::TTS_FLAG_EMPTY as u16) != 0
    {
        return std::ptr::null_mut();
    }
    exec_copy_slot(result_slot, child_slot);
    result_slot
}

unsafe fn hot_child_planstate(
    node: *mut pg_sys::CustomScanState,
) -> Option<*mut pg_sys::PlanState> {
    let list = (*node).custom_ps;
    if list_len(list) < 1 {
        return None;
    }
    let child = list_nth_ptr(list, 0) as *mut pg_sys::PlanState;
    if child.is_null() {
        None
    } else {
        Some(child)
    }
}

unsafe fn hot_child_explain_label(node: *mut pg_sys::CustomScanState) -> String {
    let plan = (*node).ss.ps.plan;
    if plan.is_null() {
        return String::new();
    }
    let custom_scan = plan.cast::<pg_sys::CustomScan>();
    let plans = (*custom_scan).custom_plans;
    if list_len(plans) < 1 {
        return "native child".to_string();
    }
    let child = list_nth_ptr(plans, 0) as *mut pg_sys::Plan;
    if child.is_null() {
        return "native child".to_string();
    }
    match (*child).type_ {
        pg_sys::NodeTag::T_IndexScan | pg_sys::NodeTag::T_IndexOnlyScan => "Index Scan".to_string(),
        pg_sys::NodeTag::T_BitmapHeapScan => "Bitmap Heap Scan".to_string(),
        pg_sys::NodeTag::T_SeqScan => "Seq Scan".to_string(),
        _ => format!("{:?}", (*child).type_),
    }
}

/// Returns true when equality filters cover every primary-key column.
fn hot_equality_covers_primary_key(
    filters: &[hot::HotEqualityFilter],
    primary_key_columns: &[String],
) -> bool {
    !primary_key_columns.is_empty()
        && primary_key_columns.iter().all(|pk| {
            filters
                .iter()
                .any(|filter| filter.column.eq_ignore_ascii_case(pk))
        })
}

fn filter_cold_rows_with_overlay(
    cold_rows: Vec<koldstore_common::ColdRow>,
    overlay: &MirrorOverlay,
) -> Vec<koldstore_common::ColdRow> {
    if overlay.is_empty() {
        return cold_rows;
    }
    cold_rows
        .into_iter()
        .filter(|row| !overlay.masks_pk(&row.pk))
        .collect()
}

unsafe fn find_cheapest_path(pathlist: *mut pg_sys::List) -> Option<*mut pg_sys::Path> {
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

unsafe fn serialize_custom_private(payload: &str) -> *mut pg_sys::List {
    if payload.is_empty() {
        return std::ptr::null_mut();
    }
    let cstr = match CString::new(payload) {
        Ok(value) => value,
        Err(_) => return std::ptr::null_mut(),
    };
    let copied = pg_sys::pstrdup(cstr.as_ptr());
    let value = pg_sys::makeString(copied);
    pg_sys::lappend(std::ptr::null_mut(), value.cast::<c_void>())
}

unsafe fn deserialize_custom_private(plan: *mut pg_sys::Plan) -> Option<MergeScanPlan> {
    if plan.is_null() {
        return None;
    }
    let custom_scan = plan.cast::<pg_sys::CustomScan>();
    let private = (*custom_scan).custom_private;
    if list_len(private) < 1 {
        return None;
    }
    let string_node = list_nth_ptr(private, 0) as *mut pg_sys::String;
    if string_node.is_null() {
        return None;
    }
    let ptr = (*string_node).sval;
    if ptr.is_null() {
        return None;
    }
    let payload = std::ffi::CStr::from_ptr(ptr).to_string_lossy();
    MergeScanPlan::deserialize(&payload).ok()
}

unsafe fn resolve_rte_oid(
    root: *mut pg_sys::PlannerInfo,
    scanrelid: pg_sys::Index,
) -> Option<pg_sys::Oid> {
    if root.is_null() || scanrelid == 0 || (*root).parse.is_null() {
        return None;
    }
    let rte = pg_sys::rt_fetch(scanrelid, (*(*root).parse).rtable);
    if rte.is_null() {
        None
    } else {
        Some((*rte).relid)
    }
}

unsafe fn resolve_table_oid(node: *mut pg_sys::CustomScanState) -> Result<pg_sys::Oid, String> {
    if !(*node).ss.ss_currentRelation.is_null() {
        return Ok((*(*node).ss.ss_currentRelation).rd_id);
    }

    let plan = (*node).ss.ps.plan;
    if plan.is_null() {
        return Err("custom scan plan is missing".to_string());
    }
    let custom_scan = plan.cast::<pg_sys::CustomScan>();
    let scanrelid = (*custom_scan).scan.scanrelid;
    if scanrelid == 0 {
        return Err("custom scan relid is missing".to_string());
    }

    let estate = (*node).ss.ps.state;
    if estate.is_null() {
        return Err("executor state is missing".to_string());
    }
    let rte = pg_sys::rt_fetch(scanrelid, (*estate).es_range_table);
    if rte.is_null() {
        return Err("range table entry is missing".to_string());
    }
    Ok((*rte).relid)
}

unsafe fn should_keep_explain_profile(node: *mut pg_sys::CustomScanState) -> bool {
    !(*node).ss.ps.instrument.is_null()
}
