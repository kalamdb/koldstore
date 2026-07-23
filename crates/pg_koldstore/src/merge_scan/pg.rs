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

use koldstore_merge::scan::{MergeScanPlan, MirrorOverlayStrategy, CUSTOM_PATH_NAME};
use pgrx::pg_sys;

mod cold;
mod emit;
mod execute;
mod hot;
mod literals;
mod mirror;
mod profile;
mod qual;
mod spi_query;
mod tuple;

use cold::planned_cold_read_profile;
use profile::{ColdReadProfile, EmitPath, ScanExecutionProfile, ScanProfileSink, ScanProfiler};
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
    hot_plan_label: String,
    emit_path: EmitPath,
    /// Allocated only when PostgreSQL instruments this node for EXPLAIN.
    execution: Option<Box<ScanExecutionProfile>>,
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
    // PostgreSQL attaches PlanState.instrument only after BeginCustomScan
    // returns. EState already carries the same native instrumentation request.
    let instrumentation = estate.as_ref().map_or(0, |estate| estate.es_instrument);
    let mut profiler = ScanProfiler::from_instrumentation(instrumentation);
    if profiler.is_enabled() {
        profile::clear_completed_explain_state(node as usize);
    }
    let scan_started = profiler.start_timer();
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

    let metadata_started = profiler.start_timer();
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
    let pk_point_lookup =
        hot::equality_covers_primary_key(&pk_equality, &snapshot.primary_key_columns);

    profiler.record_metadata(metadata_started);
    let source_inputs = execute::ScanSourceInputs {
        node,
        estate,
        eflags,
        table_oid,
        scanrelid,
        relation_owner,
        relation: &relation,
        snapshot: &snapshot,
        catalog: catalog.as_ref(),
        qual,
        params,
        projection: &scan_projection,
        image_columns: &image_columns,
        pk_equality: &pk_equality,
        pk_point_lookup,
    };
    let source_execution = if profiler.is_enabled() {
        unsafe { execute::execute_scan_sources(source_inputs, &mut profiler) }
    } else {
        unsafe { execute::execute_scan_sources_unprofiled(source_inputs) }
    };
    let execute::ScanSourceExecution {
        mode,
        mut cold_profile,
        emit_path,
        hot_rows,
        memory,
    } = source_execution;

    let hot_plan_label = hot_child_explain_label(node);
    cold_profile.segments_opened = cold_profile.segments.len();
    let execution = profiler.finish(hot_rows, scan_started);

    SCAN_STATES.with(|states| {
        states.borrow_mut().insert(
            node as usize,
            ScanExecutionState {
                mode,
                cold_profile,
                hot_plan_label,
                emit_path,
                execution,
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
            if let Some(mut scan) = states.borrow_mut().remove(&(node as usize)) {
                if let Some(mut execution) = scan.execution.take() {
                    if scan.emit_path == EmitPath::HotChild {
                        if let Some(rows) = hot_child_instrumented_rows(node) {
                            execution.hot_rows = rows;
                            execution.merge_input_rows = rows;
                            execution.merge_output_rows = rows;
                        }
                    }
                    profile::remember_completed_explain_state(
                        node as usize,
                        profile::CompletedExplainState {
                            cold_profile: scan.cold_profile,
                            hot_plan_label: scan.hot_plan_label,
                            emit_path: scan.emit_path,
                            execution,
                        },
                    );
                }
                // `scan` drops here, releasing ScanMemory and buffered Datums.
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

    let execution_meta = if (*es).analyze {
        let active = SCAN_STATES.with(|states| {
            states.borrow().get(&(node as usize)).map(|scan| {
                (
                    scan.cold_profile.clone(),
                    scan.hot_plan_label.clone(),
                    scan.emit_path,
                    scan.execution.as_deref().cloned(),
                )
            })
        });
        active.or_else(|| {
            profile::take_completed_explain_state(node as usize).map(|scan| {
                (
                    scan.cold_profile,
                    scan.hot_plan_label,
                    scan.emit_path,
                    Some(*scan.execution),
                )
            })
        })
    } else {
        None
    };
    let (cold_profile, hot_label, emit_path, mut execution) = match execution_meta {
        Some((cold_profile, hot_plan_label, emit_path, execution)) => {
            (cold_profile, hot_plan_label, emit_path, execution)
        }
        None => {
            // EXPLAIN without ANALYZE: inspect catalog metadata, but do not claim
            // that any source, overlay, or merge phase executed.
            let cold_profile = match resolve_table_oid(node).and_then(planned_cold_read_profile) {
                Ok(profile) => profile,
                Err(error) => {
                    profile::explain_property(es, "Cold Storage", &format!("unavailable: {error}"));
                    return;
                }
            };
            (
                cold_profile,
                hot_child_explain_label(node),
                EmitPath::default(),
                None,
            )
        }
    };

    if emit_path == EmitPath::HotChild {
        if let (Some(execution), Some(rows)) =
            (execution.as_mut(), hot_child_instrumented_rows(node))
        {
            execution.hot_rows = rows;
            execution.merge_input_rows = rows;
            execution.merge_output_rows = rows;
        }
    }

    if !hot_label.is_empty() && hot_child_planstate(node).is_none() {
        // Fallback when the hot child was not initialized into custom_ps (cold
        // emit paths). Graph clients that walk custom_ps still see nested Plans
        // when the child was initialized for hot-only streaming.
        profile::explain_property(es, "Hot Plan", &hot_label);
    }
    if let Some(execution) = execution.as_ref() {
        profile::explain_integer(es, "Mirror Tombstones", None, execution.mirror_rows as i64);
    }
    profile::explain_scan_profile(es, &cold_profile, &hot_label, emit_path, execution.as_ref());
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

/// Returns rows emitted by the native hot child from PostgreSQL instrumentation.
///
/// The child is instrumented whenever the parent is executing under
/// `EXPLAIN ANALYZE`, so KoldMergeScan does not need per-row bookkeeping.
unsafe fn hot_child_instrumented_rows(node: *mut pg_sys::CustomScanState) -> Option<usize> {
    let child = hot_child_planstate(node)?;
    let instrumentation = (*child).instrument.as_ref()?;
    let rows = if instrumentation.nloops > 0.0 {
        instrumentation.ntuples
    } else {
        instrumentation.tuplecount
    };
    rows.is_finite().then(|| rows.max(0.0).round() as usize)
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
