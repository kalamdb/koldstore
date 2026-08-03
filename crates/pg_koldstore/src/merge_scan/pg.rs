//! PostgreSQL CustomScan wiring for managed hot/cold reads.
//!
//! `KoldMergeScan` is a merge coordinator over:
//! - a native PostgreSQL hot child plan (`custom_paths` / `custom_plans`)
//! - streaming cold Parquet reads
//! - an immediate mirror overlay for unflushed inserts/updates/tombstones
#![allow(unsafe_op_in_unsafe_fn)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::os::raw::{c_char, c_int, c_void};
#[cfg(feature = "pg_test")]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use koldstore_merge::scan::CUSTOM_PATH_NAME;
use pgrx::pg_sys;

mod cold;
mod cold_frontier;
mod emit;
mod execute;
mod hot;
mod hot_cursor;
mod keyset;
mod literals;
mod mirror;
mod path_strategy;
mod profile;
mod qual;
mod spi_query;
mod tuple;

use cold::{cold_side_proven_empty, planned_cold_read_profile};
use path_strategy::{
    install_path_portfolio, leading_column_id_from_path_private,
    order_descending_from_path_private, path_strategy_tag_from_private,
    scope_key_from_path_private, sort_order_id_from_path_private, strategy_explain_label,
    PortfolioInstallArgs, STRATEGY_TAG_GENERAL_MERGE, STRATEGY_TAG_ORDERED_PROGRESSIVE,
};
use profile::{ColdReadProfile, EmitPath, ScanExecutionProfile, ScanProfileSink, ScanProfiler};
use qual::{required_scan_projection, residual_filters};
use tuple::{slot_attribute_count, store_materialized_row, MaterializedRow, ScanMemory};

const CUSTOM_SCAN_NAME: &[u8] = b"KoldMergeScan\0";
const PRIVATE_EXACT_PK_INDEX: i32 = 0;
const PRIVATE_RUNTIME_DELEGATE_SAFE_INDEX: i32 = 1;
const PRIVATE_STRATEGY_TAG_INDEX: i32 = 2;
const PRIVATE_SCOPE_KEY_INDEX: i32 = 3;
#[allow(dead_code)]
const PRIVATE_SORT_ORDER_ID_INDEX: i32 = 4;
const PRIVATE_LEADING_COLUMN_ID_INDEX: i32 = 5;
const PRIVATE_ORDER_DESCENDING_INDEX: i32 = 6;

thread_local! {
    static SCAN_STATES: RefCell<HashMap<usize, ScanExecutionState>> = RefCell::new(HashMap::new());
    static DISABLE_HOOK: RefCell<bool> = const { RefCell::new(false) };
}

#[cfg(feature = "pg_test")]
static FAST_PATH_GLOBAL_STATE_INSERTS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "pg_test")]
static FAST_PATH_TUPLE_COPIES: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "pg_test")]
static FALLBACK_INITIALIZATIONS: AtomicU64 = AtomicU64::new(0);

/// Test-only counters for work forbidden on an uninstrumented exact-PK hot hit.
#[cfg(feature = "pg_test")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FastPathTestCounters {
    pub(crate) global_state_inserts: u64,
    pub(crate) tuple_copies: u64,
    pub(crate) fallback_initializations: u64,
}

/// Resets exact-PK hot-path regression counters.
#[cfg(feature = "pg_test")]
pub(crate) fn reset_fast_path_test_counters() {
    FAST_PATH_GLOBAL_STATE_INSERTS.store(0, Ordering::Relaxed);
    FAST_PATH_TUPLE_COPIES.store(0, Ordering::Relaxed);
    FALLBACK_INITIALIZATIONS.store(0, Ordering::Relaxed);
}

/// Returns exact-PK hot-path regression counters.
#[cfg(feature = "pg_test")]
pub(crate) fn fast_path_test_counters() -> FastPathTestCounters {
    FastPathTestCounters {
        global_state_inserts: FAST_PATH_GLOBAL_STATE_INSERTS.load(Ordering::Relaxed),
        tuple_copies: FAST_PATH_TUPLE_COPIES.load(Ordering::Relaxed),
        fallback_initializations: FALLBACK_INITIALIZATIONS.load(Ordering::Relaxed),
    }
}

#[derive(Debug)]
enum ScanEmitMode {
    /// Hot-only: pull tuples from the native child plan one at a time.
    HotChild {
        /// First tuple probed before KoldStore executor metadata was initialized.
        prefetched: Option<*mut pg_sys::TupleTableSlot>,
    },
    /// Merged/materialized buffer (mirror-filtered). Parent LIMIT stops pulling.
    Buffer {
        rows: Vec<MaterializedRow>,
        next: usize,
        slot_indexes: Vec<usize>,
        tuple_width: usize,
    },
    /// Hot winners plus newest-first cold segment groups emitted lazily.
    Stream {
        stream: Box<execute::MergeRowStream>,
        projection: qual::ScanProjection,
        slot_indexes: Vec<usize>,
        tuple_width: usize,
    },
}

impl ScanEmitMode {
    fn buffer(rows: Vec<MaterializedRow>, projection: &qual::ScanProjection) -> Self {
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

    fn stream(stream: execute::MergeRowStream, projection: qual::ScanProjection) -> Self {
        let slot_indexes = projection
            .columns
            .iter()
            .map(|column| column.slot_index)
            .collect();
        let tuple_width = projection.tuple_width;
        Self::Stream {
            stream: Box::new(stream),
            projection,
            slot_indexes,
            tuple_width,
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
    /// Owns buffered Datums or the current streamed row's pass-by-ref Datums.
    ///
    /// Native hot-child hits need no KoldStore allocation.
    memory: Option<ScanMemory>,
}

/// Provider-owned executor state.
///
/// PostgreSQL explicitly permits providers to place [`pg_sys::CustomScanState`]
/// first in a larger `repr(C)` structure. The compact fields below serve the
/// exact-PK hot probe without consulting [`SCAN_STATES`]. Rich Rust merge state
/// remains in that map only after the conservative fallback is initialized.
#[repr(C)]
struct KoldMergeScanState {
    custom: pg_sys::CustomScanState,
    hot_probe: HotProbeState,
    hot_child: *mut pg_sys::PlanState,
    eflags: c_int,
}

/// Allocation-free exact-PK probe lifecycle.
///
/// `Disabled` is deliberately zero so `palloc0` creates a valid value.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HotProbeState {
    Disabled = 0,
    Pending = 1,
    Hit = 2,
    Fallback = 3,
    Delegate = 4,
}

impl KoldMergeScanState {
    /// Recovers provider state from its first-field PostgreSQL base.
    ///
    /// # Safety
    ///
    /// `node` must have been allocated by [`create_custom_scan_state`].
    unsafe fn from_custom(node: *mut pg_sys::CustomScanState) -> *mut Self {
        node.cast()
    }
}

impl ScanExecutionState {
    unsafe fn store_next_row(&mut self, slot: *mut pg_sys::TupleTableSlot) -> Result<bool, String> {
        let Self {
            mode,
            cold_profile,
            execution,
            memory,
            ..
        } = self;
        match mode {
            ScanEmitMode::HotChild { .. } => Ok(false),
            ScanEmitMode::Buffer {
                rows,
                next,
                slot_indexes,
                tuple_width,
            } => {
                let Some(row) = rows.get(*next) else {
                    return Ok(false);
                };
                *next += 1;
                store_materialized_row(slot, row, slot_indexes, *tuple_width);
                Ok(true)
            }
            ScanEmitMode::Stream {
                stream,
                projection,
                slot_indexes,
                tuple_width,
            } => {
                let memory = memory
                    .as_mut()
                    .ok_or_else(|| "stream scan memory is unavailable".to_string())?;
                memory.reset();
                let Some(row) = stream.next_materialized(
                    projection,
                    memory,
                    cold_profile,
                    execution.as_deref_mut(),
                )?
                else {
                    return Ok(false);
                };
                store_materialized_row(slot, &row, slot_indexes, *tuple_width);
                Ok(true)
            }
        }
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
    crate::catalog::cache::managed_catalog_ready()
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

    let known_manifest = with_hook_disabled(|| {
        match crate::catalog::cache::cached_manifest_planner_hint(table_oid) {
            Ok(Some(hint)) => Some(hint),
            Ok(None) => Some((0, 0)),
            Err(_) => None,
        }
    });
    // Keep PostgreSQL's original paths when the published catalog proves that
    // cold storage cannot contribute. Flush publication broadcasts a relcache
    // invalidation, so cached native plans are rebuilt before the first read
    // that can observe a newly published cold segment. Catalog errors remain
    // fail-closed and continue through KoldMergeScan.
    if known_manifest.is_some_and(|(segment_count, _)| segment_count == 0) {
        return;
    }

    let snapshot = with_hook_disabled(|| {
        crate::catalog::cache::managed_table_snapshot(table_oid)
            .ok()
            .flatten()
    });

    if let Some((_, generation)) = known_manifest {
        let cold_side_empty = with_hook_disabled(|| {
            let Some(snapshot) = snapshot.as_ref() else {
                return Ok(false);
            };
            let catalog = crate::catalog::cache::cached_migration_catalog(table_oid)?;
            let actual_clauses = pg_sys::extract_actual_clauses((*rel).baserestrictinfo, false);
            unsafe {
                cold_side_proven_empty(
                    table_oid,
                    rti,
                    snapshot,
                    &catalog,
                    actual_clauses,
                    generation,
                    std::ptr::null_mut(),
                )
            }
        })
        .unwrap_or(false);
        if cold_side_empty {
            return;
        }
    }

    let segment_count = known_manifest.map_or(0, |(segment_count, _)| segment_count);
    let primary_key_attnums = snapshot
        .as_ref()
        .map(|snap| {
            snap.primary_key_columns
                .iter()
                .map(|column| column.column_id.get())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let segment_order_attnum = snapshot
        .as_ref()
        .and_then(|snap| snap.segment_order_column_id.map(|id| id.get()));
    let actual_clauses = pg_sys::extract_actual_clauses((*rel).baserestrictinfo, false);
    let exact_full_primary_key_equality =
        qual::quals_cover_primary_key(rti, actual_clauses, &primary_key_attnums);

    // Strategy selection and CustomPath portfolio live in `path_strategy`.
    install_path_portfolio(
        rel,
        &PortfolioInstallArgs {
            scanrelid: rti,
            primary_key_attnums,
            segment_order_attnum,
            exact_full_primary_key_equality,
            segment_count,
            scope_key: String::new(),
        },
        &raw const PATH_METHODS,
    );
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn plan_custom_path(
    root: *mut pg_sys::PlannerInfo,
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
    let table_oid = resolve_rte_oid(root, scanrelid).unwrap_or(pg_sys::InvalidOid);
    let primary_key_attnums = with_hook_disabled(|| {
        crate::catalog::cache::managed_table_snapshot(table_oid)
            .ok()
            .flatten()
            .map(|snapshot| {
                snapshot
                    .primary_key_columns
                    .iter()
                    .map(|column| column.column_id.get())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    });
    let actual_clauses = pg_sys::extract_actual_clauses(clauses, false);
    let exact_pk_lookup = (*best_path).path.param_info.is_null()
        && qual::quals_cover_primary_key(scanrelid, actual_clauses, &primary_key_attnums);
    let runtime_delegate_safe = (*best_path).path.param_info.is_null();

    let path_private = (*best_path).custom_private;
    let strategy_tag = path_strategy_tag_from_private(path_private);
    let scope_key = unsafe { scope_key_from_path_private(path_private) };
    let sort_order_id = unsafe { sort_order_id_from_path_private(path_private) };
    let leading_column_id = unsafe { leading_column_id_from_path_private(path_private) };
    let order_descending = unsafe { order_descending_from_path_private(path_private) };

    // Ordered progressive merge reads hot winners from the native child, so the
    // child must project a full physical row (PK + all attrs) even when the
    // query SELECT list is a narrow projection. Final projection stays on the
    // CustomScan targetlist via merge materialization + ExecScan.
    let mut planned_children = custom_plans;
    if strategy_tag == STRATEGY_TAG_ORDERED_PROGRESSIVE && !root.is_null() && !rel.is_null() {
        planned_children = widen_ordered_hot_children(root, rel, custom_plans);
    }

    (*scan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
    (*scan).scan.plan.startup_cost = (*best_path).path.startup_cost;
    (*scan).scan.plan.total_cost = (*best_path).path.total_cost;
    (*scan).scan.plan.plan_rows = (*best_path).path.rows;
    (*scan).scan.plan.targetlist = tlist;
    (*scan).scan.plan.qual = actual_clauses;
    (*scan).scan.scanrelid = scanrelid;
    (*scan).flags = (*best_path).flags;
    (*scan).custom_plans = planned_children;
    // Do not alias `qual` here: Postgres frees `custom_exprs` and `qual` separately.
    (*scan).custom_exprs = std::ptr::null_mut();
    (*scan).custom_private = serialize_custom_private(
        exact_pk_lookup,
        runtime_delegate_safe,
        strategy_tag,
        &scope_key,
        sort_order_id,
        leading_column_id,
        order_descending,
    );
    (*scan).custom_scan_tlist = std::ptr::null_mut();
    (*scan).custom_relids = std::ptr::null_mut();
    (*scan).methods = &raw const SCAN_METHODS;

    scan.cast::<pg_sys::Plan>()
}

/// Widens each ordered-progressive hot child to a physical relation tlist.
///
/// Native hot merge needs PK (and full row images) from `ExecProcNode`; a
/// query-shaped projection like `SELECT body` would omit those attributes.
unsafe fn widen_ordered_hot_children(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::List {
    if custom_plans.is_null() {
        return custom_plans;
    }
    let physical = pg_sys::build_physical_tlist(root, rel);
    if physical.is_null() {
        return custom_plans;
    }
    let len = list_len(custom_plans);
    let mut widened_list: *mut pg_sys::List = std::ptr::null_mut();
    for idx in 0..len {
        let child = list_nth_ptr(custom_plans, idx).cast::<pg_sys::Plan>();
        if child.is_null() {
            continue;
        }
        let parallel_safe = (*child).parallel_safe;
        let widened = pg_sys::change_plan_targetlist(child, physical, parallel_safe);
        let node = if widened.is_null() { child } else { widened };
        widened_list = pg_sys::lappend(widened_list, node.cast::<c_void>());
    }
    if widened_list.is_null() {
        custom_plans
    } else {
        widened_list
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn create_custom_scan_state(
    _cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    let provider =
        pg_sys::palloc0(std::mem::size_of::<KoldMergeScanState>()) as *mut KoldMergeScanState;
    if provider.is_null() {
        return std::ptr::null_mut();
    }
    let state = (&raw mut (*provider).custom).cast::<pg_sys::CustomScanState>();
    (*state).ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
    (*state).methods = &raw const EXEC_METHODS;
    (*provider).hot_probe = HotProbeState::Disabled;
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
    let profiler = ScanProfiler::from_instrumentation(instrumentation);
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

    let plan = (*node).ss.ps.plan;
    let mut release_exhausted_hot_child = false;
    if custom_private_exact_pk(plan) {
        initialize_custom_plan_children(node, estate, eflags);
        if let Some(child) = hot_child_planstate(node) {
            if !profiler.is_enabled() {
                let provider = KoldMergeScanState::from_custom(node);
                (*provider).hot_probe = HotProbeState::Pending;
                (*provider).hot_child = child;
                (*provider).eflags = eflags;
                return;
            }
            let child_slot = exec_proc_node(child);
            if !tuple_slot_is_empty(child_slot) {
                store_profiled_hot_hit(node, child_slot, profiler, scan_started);
                return;
            }
            // EXPLAIN ANALYZE probed the native child, but its miss means the
            // cold fallback now owns the real work. Drop the exhausted child
            // so PostgreSQL does not render it as the only `Plans` entry;
            // the fallback's diagnostic JSON tree then represents the actual
            // catalog, Parquet, overlay, and merge pipeline.
            release_exhausted_hot_child = true;
        }
    }

    if !profiler.is_enabled()
        && custom_private_runtime_delegate_safe(plan)
        && runtime_cold_side_proven_empty(node, estate)
    {
        initialize_custom_plan_children(node, estate, eflags);
        if let Some(child) = hot_child_planstate(node) {
            let provider = KoldMergeScanState::from_custom(node);
            (*provider).hot_probe = HotProbeState::Delegate;
            (*provider).hot_child = child;
            (*provider).eflags = eflags;
            return;
        }
    }

    if release_exhausted_hot_child {
        end_custom_plan_children(node);
    }
    initialize_fallback_scan(node, estate, eflags, profiler, scan_started);
}

/// Resolves executor parameters and proves whether the cold side is empty.
///
/// Failures stay conservative: the normal merge fallback owns user-visible
/// diagnostics and correctness when any catalog metadata is unavailable.
///
/// # Safety
///
/// `node` and `estate` must belong to the active executor invocation.
unsafe fn runtime_cold_side_proven_empty(
    node: *mut pg_sys::CustomScanState,
    estate: *mut pg_sys::EState,
) -> bool {
    if node.is_null() || estate.is_null() || (*node).ss.ss_currentRelation.is_null() {
        return false;
    }
    let table_oid = (*(*node).ss.ss_currentRelation).rd_id;
    let plan = (*node).ss.ps.plan;
    if plan.is_null() {
        return false;
    }
    let scan = plan.cast::<pg_sys::CustomScan>();
    let scanrelid = (*scan).scan.scanrelid;
    let qual = (*plan).qual;
    let params = (*estate).es_param_list_info;

    with_hook_disabled(|| {
        let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "managed table snapshot is unavailable".to_string())?;
        let catalog = crate::catalog::cache::cached_migration_catalog(table_oid)?;
        let manifest_generation = crate::catalog::cache::cached_manifest_planner_hint(table_oid)?
            .ok_or_else(|| "published manifest is unavailable".to_string())?
            .1;
        unsafe {
            cold_side_proven_empty(
                table_oid,
                scanrelid,
                &snapshot,
                &catalog,
                qual,
                manifest_generation,
                params,
            )
        }
    })
    .unwrap_or(false)
}

/// Stores an instrumented exact-PK hot hit for `EXPLAIN ANALYZE`.
///
/// Ordinary execution uses [`KoldMergeScanState`] instead and therefore avoids
/// allocating labels, profiles, and a thread-local hash-map entry.
///
/// # Safety
///
/// `node` and `child_slot` must belong to the active executor invocation, and
/// `child_slot` must contain the first tuple returned by the native hot child.
unsafe fn store_profiled_hot_hit(
    node: *mut pg_sys::CustomScanState,
    child_slot: *mut pg_sys::TupleTableSlot,
    profiler: ScanProfiler,
    scan_started: Option<Instant>,
) {
    let cold_profile = ColdReadProfile::empty("(none)");
    let hot_plan_label = hot_child_explain_label(node);
    let execution = profiler.finish(1, scan_started);
    #[cfg(feature = "pg_test")]
    FAST_PATH_GLOBAL_STATE_INSERTS.fetch_add(1, Ordering::Relaxed);
    SCAN_STATES.with(|states| {
        states.borrow_mut().insert(
            node as usize,
            ScanExecutionState {
                mode: ScanEmitMode::HotChild {
                    prefetched: Some(child_slot),
                },
                cold_profile,
                hot_plan_label,
                emit_path: EmitPath::HotChild,
                execution,
                memory: None,
            },
        );
    });
}

/// Initializes the complete hot/cold merge pipeline after fast-path rejection.
///
/// This is intentionally separate from [`begin_custom_scan`]: an
/// uninstrumented exact-PK lookup calls it only when the native hot child
/// returns no row. General predicates and instrumented scans initialize it
/// eagerly because they must inspect cold storage or expose execution metrics.
///
/// # Safety
///
/// All executor pointers must belong to the current query. The function may be
/// called at most once for a node unless its previous [`SCAN_STATES`] entry has
/// been removed.
unsafe fn initialize_fallback_scan(
    node: *mut pg_sys::CustomScanState,
    estate: *mut pg_sys::EState,
    eflags: c_int,
    mut profiler: ScanProfiler,
    scan_started: Option<Instant>,
) {
    #[cfg(feature = "pg_test")]
    FALLBACK_INITIALIZATIONS.fetch_add(1, Ordering::Relaxed);

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
        required_scan_projection(scanrelid, targetlist, qual, &catalog.columns, tuple_width)
    }
    .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} projection failed: {error}"));
    let residual = unsafe { residual_filters(scanrelid, qual, &catalog.columns, params) };
    // Hot heap is current-state only, so PK + scope equality and PK range
    // predicates can be pushed into the SPI load. Mutable columns stay residual
    // for cold (pre-merge), but may still appear in hot_equality for post-merge
    // ExecScan. Scope pushdown matches catalog segment-index prune on the shared
    // manifest until per-scope manifests land.
    let mut source_equality_columns = snapshot
        .primary_key_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<std::collections::HashSet<_>>();
    if let Some(scope_id) = snapshot.scope_column_id {
        if let Some(scope) = catalog
            .columns
            .iter()
            .find(|column| column.column_id == scope_id)
        {
            source_equality_columns.insert(scope.name.as_str());
        }
    }
    let pk_equality = residual
        .hot_equality
        .iter()
        .filter(|filter| source_equality_columns.contains(filter.column.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let primary_key_columns = snapshot
        .primary_key_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<std::collections::HashSet<_>>();
    let pk_range = residual
        .hot_range
        .iter()
        .filter(|filter| primary_key_columns.contains(filter.column.as_str()))
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
        pk_range: &pk_range,
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
                memory: Some(memory),
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

    // PostgreSQL's nodeCustom.c runs CHECK_FOR_INTERRUPTS immediately before
    // this provider callback. Do not duplicate that per-tuple work here.
    let provider = KoldMergeScanState::from_custom(node);
    match (*provider).hot_probe {
        HotProbeState::Pending => {
            let child = (*provider).hot_child;
            let child_slot = if child.is_null() {
                std::ptr::null_mut()
            } else {
                exec_proc_node(child)
            };
            if !tuple_slot_is_empty(child_slot) {
                (*provider).hot_probe = HotProbeState::Hit;
                // The native child has already applied the complete PostgreSQL
                // qual and projection, so its slot is safe to return directly.
                return child_slot;
            }

            if custom_private_runtime_delegate_safe((*node).ss.ps.plan)
                && runtime_cold_side_proven_empty(node, (*node).ss.ps.state)
            {
                (*provider).hot_probe = HotProbeState::Hit;
                return std::ptr::null_mut();
            }

            (*provider).hot_probe = HotProbeState::Fallback;
            let profiler = ScanProfiler::from_instrumentation(0);
            let scan_started = profiler.start_timer();
            initialize_fallback_scan(
                node,
                (*node).ss.ps.state,
                (*provider).eflags,
                profiler,
                scan_started,
            );
        }
        HotProbeState::Hit => return std::ptr::null_mut(),
        HotProbeState::Delegate => {
            let child = (*provider).hot_child;
            return if child.is_null() {
                std::ptr::null_mut()
            } else {
                exec_proc_node(child)
            };
        }
        HotProbeState::Disabled | HotProbeState::Fallback => {}
    }

    let slot = (*node).ss.ps.ps_ResultTupleSlot;
    if slot.is_null() {
        return std::ptr::null_mut();
    }

    let use_child = SCAN_STATES.with(|states| {
        states
            .borrow()
            .get(&(node as usize))
            .is_some_and(|scan| matches!(scan.mode, ScanEmitMode::HotChild { .. }))
    });

    if use_child {
        return exec_hot_child_slot(node, slot);
    }

    // Emitted rows are base-relation scan tuples. ExecScan applies the
    // ExprState compiled from plan.qual (including RLS/security quals), counts
    // rejected rows, and projects into ps_ResultTupleSlot.
    pg_sys::ExecScan(
        &raw mut (*node).ss,
        Some(next_scan_tuple),
        Some(recheck_scan_tuple),
    )
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn next_scan_tuple(
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
        Some(
            scan.store_next_row(slot)
                .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} stream failed: {error}")),
        )
    });

    if stored == Some(true) {
        slot
    } else {
        std::ptr::null_mut()
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn recheck_scan_tuple(
    _scan_state: *mut pg_sys::ScanState,
    _slot: *mut pg_sys::TupleTableSlot,
) -> bool {
    true
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn end_custom_scan(node: *mut pg_sys::CustomScanState) {
    if !node.is_null() {
        let provider = KoldMergeScanState::from_custom(node);
        if !matches!(
            (*provider).hot_probe,
            HotProbeState::Pending | HotProbeState::Hit
        ) {
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
        }
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
    let provider = KoldMergeScanState::from_custom(node);
    if matches!(
        (*provider).hot_probe,
        HotProbeState::Pending | HotProbeState::Hit
    ) {
        (*provider).hot_probe = HotProbeState::Pending;
    }
    SCAN_STATES.with(|states| {
        if let Some(scan) = states.borrow_mut().get_mut(&(node as usize)) {
            match &mut scan.mode {
                ScanEmitMode::Buffer { next, .. } => *next = 0,
                ScanEmitMode::Stream { stream, .. } => {
                    stream.reset();
                    scan.cold_profile.segments.clear();
                    scan.cold_profile.segments_opened = 0;
                    if let Some(memory) = scan.memory.as_mut() {
                        memory.reset();
                    }
                }
                ScanEmitMode::HotChild { prefetched } => *prefetched = None,
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

    let plan = (*node).ss.ps.plan;
    let strategy_tag = custom_private_strategy_tag(plan);
    profile::explain_text(es, "Strategy", strategy_explain_label(strategy_tag));
    if strategy_tag == STRATEGY_TAG_ORDERED_PROGRESSIVE {
        let leading = custom_private_leading_column_id(plan);
        if leading > 0 {
            profile::explain_text(
                es,
                "Output Order",
                &format!("column_id {leading} (with primary-key tie-break)"),
            );
        }
        profile::explain_text(
            es,
            "Cold Frontier Source",
            "koldstore.cold_segment_order_index",
        );
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
                    profile::explain_text(es, "Cold Storage", &format!("unavailable: {error}"));
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
        profile::explain_text(es, "Hot Plan", &hot_label);
        profile::explain_visual_pipeline(
            es,
            &cold_profile,
            &hot_label,
            emit_path,
            execution.as_ref(),
        );
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

pub(super) unsafe fn exec_proc_node(node: *mut pg_sys::PlanState) -> *mut pg_sys::TupleTableSlot {
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
    let prefetched = SCAN_STATES.with(|states| {
        let mut states = states.borrow_mut();
        let scan = states.get_mut(&(node as usize))?;
        match &mut scan.mode {
            ScanEmitMode::HotChild { prefetched } => prefetched.take(),
            _ => None,
        }
    });
    let child_slot = prefetched.unwrap_or_else(|| exec_proc_node(child));
    if tuple_slot_is_empty(child_slot) {
        return std::ptr::null_mut();
    }
    #[cfg(feature = "pg_test")]
    FAST_PATH_TUPLE_COPIES.fetch_add(1, Ordering::Relaxed);
    exec_copy_slot(result_slot, child_slot);
    result_slot
}

pub(super) unsafe fn tuple_slot_is_empty(slot: *mut pg_sys::TupleTableSlot) -> bool {
    slot.is_null()
        || ((*slot).tts_nvalid == 0 && ((*slot).tts_flags & pg_sys::TTS_FLAG_EMPTY as u16) != 0)
}

pub(super) unsafe fn hot_child_planstate(
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

/// Encodes executor-critical flags plus strategy identity as native nodes.
///
/// Layout: `[exact_pk, runtime_delegate_safe, strategy_tag, scope_key,
/// sort_order_id, leading_column_id, order_descending]`.
unsafe fn serialize_custom_private(
    exact_pk_lookup: bool,
    runtime_delegate_safe: bool,
    strategy_tag: i32,
    scope_key: &str,
    sort_order_id: i32,
    leading_column_id: i16,
    order_descending: bool,
) -> *mut pg_sys::List {
    let exact_pk = pg_sys::makeInteger(i32::from(exact_pk_lookup));
    let runtime_delegate = pg_sys::makeInteger(i32::from(runtime_delegate_safe));
    let strategy = pg_sys::makeInteger(strategy_tag);
    let scope = match std::ffi::CString::new(scope_key) {
        Ok(value) => value,
        Err(_) => std::ffi::CString::new("").expect("empty scope is valid"),
    };
    let scope_node = pg_sys::makeString(scope.as_ptr() as *mut c_char);
    let sort_order = pg_sys::makeInteger(sort_order_id);
    let leading = pg_sys::makeInteger(i32::from(leading_column_id));
    let descending = pg_sys::makeInteger(i32::from(order_descending));
    let mut private = pg_sys::lappend(std::ptr::null_mut(), exact_pk.cast::<c_void>());
    private = pg_sys::lappend(private, runtime_delegate.cast::<c_void>());
    private = pg_sys::lappend(private, strategy.cast::<c_void>());
    private = pg_sys::lappend(private, scope_node.cast::<c_void>());
    private = pg_sys::lappend(private, sort_order.cast::<c_void>());
    private = pg_sys::lappend(private, leading.cast::<c_void>());
    pg_sys::lappend(private, descending.cast::<c_void>())
}

/// Returns the path strategy tag from scan private data (default: general merge).
pub(super) unsafe fn custom_private_strategy_tag(plan: *mut pg_sys::Plan) -> i32 {
    if plan.is_null() {
        return STRATEGY_TAG_GENERAL_MERGE;
    }
    let custom_scan = plan.cast::<pg_sys::CustomScan>();
    let private = (*custom_scan).custom_private;
    if list_len(private) <= PRIVATE_STRATEGY_TAG_INDEX {
        return STRATEGY_TAG_GENERAL_MERGE;
    }
    let marker = list_nth_ptr(private, PRIVATE_STRATEGY_TAG_INDEX).cast::<pg_sys::Integer>();
    if marker.is_null() || (*marker).type_ != pg_sys::NodeTag::T_Integer {
        return STRATEGY_TAG_GENERAL_MERGE;
    }
    (*marker).ival
}

/// Returns the single-scope key from scan private data (default: `""`).
#[allow(dead_code)] // Used by Task 1.4+ frontier / EXPLAIN.
unsafe fn custom_private_scope_key(plan: *mut pg_sys::Plan) -> String {
    if plan.is_null() {
        return String::new();
    }
    let custom_scan = plan.cast::<pg_sys::CustomScan>();
    let private = (*custom_scan).custom_private;
    if list_len(private) <= PRIVATE_SCOPE_KEY_INDEX {
        return String::new();
    }
    let string_node = list_nth_ptr(private, PRIVATE_SCOPE_KEY_INDEX).cast::<pg_sys::String>();
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

unsafe fn custom_private_leading_column_id(plan: *mut pg_sys::Plan) -> i16 {
    if plan.is_null() {
        return 0;
    }
    let custom_scan = plan.cast::<pg_sys::CustomScan>();
    let private = (*custom_scan).custom_private;
    if list_len(private) <= PRIVATE_LEADING_COLUMN_ID_INDEX {
        return 0;
    }
    let marker = list_nth_ptr(private, PRIVATE_LEADING_COLUMN_ID_INDEX).cast::<pg_sys::Integer>();
    if marker.is_null() || (*marker).type_ != pg_sys::NodeTag::T_Integer {
        return 0;
    }
    (*marker).ival as i16
}

/// Returns true when ordered progressive private data advertises DESC order.
pub(super) unsafe fn custom_private_order_descending(plan: *mut pg_sys::Plan) -> bool {
    if plan.is_null() {
        return true;
    }
    let custom_scan = plan.cast::<pg_sys::CustomScan>();
    let private = (*custom_scan).custom_private;
    if list_len(private) <= PRIVATE_ORDER_DESCENDING_INDEX {
        return true;
    }
    let marker = list_nth_ptr(private, PRIVATE_ORDER_DESCENDING_INDEX).cast::<pg_sys::Integer>();
    if marker.is_null() || (*marker).type_ != pg_sys::NodeTag::T_Integer {
        return true;
    }
    (*marker).ival != 0
}

/// Reads the executor's exact-PK marker without allocation or JSON parsing.
unsafe fn custom_private_exact_pk(plan: *mut pg_sys::Plan) -> bool {
    if plan.is_null() {
        return false;
    }
    let custom_scan = plan.cast::<pg_sys::CustomScan>();
    let private = (*custom_scan).custom_private;
    if list_len(private) <= PRIVATE_EXACT_PK_INDEX {
        return false;
    }
    let marker = list_nth_ptr(private, PRIVATE_EXACT_PK_INDEX).cast::<pg_sys::Integer>();
    if marker.is_null() || (*marker).type_ != pg_sys::NodeTag::T_Integer {
        return false;
    }
    (*marker).ival != 0
}

/// Returns whether executor-time external parameters are stable across rescans.
unsafe fn custom_private_runtime_delegate_safe(plan: *mut pg_sys::Plan) -> bool {
    if plan.is_null() {
        return false;
    }
    let custom_scan = plan.cast::<pg_sys::CustomScan>();
    let private = (*custom_scan).custom_private;
    if list_len(private) <= PRIVATE_RUNTIME_DELEGATE_SAFE_INDEX {
        return false;
    }
    let marker =
        list_nth_ptr(private, PRIVATE_RUNTIME_DELEGATE_SAFE_INDEX).cast::<pg_sys::Integer>();
    !marker.is_null() && (*marker).type_ == pg_sys::NodeTag::T_Integer && (*marker).ival != 0
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
