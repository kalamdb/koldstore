//! PostgreSQL CustomScan wiring for managed hot/cold reads.
#![allow(unsafe_op_in_unsafe_fn)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::os::raw::{c_char, c_int};

use koldstore_merge::scan::{execute_merge_scan_with_filters, FilterPlan, CUSTOM_PATH_NAME};
use pgrx::pg_sys;

mod cold;
mod emit;
mod hot;
mod literals;
mod profile;
mod qual;
mod tuple;

use cold::{load_cold_rows_for_merge, planned_cold_read_profile};
use emit::materialize_row_from_image;
use hot::{load_hot_rows_for_merge, load_hot_rows_native};
use profile::ColdReadProfile;
use qual::{requested_target_columns, residual_filters};
use tuple::{store_materialized_row, MaterializedRow, ScanMemory};

const CUSTOM_SCAN_NAME: &[u8] = b"KoldMergeScan\0";

thread_local! {
    static SCAN_STATES: RefCell<HashMap<usize, ScanExecutionState>> = RefCell::new(HashMap::new());
    static DISABLE_HOOK: RefCell<bool> = const { RefCell::new(false) };
}

#[derive(Debug)]
struct ScanExecutionState {
    rows: Vec<MaterializedRow>,
    next: usize,
    cold_profile: ColdReadProfile,
    /// Owns all Datums in `rows`; deleted on EndCustomScan.
    _memory: ScanMemory,
}

impl ScanExecutionState {
    unsafe fn store_next_row(&mut self, slot: *mut pg_sys::TupleTableSlot) -> bool {
        let Some(row) = self.rows.get(self.next) else {
            return false;
        };
        self.next += 1;
        store_materialized_row(slot, row);
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
    ShutdownCustomScan: Some(end_custom_scan),
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
    if !crate::guc::enable_merge_scan() {
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

    let table_oid = (*rte).relid;
    let managed = with_hook_disabled(|| {
        crate::catalog::cache::managed_table_snapshot(table_oid)
            .ok()
            .flatten()
            .map(|snapshot| snapshot.active)
            .unwrap_or(false)
    });
    if !managed {
        return;
    }

    let custom_path =
        pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;
    if custom_path.is_null() {
        return;
    }

    (*custom_path).path.type_ = pg_sys::NodeTag::T_CustomPath;
    (*custom_path).path.pathtype = pg_sys::NodeTag::T_CustomScan;
    (*custom_path).path.parent = rel;
    (*custom_path).path.pathtarget = (*rel).reltarget;
    (*custom_path).path.rows = (*rel).rows;
    (*custom_path).path.startup_cost = 0.0;
    (*custom_path).path.total_cost = 0.0;
    (*custom_path).path.parallel_safe = false;
    (*custom_path).methods = &raw const PATH_METHODS;

    pg_sys::add_path(rel, &mut (*custom_path).path);
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn plan_custom_path(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    tlist: *mut pg_sys::List,
    clauses: *mut pg_sys::List,
    _custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
    let scan =
        pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScan>()) as *mut pg_sys::CustomScan;
    if scan.is_null() {
        return std::ptr::null_mut();
    }

    (*scan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
    (*scan).scan.plan.startup_cost = (*best_path).path.startup_cost;
    (*scan).scan.plan.total_cost = (*best_path).path.total_cost;
    (*scan).scan.plan.plan_rows = (*best_path).path.rows;
    (*scan).scan.plan.targetlist = tlist;
    let actual_clauses = pg_sys::extract_actual_clauses(clauses, false);
    (*scan).scan.plan.qual = actual_clauses;
    (*scan).scan.scanrelid = if rel.is_null() { 0 } else { (*rel).relid };
    (*scan).flags = (*best_path).flags;
    (*scan).custom_plans = std::ptr::null_mut();
    (*scan).custom_exprs = actual_clauses;
    (*scan).custom_private = std::ptr::null_mut();
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
    _eflags: c_int,
) {
    if node.is_null() || (*node).ss.ss_currentRelation.is_null() {
        return;
    }
    let table_oid = (*(*node).ss.ss_currentRelation).rd_id;
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
    // Bound query parameters (`$1`, …) for prune + hot equality pushdown.
    let params = if estate.is_null() {
        std::ptr::null_mut()
    } else {
        (*estate).es_param_list_info
    };

    let (relation, catalog, snapshot) = with_hook_disabled(|| {
        let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
        let catalog = crate::catalog::cache::cached_migration_catalog(table_oid)?;
        let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
        Ok::<_, String>((relation, catalog, snapshot))
    })
    .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} catalog lookup failed: {error}"));

    let requested = unsafe { requested_target_columns(targetlist, &catalog.columns) };
    let residual = unsafe { residual_filters(qual, &catalog.columns, params) };
    let emit_columns: Vec<&koldstore_migrate::order::CatalogColumn> = if requested.is_empty() {
        catalog.columns.iter().collect()
    } else {
        requested
    };
    let mut image_columns = emit_columns.clone();
    // Residual filters need their columns present in the JSON image on the merge path.
    for column in residual
        .equality
        .iter()
        .map(|(column, _)| column.as_str())
        .chain(
            residual
                .membership
                .iter()
                .map(|(column, _)| column.as_str()),
        )
    {
        if let Some(catalog_column) = catalog.columns.iter().find(|c| c.name == column) {
            if !image_columns.iter().any(|c| c.name == catalog_column.name) {
                image_columns.push(catalog_column);
            }
        }
    }

    let (cold_profile, cold_rows) = match load_cold_rows_for_merge(
        table_oid,
        &snapshot,
        catalog.as_ref(),
        qual,
        &image_columns,
        params,
    ) {
        Ok(result) => result,
        Err(error) => pgrx::error!("{CUSTOM_PATH_NAME} cold read failed: {error}"),
    };

    let mut memory = ScanMemory::create("KoldMergeScan");

    // Hot-only fast path: every cold segment was pruned by PK/indexed min-max.
    // Load projected columns as native Datums and skip JSON encode/decode.
    let rows = if cold_rows.is_empty()
        && cold_profile.segments.is_empty()
        && residual.membership.is_empty()
    {
        match load_hot_rows_native(
            &relation,
            &residual.hot_equality,
            &emit_columns,
            &mut memory,
        ) {
            Ok(rows) => rows,
            Err(error) => pgrx::error!("{CUSTOM_PATH_NAME} hot-only read failed: {error}"),
        }
    } else {
        let mut filters = FilterPlan::new();
        for (column, expected) in &residual.equality {
            filters = filters.with_required_json_eq(column.clone(), expected.clone());
        }
        for (column, values) in residual.membership {
            filters = filters.with_required_json_in(column, values);
        }

        let hot_rows = match load_hot_rows_for_merge(
            &relation,
            &snapshot,
            &residual.hot_equality,
            &image_columns,
        ) {
            Ok(rows) => rows,
            Err(error) => pgrx::error!("{CUSTOM_PATH_NAME} hot read failed: {error}"),
        };

        let merged = match execute_merge_scan_with_filters(hot_rows, cold_rows, filters) {
            Ok(result) => result,
            Err(error) => pgrx::error!("{CUSTOM_PATH_NAME} merge failed: {error}"),
        };

        unsafe {
            memory.switch(|| {
                merged
                    .rows
                    .iter()
                    .map(|row| materialize_row_from_image(&row.row_image, &emit_columns))
                    .collect::<Result<Vec<_>, _>>()
            })
        }
        .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} emit failed: {error}"))
    };

    SCAN_STATES.with(|states| {
        states.borrow_mut().insert(
            node as usize,
            ScanExecutionState {
                rows,
                next: 0,
                cold_profile,
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

    let stored = SCAN_STATES.with(|states| {
        let mut states = states.borrow_mut();
        let scan = states.get_mut(&(node as usize))?;
        Some(scan.store_next_row(slot))
    });

    if stored == Some(true) {
        slot
    } else {
        std::ptr::null_mut()
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn end_custom_scan(node: *mut pg_sys::CustomScanState) {
    if !node.is_null() {
        SCAN_STATES.with(|states| {
            if let Some(scan) = states.borrow_mut().remove(&(node as usize)) {
                if should_keep_explain_profile(node) {
                    profile::remember_explain_profile(node as usize, scan.cold_profile);
                }
            }
        });
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn rescan_custom_scan(node: *mut pg_sys::CustomScanState) {
    if !node.is_null() {
        SCAN_STATES.with(|states| {
            if let Some(scan) = states.borrow_mut().get_mut(&(node as usize)) {
                scan.next = 0;
            }
        });
    }
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
        states
            .borrow()
            .get(&(node as usize))
            .map(|scan| scan.cold_profile.clone())
    });
    let profile = profile.or_else(|| profile::saved_explain_profile(node as usize));
    let profile = match profile {
        Some(profile) => profile,
        None => match resolve_table_oid(node).and_then(planned_cold_read_profile) {
            Ok(profile) => profile,
            Err(error) => {
                profile::explain_property(es, "Cold storage", &format!("unavailable: {error}"));
                return;
            }
        },
    };

    profile::explain_cold_read_profile(es, &profile);

    if (*es).analyze {
        profile::forget_explain_profile(node as usize);
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
