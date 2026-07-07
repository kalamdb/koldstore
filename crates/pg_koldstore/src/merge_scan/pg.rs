//! PostgreSQL CustomScan wiring for managed hot/cold reads.
#![allow(unsafe_op_in_unsafe_fn)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::time::{Duration, Instant};

use koldstore_common::{quote_ident, QualifiedTableName};
use pgrx::pg_sys;

use crate::merge_scan::path::CUSTOM_PATH_NAME;

mod literals;
mod profile;
mod tuple;

use literals::{list_node_pointers, scalar_array_filter_sql, sql_literal, typed_literal_sql, unwrap_relabel};

use profile::{ColdReadProfile, SegmentReadProfile};
use tuple::{copy_spi_datum, store_materialized_row, MaterializedRow};

const CUSTOM_SCAN_NAME: &[u8] = b"KoldMergeScan\0";
const HOT_SEQ: i64 = i64::MAX;
const READER_PERMIT_RETRY_SLEEP: Duration = Duration::from_millis(10);
const READER_PERMIT_TIMEOUT: Duration = Duration::from_secs(30);

thread_local! {
    static SCAN_STATES: RefCell<HashMap<usize, ScanExecutionState>> = RefCell::new(HashMap::new());
    static DISABLE_HOOK: RefCell<bool> = const { RefCell::new(false) };
}

#[derive(Debug)]
struct ScanExecutionState {
    rows: Vec<MaterializedRow>,
    next: usize,
    cold_profile: ColdReadProfile,
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
    _estate: *mut pg_sys::EState,
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
    let (relation, catalog, snapshot) = with_hook_disabled(|| {
        let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
        let catalog = crate::sql::migrate_pg::migration_catalog(table_oid.to_u32())?;
        let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
        Ok::<_, String>((relation, catalog, snapshot))
    })
    .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} catalog lookup failed: {error}"));
    let (cold_profile, cold_rows_json) =
        match load_cold_rows_for_merge(table_oid, &snapshot, &catalog) {
            Ok(result) => result,
            Err(error) => pgrx::error!("{CUSTOM_PATH_NAME} cold read failed: {error}"),
        };
    let query = match build_materialize_query(
        &relation,
        &catalog,
        &snapshot,
        &cold_rows_json,
        targetlist,
        qual,
    ) {
        Ok(query) => query,
        Err(error) => pgrx::error!("{CUSTOM_PATH_NAME} planning failed: {error}"),
    };
    let rows = materialize_query(&query)
        .unwrap_or_else(|error| pgrx::error!("{CUSTOM_PATH_NAME} execution failed: {error}"));
    SCAN_STATES.with(|states| {
        states.borrow_mut().insert(
            node as usize,
            ScanExecutionState {
                rows,
                next: 0,
                cold_profile,
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

fn build_materialize_query(
    relation: &str,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    cold_rows_json: &str,
    targetlist: *mut pg_sys::List,
    qual: *mut pg_sys::List,
) -> Result<String, String> {
    let table = QualifiedTableName::parse(relation).map_err(|error| error.to_string())?;

    let hot_pk = snapshot
        .primary_key_columns
        .iter()
        .map(|column| {
            format!(
                "'{column}', hot.{quoted_column}",
                column = sql_literal(column),
                quoted_column = quote_ident(column),
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let requested_columns = unsafe { requested_target_columns(targetlist, &catalog.columns) };
    let selected_columns = if requested_columns.is_empty() {
        catalog.columns.iter().collect::<Vec<_>>()
    } else {
        requested_columns
    };
    let projection = selected_columns
        .iter()
        .map(|column| {
            format!(
                "(row_image ->> '{name}')::{type_name} AS {quoted_name}",
                name = sql_literal(&column.name),
                type_name = column.catalog_type_name(),
                quoted_name = quote_ident(&column.name),
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let residual_where = unsafe { residual_filter_sql(qual, &catalog.columns) }
        .map(|predicate| format!(" AND ({predicate})"))
        .unwrap_or_default();

    Ok(format!(
        r#"
WITH hot AS (
    SELECT
        to_jsonb(hot) AS row_image,
        jsonb_build_object({hot_pk}) AS pk_json,
        {hot_seq}::bigint AS seq,
        {hot_seq}::bigint AS commit_seq,
        false AS deleted,
        true AS from_hot
    FROM ONLY {table} AS hot
),
candidates AS (
    SELECT row_image, pk_json, seq, commit_seq, deleted, false AS from_hot
    FROM jsonb_to_recordset('{cold_rows_json}'::jsonb) AS cold(
        pk_json jsonb,
        row_image jsonb,
        seq bigint,
        commit_seq bigint,
        deleted boolean,
        schema_version integer
    )
    UNION ALL
    SELECT row_image, pk_json, seq, commit_seq, deleted, from_hot
    FROM hot
),
winners AS (
    SELECT DISTINCT ON (pk_json::text)
        row_image,
        deleted
    FROM candidates
    ORDER BY pk_json::text, seq DESC, commit_seq DESC, from_hot DESC
)
SELECT {projection}
FROM winners
WHERE NOT deleted{residual_where}
"#,
        hot_pk = hot_pk,
        hot_seq = HOT_SEQ,
        table = table.quoted(),
        cold_rows_json = sql_literal(cold_rows_json),
        projection = projection,
        residual_where = residual_where,
    ))
}

fn active_cold_segment_paths(table_oid: pg_sys::Oid) -> Result<Vec<String>, String> {
    let json = spi_query_one_string(
        r#"
SELECT COALESCE(jsonb_agg(object_path ORDER BY batch_number)::text, '[]')
FROM koldstore.cold_segments
WHERE table_oid = $1::oid
  AND scope_key = ''
  AND status = 'active'
"#,
        table_oid,
    )?
    .unwrap_or_else(|| "[]".to_string());
    serde_json::from_str(&json).map_err(|error| error.to_string())
}

fn optional_manifest_path(table_oid: pg_sys::Oid) -> Result<Option<String>, String> {
    spi_query_one_string(
        r#"
SELECT manifest_path
FROM koldstore.manifest
WHERE table_oid = $1::oid
  AND sync_state = 'in_sync'
ORDER BY generation DESC
LIMIT 1
"#,
        table_oid,
    )
}

fn spi_query_one_string(sql: &str, table_oid: pg_sys::Oid) -> Result<Option<String>, String> {
    pgrx::Spi::connect(|client| {
        let args = [pgrx::datum::DatumWithOid::from(table_oid)];
        let tuples = client
            .select(sql, Some(1), &args)
            .map_err(|error| error.to_string())?;
        if tuples.is_empty() {
            return Ok(None);
        }
        tuples
            .first()
            .get_one::<String>()
            .map_err(|error| error.to_string())
    })
    .map_err(|error| error.to_string())
}

fn load_cold_rows_for_merge(
    table_oid: pg_sys::Oid,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    catalog: &koldstore_migrate::ExistingTableCatalog,
) -> Result<(ColdReadProfile, String), String> {
    with_hook_disabled(|| {
        let manifest_started = Instant::now();
        let segment_paths = active_cold_segment_paths(table_oid)?;
        let manifest_path = optional_manifest_path(table_oid)?;

        if segment_paths.is_empty() {
            return Ok((
                ColdReadProfile {
                    manifest_path: manifest_path.unwrap_or_else(|| "(none)".to_string()),
                    manifest_read_ms: Some(profile::elapsed_ms(manifest_started)),
                    segments: vec![],
                },
                "[]".to_string(),
            ));
        }

        if crate::guc::cold_reads_mode() == crate::settings::ColdReadsMode::Off {
            return Err("cold reads are disabled by koldstore.cold_reads".to_string());
        }

        let manifest_path = manifest_path.ok_or_else(|| {
            "cold segments are present but no in-sync manifest was found".to_string()
        })?;
        let storage = crate::catalog::resolve::active_flush_storage_context(table_oid)?;
        let manifest_file = std::path::Path::new(&storage.base_path).join(&manifest_path);
        std::fs::read_to_string(&manifest_file)
            .map_err(|error| format!("read manifest file {}: {error}", manifest_file.display()))?;
        let manifest_read_ms = profile::elapsed_ms(manifest_started);
        let parquet_columns = catalog
            .columns
            .iter()
            .map(|column| {
                koldstore_parquet::PgColumn::new(column.name.clone(), column.pg_type, true)
            })
            .collect::<Vec<_>>();

        let (cold_rows_json, segments) = cold_rows_json_from_segments(
            &storage.base_path,
            &segment_paths,
            &parquet_columns,
            &snapshot.primary_key_columns,
        )?;

        Ok((
            ColdReadProfile {
                manifest_path,
                manifest_read_ms: Some(manifest_read_ms),
                segments,
            },
            cold_rows_json,
        ))
    })
}

fn planned_cold_read_profile(table_oid: pg_sys::Oid) -> Result<ColdReadProfile, String> {
    with_hook_disabled(|| {
        let manifest_started = Instant::now();
        let segment_paths = active_cold_segment_paths(table_oid)?;
        let manifest_path =
            optional_manifest_path(table_oid)?.unwrap_or_else(|| "(none)".to_string());
        Ok(ColdReadProfile {
            manifest_path,
            manifest_read_ms: Some(profile::elapsed_ms(manifest_started)),
            segments: segment_paths
                .into_iter()
                .map(|object_path| SegmentReadProfile {
                    object_path,
                    row_count: 0,
                    read_ms: None,
                })
                .collect(),
        })
    })
}

fn cold_rows_json_from_segments(
    base_path: &str,
    segment_paths: &[String],
    columns: &[koldstore_parquet::PgColumn],
    primary_key_columns: &[String],
) -> Result<(String, Vec<SegmentReadProfile>), String> {
    let mut rows = Vec::new();
    let mut segments = Vec::with_capacity(segment_paths.len());
    for object_path in segment_paths {
        let path = std::path::Path::new(base_path).join(object_path);
        let started = Instant::now();
        let _permit = acquire_parquet_reader_permit(crate::guc::max_open_parquet_readers())?;
        let segment_rows =
            koldstore_parquet::read_clean_cold_rows_from_path(&path, columns, primary_key_columns)?;
        segments.push(SegmentReadProfile {
            object_path: object_path.clone(),
            row_count: segment_rows.len(),
            read_ms: Some(profile::elapsed_ms(started)),
        });
        rows.extend(segment_rows);
    }
    let json = serde_json::to_string(&rows).map_err(|error| error.to_string())?;
    Ok((json, segments))
}

#[derive(Debug)]
struct ParquetReaderPermit {
    key: crate::merge_scan::reader_pool::ParquetReaderLockKey,
}

impl Drop for ParquetReaderPermit {
    fn drop(&mut self) {
        let _ = release_parquet_reader_slot(self.key);
    }
}

fn acquire_parquet_reader_permit(configured_limit: i32) -> Result<ParquetReaderPermit, String> {
    let max_open =
        crate::merge_scan::reader_pool::validated_max_open_parquet_readers(configured_limit);
    let started = Instant::now();
    loop {
        for slot in 0..max_open {
            let key = crate::merge_scan::reader_pool::parquet_reader_lock_key(slot);
            if try_lock_parquet_reader_slot(key)? {
                return Ok(ParquetReaderPermit { key });
            }
        }
        if started.elapsed() >= READER_PERMIT_TIMEOUT {
            return Err(format!(
                "timed out waiting for a Parquet reader slot after {} ms",
                profile::elapsed_ms(started)
            ));
        }
        std::thread::sleep(READER_PERMIT_RETRY_SLEEP);
    }
}

fn try_lock_parquet_reader_slot(
    key: crate::merge_scan::reader_pool::ParquetReaderLockKey,
) -> Result<bool, String> {
    let args = [
        pgrx::datum::DatumWithOid::from(key.0),
        pgrx::datum::DatumWithOid::from(key.1),
    ];
    pgrx::Spi::get_one_with_args::<bool>(
        "SELECT pg_try_advisory_lock($1::integer, $2::integer)",
        &args,
    )
    .map_err(|error| error.to_string())
    .map(|value| value.unwrap_or(false))
}

fn release_parquet_reader_slot(
    key: crate::merge_scan::reader_pool::ParquetReaderLockKey,
) -> Result<(), String> {
    let args = [
        pgrx::datum::DatumWithOid::from(key.0),
        pgrx::datum::DatumWithOid::from(key.1),
    ];
    pgrx::Spi::get_one_with_args::<bool>(
        "SELECT pg_advisory_unlock($1::integer, $2::integer)",
        &args,
    )
    .map_err(|error| error.to_string())
    .map(|_| ())
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

    if unsafe { (*es).analyze } {
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

unsafe fn requested_target_columns(
    targetlist: *mut pg_sys::List,
    columns: &[koldstore_migrate::order::CatalogColumn],
) -> Vec<&koldstore_migrate::order::CatalogColumn> {
    if targetlist.is_null() {
        return Vec::new();
    }
    let mut requested = Vec::new();
    let len = usize::try_from((*targetlist).length).unwrap_or(0);
    for index in 0..len {
        let entry = (*(*targetlist).elements.add(index))
            .ptr_value
            .cast::<pg_sys::TargetEntry>();
        if entry.is_null() || (*entry).resjunk {
            continue;
        }
        let expr = (*entry).expr;
        if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_Var {
            continue;
        }
        let var = expr.cast::<pg_sys::Var>();
        let attno = (*var).varattno;
        if attno <= 0 {
            continue;
        }
        if let Some(column) = columns.get(usize::try_from(attno - 1).unwrap_or(usize::MAX)) {
            requested.push(column);
        }
    }
    requested
}

unsafe fn residual_filter_sql(
    qual: *mut pg_sys::List,
    columns: &[koldstore_migrate::order::CatalogColumn],
) -> Option<String> {
    let predicates = list_node_pointers(qual)
        .into_iter()
        .filter_map(|node| residual_node_sql(node.cast::<pg_sys::Expr>(), columns))
        .collect::<Vec<_>>();
    if predicates.is_empty() {
        None
    } else {
        Some(predicates.join(" AND "))
    }
}

unsafe fn residual_node_sql(
    expr: *mut pg_sys::Expr,
    columns: &[koldstore_migrate::order::CatalogColumn],
) -> Option<String> {
    if expr.is_null() {
        return None;
    }
    match (*expr).type_ {
        pg_sys::NodeTag::T_OpExpr => op_expr_filter_sql(expr, columns),
        pg_sys::NodeTag::T_ScalarArrayOpExpr => scalar_array_filter_sql(expr, columns),
        pg_sys::NodeTag::T_BoolExpr => {
            let bool_expr = expr.cast::<pg_sys::BoolExpr>();
            if (*bool_expr).boolop != pg_sys::BoolExprType::AND_EXPR {
                return None;
            }
            let predicates = list_node_pointers((*bool_expr).args)
                .into_iter()
                .filter_map(|node| residual_node_sql(node.cast::<pg_sys::Expr>(), columns))
                .collect::<Vec<_>>();
            (!predicates.is_empty()).then(|| predicates.join(" AND "))
        }
        _ => None,
    }
}

unsafe fn op_expr_filter_sql(
    expr: *mut pg_sys::Expr,
    columns: &[koldstore_migrate::order::CatalogColumn],
) -> Option<String> {
    let op_expr = expr.cast::<pg_sys::OpExpr>();
    let opname = cstr_to_str(pg_sys::get_opname((*op_expr).opno))?;
    if opname != "=" {
        return None;
    }
    let args = list_node_pointers((*op_expr).args);
    if args.len() != 2 {
        return None;
    }
    let (column, literal) = equality_var_and_literal(args[0], args[1], columns)?;
    Some(format!(
        "(row_image ->> '{name}')::{type_name} = {literal}",
        name = sql_literal(&column.name),
        type_name = column.catalog_type_name(),
        literal = literal,
    ))
}

unsafe fn equality_var_and_literal(
    left: *mut std::ffi::c_void,
    right: *mut std::ffi::c_void,
    columns: &[koldstore_migrate::order::CatalogColumn],
) -> Option<(&koldstore_migrate::order::CatalogColumn, String)> {
    if let Some(column) = var_column(left.cast::<pg_sys::Expr>(), columns) {
        if let Some(literal) = typed_literal_sql(right.cast::<pg_sys::Expr>(), column) {
            return Some((column, literal));
        }
    }
    if let Some(column) = var_column(right.cast::<pg_sys::Expr>(), columns) {
        if let Some(literal) = typed_literal_sql(left.cast::<pg_sys::Expr>(), column) {
            return Some((column, literal));
        }
    }
    None
}

unsafe fn var_column(
    expr: *mut pg_sys::Expr,
    columns: &[koldstore_migrate::order::CatalogColumn],
) -> Option<&koldstore_migrate::order::CatalogColumn> {
    let expr = unwrap_relabel(expr);
    if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_Var {
        return None;
    }
    let var = expr.cast::<pg_sys::Var>();
    let attno = (*var).varattno;
    if attno <= 0 {
        return None;
    }
    columns.get(usize::try_from(attno - 1).ok()?)
}

unsafe fn materialize_query(query: &str) -> Result<Vec<MaterializedRow>, String> {
    let query = CString::new(query).map_err(|error| error.to_string())?;
    with_hook_disabled(|| {
        let connect = pg_sys::SPI_connect();
        if connect < 0 {
            return Err(format!("SPI_connect failed with code {connect}"));
        }
        let execute = pg_sys::SPI_execute(query.as_ptr(), true, 0);
        if execute < 0 {
            let _ = pg_sys::SPI_finish();
            return Err(format!("SPI_execute failed with code {execute}"));
        }
        let processed =
            usize::try_from(pg_sys::SPI_processed).map_err(|error| error.to_string())?;
        let tuptable = pg_sys::SPI_tuptable;
        let mut rows = Vec::with_capacity(processed);
        if !tuptable.is_null() {
            let tupdesc = (*tuptable).tupdesc;
            let natts = usize::try_from((*tupdesc).natts).map_err(|error| error.to_string())?;
            for index in 0..processed {
                let tuple = *(*tuptable).vals.add(index);
                let mut values = Vec::with_capacity(natts);
                let mut is_null = Vec::with_capacity(natts);
                for attr_index in 0..natts {
                    let mut null = false;
                    let datum = pg_sys::SPI_getbinval(
                        tuple,
                        tupdesc,
                        i32::try_from(attr_index + 1).map_err(|error| error.to_string())?,
                        &mut null,
                    );
                    if null {
                        values.push(pg_sys::Datum::null());
                        is_null.push(true);
                    } else {
                        values.push(copy_spi_datum(tupdesc, attr_index, datum));
                        is_null.push(false);
                    }
                }
                rows.push(MaterializedRow { values, is_null });
            }
        }
        let finish = pg_sys::SPI_finish();
        if finish < 0 {
            return Err(format!("SPI_finish failed with code {finish}"));
        }
        Ok(rows)
    })
}

fn with_hook_disabled<T>(f: impl FnOnce() -> T) -> T {
    DISABLE_HOOK.with(|disabled| {
        let was_disabled = *disabled.borrow();
        *disabled.borrow_mut() = true;
        let result = f();
        *disabled.borrow_mut() = was_disabled;
        result
    })
}

fn cstr_to_str(value: *const c_char) -> Option<&'static str> {
    if value.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(value).to_str().ok() }
}
