//! PostgreSQL CustomScan wiring for managed hot/cold reads.
#![allow(unsafe_op_in_unsafe_fn)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};

use koldstore_common::{quote_ident, QualifiedTableName};
use pgrx::pg_sys;

const CUSTOM_SCAN_NAME: &[u8] = b"KoldstoreMergeScan\0";
const HOT_SEQ: i64 = i64::MAX;

thread_local! {
    static SCAN_STATES: RefCell<HashMap<usize, MaterializedScan>> = RefCell::new(HashMap::new());
    static DISABLE_HOOK: RefCell<bool> = const { RefCell::new(false) };
}

#[derive(Debug)]
struct MaterializedScan {
    rows: Vec<MaterializedRow>,
    next: usize,
}

#[derive(Debug, Clone)]
struct MaterializedRow {
    values: Vec<pg_sys::Datum>,
    is_null: Vec<bool>,
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
    ExplainCustomScan: None,
};

/// Registers KoldstoreMergeScan with PostgreSQL and installs the planner hook.
pub fn register_custom_scan_hooks() {
    unsafe {
        pg_sys::RegisterCustomScanMethods(&raw const SCAN_METHODS);
        PREVIOUS_SET_REL_PATHLIST_HOOK = pg_sys::set_rel_pathlist_hook;
        pg_sys::set_rel_pathlist_hook = Some(set_rel_pathlist);
    }
}

/// Runs extension-internal SQL without injecting KoldstoreMergeScan paths.
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

    let table_oid = pg_sys::Oid::from((*rte).relid);
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
    (*state).slotOps = &raw const pg_sys::TTSOpsVirtual;
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
    let table_oid = pg_sys::Oid::from((*(*node).ss.ss_currentRelation).rd_id);
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
    let query = match build_materialize_query(table_oid, targetlist, qual) {
        Ok(query) => query,
        Err(error) => pgrx::error!("KoldstoreMergeScan planning failed: {error}"),
    };
    let rows = materialize_query(&query)
        .unwrap_or_else(|error| pgrx::error!("KoldstoreMergeScan execution failed: {error}"));
    SCAN_STATES.with(|states| {
        states
            .borrow_mut()
            .insert(node as usize, MaterializedScan { rows, next: 0 });
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

    loop {
        let row = SCAN_STATES.with(|states| {
            let mut states = states.borrow_mut();
            let scan = states.get_mut(&(node as usize))?;
            let row = scan.rows.get(scan.next).cloned();
            scan.next += usize::from(row.is_some());
            row
        });

        match row {
            Some(row) => {
                if !(*slot).tts_ops.is_null() {
                    if let Some(clear) = (*(*slot).tts_ops).clear {
                        clear(slot);
                    }
                }
                let slot_natts = if (*slot).tts_tupleDescriptor.is_null() {
                    row.values.len()
                } else {
                    usize::try_from((*(*slot).tts_tupleDescriptor).natts)
                        .unwrap_or(row.values.len())
                };
                let copied = row.values.len().min(slot_natts);
                for index in 0..copied {
                    let value = row.values[index];
                    *(*slot).tts_values.add(index) = value;
                    *(*slot).tts_isnull.add(index) = row.is_null[index];
                }
                (*slot).tts_nvalid = copied as pg_sys::AttrNumber;
                pg_sys::ExecStoreVirtualTuple(slot);

                return slot;
            }
            None => return std::ptr::null_mut(),
        }
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn end_custom_scan(node: *mut pg_sys::CustomScanState) {
    if !node.is_null() {
        SCAN_STATES.with(|states| {
            states.borrow_mut().remove(&(node as usize));
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
    table_oid: pg_sys::Oid,
    targetlist: *mut pg_sys::List,
    qual: *mut pg_sys::List,
) -> Result<String, String> {
    let (relation, catalog, snapshot, storage, segment_paths) = with_hook_disabled(|| {
        let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
        let catalog = crate::sql::migrate_pg::migration_catalog(table_oid.to_u32())?;
        let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
        let storage = crate::catalog::resolve::active_flush_storage_context(table_oid)?;
        let segment_paths = active_cold_segment_paths(table_oid)?;
        Ok::<_, String>((relation, catalog, snapshot, storage, segment_paths))
    })?;
    let table = QualifiedTableName::parse(&relation).map_err(|error| error.to_string())?;
    let parquet_columns = catalog
        .columns
        .iter()
        .map(|column| {
            koldstore_parquet::PgColumn::from_catalog(&column.name, &column.type_name, true)
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let cold_rows_json = cold_rows_json_from_segments(
        &storage.base_path,
        &segment_paths,
        &parquet_columns,
        &snapshot.primary_key_columns,
    )?;

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
                type_name = column.type_name,
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
        cold_rows_json = sql_literal(&cold_rows_json),
        projection = projection,
        residual_where = residual_where,
    ))
}

fn active_cold_segment_paths(table_oid: pg_sys::Oid) -> Result<Vec<String>, String> {
    let json = pgrx::Spi::get_one_with_args::<String>(
        r#"
SELECT COALESCE(jsonb_agg(object_path ORDER BY batch_number)::text, '[]')
FROM koldstore.cold_segments
WHERE table_oid = $1::oid
  AND scope_key = ''
  AND status = 'active'
"#,
        &[pgrx::datum::DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "cold segment lookup returned no rows".to_string())?;
    serde_json::from_str(&json).map_err(|error| error.to_string())
}

fn cold_rows_json_from_segments(
    base_path: &str,
    segment_paths: &[String],
    columns: &[koldstore_parquet::PgColumn],
    primary_key_columns: &[String],
) -> Result<String, String> {
    let mut rows = Vec::new();
    for object_path in segment_paths {
        let path = std::path::Path::new(base_path).join(object_path);
        rows.extend(koldstore_parquet::read_clean_cold_rows_from_path(
            &path,
            columns,
            primary_key_columns,
        )?);
    }
    serde_json::to_string(&rows).map_err(|error| error.to_string())
}

unsafe fn requested_target_columns<'a>(
    targetlist: *mut pg_sys::List,
    columns: &'a [koldstore_migrate::order::CatalogColumn],
) -> Vec<&'a koldstore_migrate::order::CatalogColumn> {
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

unsafe fn scalar_array_filter_sql(
    expr: *mut pg_sys::Expr,
    columns: &[koldstore_migrate::order::CatalogColumn],
) -> Option<String> {
    let scalar = expr.cast::<pg_sys::ScalarArrayOpExpr>();
    if !(*scalar).useOr {
        return None;
    }
    let args = list_node_pointers((*scalar).args);
    if args.len() != 2 {
        return None;
    }
    let var = args[0].cast::<pg_sys::Expr>();
    if var.is_null() || (*var).type_ != pg_sys::NodeTag::T_Var {
        return None;
    }
    let var = var.cast::<pg_sys::Var>();
    let attno = (*var).varattno;
    if attno <= 0 {
        return None;
    }
    let column = columns.get(usize::try_from(attno - 1).ok()?)?;
    let values = array_literal_values(args[1].cast::<pg_sys::Expr>())?;
    if values.is_empty() {
        return None;
    }
    Some(format!(
        "(row_image ->> '{name}')::{type_name} IN ({values})",
        name = sql_literal(&column.name),
        type_name = column.type_name,
        values = values.join(", ")
    ))
}

unsafe fn array_literal_values(expr: *mut pg_sys::Expr) -> Option<Vec<String>> {
    if expr.is_null() {
        return None;
    }
    match (*expr).type_ {
        pg_sys::NodeTag::T_ArrayExpr => {
            let array = expr.cast::<pg_sys::ArrayExpr>();
            Some(
                list_node_pointers((*array).elements)
                    .into_iter()
                    .filter_map(|node| const_literal_sql(node.cast::<pg_sys::Expr>()))
                    .collect::<Vec<_>>(),
            )
        }
        pg_sys::NodeTag::T_Const => const_array_literal_values(expr)
            .or_else(|| const_literal_sql(expr).map(|value| vec![value])),
        _ => None,
    }
}

unsafe fn const_array_literal_values(expr: *mut pg_sys::Expr) -> Option<Vec<String>> {
    if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_Const {
        return None;
    }
    let konst = expr.cast::<pg_sys::Const>();
    if (*konst).constisnull {
        return None;
    }
    let (element_type, elem_len, elem_byval, elem_align) = match (*konst).consttype.to_u32() {
        1005 => (21, 2, true, b's' as c_char),
        1007 => (23, 4, true, b'i' as c_char),
        1016 => (20, 8, true, b'd' as c_char),
        _ => return None,
    };
    let array = (*konst).constvalue.cast_mut_ptr::<pg_sys::ArrayType>();
    if array.is_null() {
        return None;
    }
    let mut values: *mut pg_sys::Datum = std::ptr::null_mut();
    let mut nulls: *mut bool = std::ptr::null_mut();
    let mut count: c_int = 0;
    pg_sys::deconstruct_array(
        array,
        pg_sys::Oid::from(element_type),
        elem_len,
        elem_byval,
        elem_align,
        &mut values,
        &mut nulls,
        &mut count,
    );
    let count = usize::try_from(count).ok()?;
    let mut result = Vec::with_capacity(count);
    for index in 0..count {
        if !nulls.is_null() && *nulls.add(index) {
            continue;
        }
        let value = *values.add(index);
        let sql = match element_type {
            20 => (value.value() as i64).to_string(),
            21 => (value.value() as i16).to_string(),
            23 => (value.value() as i32).to_string(),
            _ => return None,
        };
        result.push(sql);
    }
    Some(result)
}

unsafe fn const_literal_sql(expr: *mut pg_sys::Expr) -> Option<String> {
    if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_Const {
        return None;
    }
    let konst = expr.cast::<pg_sys::Const>();
    if (*konst).constisnull {
        return None;
    }
    match (*konst).consttype.to_u32() {
        20 => Some(((*konst).constvalue.value() as i64).to_string()),
        21 => Some(((*konst).constvalue.value() as i16).to_string()),
        23 => Some(((*konst).constvalue.value() as i32).to_string()),
        _ => None,
    }
}

unsafe fn list_node_pointers(list: *mut pg_sys::List) -> Vec<*mut std::ffi::c_void> {
    if list.is_null() {
        return Vec::new();
    }
    let len = usize::try_from((*list).length).unwrap_or(0);
    (0..len)
        .map(|index| (*(*list).elements.add(index)).ptr_value)
        .collect::<Vec<_>>()
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
                        let attr = (*tupdesc).attrs.as_ptr().add(attr_index);
                        values.push(pg_sys::SPI_datumTransfer(
                            datum,
                            (*attr).attbyval,
                            i32::from((*attr).attlen),
                        ));
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

fn sql_literal(value: &str) -> String {
    value.replace('\'', "''")
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

#[allow(dead_code)]
fn cstr_to_str(value: *const c_char) -> Option<&'static str> {
    if value.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(value).to_str().ok() }
}
