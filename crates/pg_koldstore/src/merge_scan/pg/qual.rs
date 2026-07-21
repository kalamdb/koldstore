//! Planner qual walking for safe prune predicates and post-merge filters.

use std::ffi::CStr;
use std::os::raw::c_char;

use koldstore_merge::scan::plan::SegmentPrunePredicate;
use pgrx::pg_sys;

use super::spi::PgAllocatedCString;

use super::literals::{list_node_pointers, literal_json_value, typed_literal_sql, unwrap_relabel};

/// One base-relation attribute required by output projection or executor quals.
#[derive(Debug, Clone, Copy)]
pub(super) struct ScanProjectionColumn<'a> {
    pub(super) catalog: &'a koldstore_migrate::order::CatalogColumn,
    /// Zero-based position in the base relation's scan tuple.
    pub(super) slot_index: usize,
}

/// Minimal base-relation projection used to build tuples for PostgreSQL `ExecScan`.
#[derive(Debug)]
pub(super) struct ScanProjection<'a> {
    pub(super) columns: Vec<ScanProjectionColumn<'a>>,
    pub(super) tuple_width: usize,
}

impl<'a> ScanProjection<'a> {
    pub(super) fn catalog_columns(&self) -> Vec<&'a koldstore_migrate::order::CatalogColumn> {
        self.columns.iter().map(|column| column.catalog).collect()
    }
}

/// Canonical equality predicates safe for primary-key source pruning.
#[derive(Debug, Default)]
pub(super) struct ResidualFilters {
    pub(super) hot_equality: Vec<super::hot::HotEqualityFilter>,
}

#[derive(Debug, Clone, Copy)]
struct QualCatalog<'a> {
    table_oid: pg_sys::Oid,
    scanrelid: pg_sys::Index,
    columns: &'a [koldstore_migrate::order::CatalogColumn],
}

/// Collects every base-table attribute referenced by the target list or quals.
///
/// PostgreSQL's generic Var walker covers arbitrary RLS expressions and planned
/// subqueries, so cold enforcement does not depend on KoldStore understanding a
/// policy's expression shape.
pub(super) unsafe fn required_scan_projection(
    table_oid: pg_sys::Oid,
    scanrelid: pg_sys::Index,
    targetlist: *mut pg_sys::List,
    qual: *mut pg_sys::List,
    columns: &[koldstore_migrate::order::CatalogColumn],
    tuple_width: usize,
) -> Result<ScanProjection<'_>, String> {
    let mut attrs: *mut pg_sys::Bitmapset = std::ptr::null_mut();
    unsafe {
        pg_sys::pull_varattnos(targetlist.cast::<pg_sys::Node>(), scanrelid, &mut attrs);
        pg_sys::pull_varattnos(qual.cast::<pg_sys::Node>(), scanrelid, &mut attrs);
    }

    let whole_row_member = -pg_sys::FirstLowInvalidHeapAttributeNumber;
    let whole_row = unsafe { pg_sys::bms_is_member(whole_row_member, attrs) };
    let mut system_column = None;
    for attnum in (pg_sys::FirstLowInvalidHeapAttributeNumber + 1)..0 {
        if unsafe {
            pg_sys::bms_is_member(attnum - pg_sys::FirstLowInvalidHeapAttributeNumber, attrs)
        } {
            system_column = Some(attnum);
            break;
        }
    }

    let mut required = Vec::with_capacity(tuple_width);
    for attnum in 1..=tuple_width {
        required.push(
            whole_row
                || unsafe {
                    pg_sys::bms_is_member(
                        i32::try_from(attnum).map_err(|error| error.to_string())?
                            - pg_sys::FirstLowInvalidHeapAttributeNumber,
                        attrs,
                    )
                },
        );
    }
    unsafe {
        pg_sys::bms_free(attrs);
    }

    if let Some(attnum) = system_column {
        return Err(format!(
            "KoldMergeScan cannot materialize PostgreSQL system attribute {attnum}"
        ));
    }

    let mut projection = Vec::new();
    for (slot_index, is_required) in required.into_iter().enumerate() {
        if !is_required {
            continue;
        }
        let attnum =
            pg_sys::AttrNumber::try_from(slot_index + 1).map_err(|error| error.to_string())?;
        let name = unsafe { pg_sys::get_attname(table_oid, attnum, true) };
        if name.is_null() {
            return Err(format!(
                "required base-relation attribute {} does not exist",
                slot_index + 1
            ));
        }
        let name = unsafe { PgAllocatedCString::from_raw(name) };
        let name_text = unsafe { name.as_c_str() }
            .to_str()
            .map_err(|error| error.to_string())?
            .to_string();
        let Some(catalog) = columns.iter().find(|column| column.name == name_text) else {
            if whole_row {
                // PostgreSQL represents a dropped attribute in a whole-row
                // value as NULL; it is intentionally absent from our catalog.
                continue;
            }
            return Err(format!(
                "required base-relation attribute {} (`{name_text}`) is not present in the managed schema",
                slot_index + 1,
            ));
        };
        projection.push(ScanProjectionColumn {
            catalog,
            slot_index,
        });
    }

    Ok(ScanProjection {
        columns: projection,
        tuple_width,
    })
}

pub(super) unsafe fn residual_filters(
    table_oid: pg_sys::Oid,
    scanrelid: pg_sys::Index,
    qual: *mut pg_sys::List,
    columns: &[koldstore_migrate::order::CatalogColumn],
    params: pg_sys::ParamListInfo,
) -> ResidualFilters {
    let catalog = QualCatalog {
        table_oid,
        scanrelid,
        columns,
    };
    let mut filters = ResidualFilters::default();
    for node in list_node_pointers(qual) {
        collect_residual_filters(node.cast::<pg_sys::Expr>(), catalog, params, &mut filters);
    }
    filters
}

pub(super) unsafe fn segment_prune_predicates(
    table_oid: pg_sys::Oid,
    scanrelid: pg_sys::Index,
    qual: *mut pg_sys::List,
    columns: &[koldstore_migrate::order::CatalogColumn],
    params: pg_sys::ParamListInfo,
) -> Vec<SegmentPrunePredicate> {
    let catalog = QualCatalog {
        table_oid,
        scanrelid,
        columns,
    };
    list_node_pointers(qual)
        .into_iter()
        .flat_map(|node| {
            segment_prune_node_predicates(node.cast::<pg_sys::Expr>(), catalog, params)
        })
        .collect()
}

unsafe fn segment_prune_node_predicates(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Vec<SegmentPrunePredicate> {
    if expr.is_null() {
        return Vec::new();
    }
    match (*expr).type_ {
        pg_sys::NodeTag::T_OpExpr => segment_prune_op_expr(expr, catalog, params)
            .into_iter()
            .collect(),
        pg_sys::NodeTag::T_BoolExpr => {
            let bool_expr = expr.cast::<pg_sys::BoolExpr>();
            if (*bool_expr).boolop != pg_sys::BoolExprType::AND_EXPR {
                return Vec::new();
            }
            list_node_pointers((*bool_expr).args)
                .into_iter()
                .flat_map(|node| {
                    segment_prune_node_predicates(node.cast::<pg_sys::Expr>(), catalog, params)
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

unsafe fn segment_prune_op_expr(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Option<SegmentPrunePredicate> {
    let op_expr = expr.cast::<pg_sys::OpExpr>();
    let opname = cstr_to_str(pg_sys::get_opname((*op_expr).opno))?;
    if !matches!(opname, "=" | "<" | "<=" | ">" | ">=") || !operator_is_pg_catalog((*op_expr).opno)
    {
        return None;
    }
    let args = list_node_pointers((*op_expr).args);
    if args.len() != 2 {
        return None;
    }

    if let Some((column, literal)) = var_and_json_literal(args[0], args[1], catalog, params) {
        return prune_predicate_from_op(column, literal, opname, false);
    }
    if let Some((column, literal)) = var_and_json_literal(args[1], args[0], catalog, params) {
        return prune_predicate_from_op(column, literal, opname, true);
    }
    None
}

fn prune_predicate_from_op(
    column: &koldstore_migrate::order::CatalogColumn,
    literal: serde_json::Value,
    opname: &str,
    reversed: bool,
) -> Option<SegmentPrunePredicate> {
    match (opname, reversed) {
        ("=", _) => Some(SegmentPrunePredicate::equality(&column.name, literal)),
        (">" | ">=", false) | ("<" | "<=", true) => {
            Some(SegmentPrunePredicate::lower_bound(&column.name, literal))
        }
        ("<" | "<=", false) | (">" | ">=", true) => {
            Some(SegmentPrunePredicate::upper_bound(&column.name, literal))
        }
        _ => None,
    }
}

unsafe fn var_and_json_literal(
    column_expr: *mut std::ffi::c_void,
    literal_expr: *mut std::ffi::c_void,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Option<(&koldstore_migrate::order::CatalogColumn, serde_json::Value)> {
    let column = var_column(column_expr.cast::<pg_sys::Expr>(), catalog)?;
    let literal = literal_json_value(literal_expr.cast::<pg_sys::Expr>(), column, params)?;
    Some((column, literal))
}

unsafe fn collect_residual_filters(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
    filters: &mut ResidualFilters,
) {
    if expr.is_null() {
        return;
    }
    match (*expr).type_ {
        pg_sys::NodeTag::T_OpExpr => {
            if let Some((column, sql_literal)) = op_expr_equality_filter(expr, catalog, params) {
                filters.hot_equality.push(super::hot::HotEqualityFilter {
                    column,
                    sql_literal,
                });
            }
        }
        pg_sys::NodeTag::T_BoolExpr => {
            let bool_expr = expr.cast::<pg_sys::BoolExpr>();
            if (*bool_expr).boolop != pg_sys::BoolExprType::AND_EXPR {
                return;
            }
            for arg in list_node_pointers((*bool_expr).args) {
                collect_residual_filters(arg.cast::<pg_sys::Expr>(), catalog, params, filters);
            }
        }
        _ => {}
    }
}

unsafe fn op_expr_equality_filter(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Option<(String, String)> {
    let op_expr = expr.cast::<pg_sys::OpExpr>();
    let opname = cstr_to_str(pg_sys::get_opname((*op_expr).opno))?;
    if opname != "=" || !operator_is_pg_catalog((*op_expr).opno) {
        return None;
    }
    let args = list_node_pointers((*op_expr).args);
    if args.len() != 2 {
        return None;
    }
    let (column, literal) = equality_var_and_literal(args[0], args[1], catalog, params)?;
    Some((column.name.clone(), literal))
}

unsafe fn equality_var_and_literal(
    left: *mut std::ffi::c_void,
    right: *mut std::ffi::c_void,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Option<(&koldstore_migrate::order::CatalogColumn, String)> {
    // Cross-type PostgreSQL equality operators can be canonical while their
    // Datum representations differ (for example float4 = float8). The literal
    // formatter below uses the column's type, so only exact operand types are
    // safe to reconstruct as a source predicate.
    if pg_sys::exprType(left.cast::<pg_sys::Node>())
        != pg_sys::exprType(right.cast::<pg_sys::Node>())
    {
        return None;
    }
    if let Some(column) = var_column(left.cast::<pg_sys::Expr>(), catalog) {
        if let Some(literal) = typed_literal_sql(right.cast::<pg_sys::Expr>(), column, params) {
            return Some((column, literal));
        }
    }
    if let Some(column) = var_column(right.cast::<pg_sys::Expr>(), catalog) {
        if let Some(literal) = typed_literal_sql(left.cast::<pg_sys::Expr>(), column, params) {
            return Some((column, literal));
        }
    }
    None
}

unsafe fn var_column(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'_>,
) -> Option<&koldstore_migrate::order::CatalogColumn> {
    let expr = unwrap_relabel(expr);
    if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_Var {
        return None;
    }
    let var = expr.cast::<pg_sys::Var>();
    let attno = (*var).varattno;
    let scanrelid = i32::try_from(catalog.scanrelid).ok()?;
    if attno <= 0 || (*var).varlevelsup != 0 || (*var).varno != scanrelid {
        return None;
    }
    let name = pg_sys::get_attname(catalog.table_oid, attno, true);
    if name.is_null() {
        return None;
    }
    let name_text = CStr::from_ptr(name).to_string_lossy().into_owned();
    pg_sys::pfree(name.cast());
    catalog
        .columns
        .iter()
        .find(|column| column.name == name_text)
}

unsafe fn operator_is_pg_catalog(operator: pg_sys::Oid) -> bool {
    let tuple = pg_sys::SearchSysCache1(
        pg_sys::SysCacheIdentifier::OPEROID as i32,
        pg_sys::Datum::from(operator),
    );
    if tuple.is_null() {
        return false;
    }
    let form = pg_sys::GETSTRUCT(tuple).cast::<pg_sys::FormData_pg_operator>();
    let is_pg_catalog = (*form).oprnamespace == pg_sys::PG_CATALOG_NAMESPACE.into();
    pg_sys::ReleaseSysCache(tuple);
    is_pg_catalog
}

fn cstr_to_str(value: *const c_char) -> Option<&'static str> {
    if value.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(value).to_str().ok() }
}

#[cfg(test)]
mod projection_tests {
    #[test]
    fn whole_row_bitmap_offset_matches_postgresql_contract() {
        assert_eq!(-pgrx::pg_sys::FirstLowInvalidHeapAttributeNumber, 7);
    }
}
