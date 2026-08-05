//! Planner qual walking for safe prune predicates and post-merge filters.

use std::collections::BTreeSet;
use std::ffi::CStr;
use std::os::raw::c_char;

use koldstore_merge::scan::plan::SegmentPrunePredicate;
use pgrx::pg_sys;

use super::literals::{list_node_pointers, literal_json_value, typed_literal_sql, unwrap_relabel};

/// One base-relation attribute required by output projection or executor quals.
#[derive(Debug, Clone)]
pub(super) struct ScanProjectionColumn {
    pub(super) catalog: koldstore_migrate::order::CatalogColumn,
    /// Zero-based position in the base relation's scan tuple.
    pub(super) slot_index: usize,
}

/// Minimal base-relation projection used to build tuples for PostgreSQL `ExecScan`.
#[derive(Debug, Clone)]
pub(super) struct ScanProjection {
    pub(super) columns: Vec<ScanProjectionColumn>,
    pub(super) tuple_width: usize,
}

impl ScanProjection {
    pub(super) fn catalog_columns(&self) -> Vec<&koldstore_migrate::order::CatalogColumn> {
        self.columns.iter().map(|column| &column.catalog).collect()
    }
}

/// Canonical predicates eligible for primary-key source pruning.
#[derive(Debug, Default)]
pub(super) struct ResidualFilters {
    pub(super) hot_equality: Vec<super::hot::HotEqualityFilter>,
    pub(super) hot_range: Vec<super::hot::HotRangeFilter>,
}

#[derive(Debug, Clone, Copy)]
struct QualCatalog<'a> {
    scanrelid: pg_sys::Index,
    columns: &'a [koldstore_migrate::order::CatalogColumn],
}

/// Full physical base-relation projection for `custom_scan_tlist` scans.
///
/// When PlanCustomPath sets `custom_scan_tlist`, setrefs rewrites plan
/// targetlist/qual Vars to `INDEX_VAR`. [`required_scan_projection`] then sees
/// no `scanrelid` Vars; materialize every user column so ExecScan can project.
pub(super) fn physical_scan_projection(
    columns: &[koldstore_migrate::order::CatalogColumn],
    tuple_width: usize,
) -> Result<ScanProjection, String> {
    let mut projection = Vec::with_capacity(tuple_width);
    for slot_index in 0..tuple_width {
        let attnum =
            pg_sys::AttrNumber::try_from(slot_index + 1).map_err(|error| error.to_string())?;
        let Some(catalog) = columns
            .iter()
            .find(|column| column.column_id.get() == attnum)
        else {
            // Dropped attributes stay NULL in the physical slot.
            continue;
        };
        projection.push(ScanProjectionColumn {
            catalog: catalog.clone(),
            slot_index,
        });
    }
    Ok(ScanProjection {
        columns: projection,
        tuple_width,
    })
}

/// Collects every base-table attribute referenced by the target list or quals.
///
/// PostgreSQL's generic Var walker covers arbitrary RLS expressions and planned
/// subqueries, so cold enforcement does not depend on KoldStore understanding a
/// policy's expression shape.
pub(super) unsafe fn required_scan_projection(
    scanrelid: pg_sys::Index,
    targetlist: *mut pg_sys::List,
    qual: *mut pg_sys::List,
    columns: &[koldstore_migrate::order::CatalogColumn],
    tuple_width: usize,
) -> Result<ScanProjection, String> {
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
        let Some(catalog) = columns
            .iter()
            .find(|column| column.column_id.get() == attnum)
        else {
            if whole_row {
                // PostgreSQL represents a dropped attribute in a whole-row
                // value as NULL; it is intentionally absent from our catalog.
                continue;
            }
            return Err(format!(
                "required base-relation attribute {} is not present in the managed schema",
                slot_index + 1,
            ));
        };
        projection.push(ScanProjectionColumn {
            catalog: catalog.clone(),
            slot_index,
        });
    }

    Ok(ScanProjection {
        columns: projection,
        tuple_width,
    })
}

pub(super) unsafe fn residual_filters(
    scanrelid: pg_sys::Index,
    qual: *mut pg_sys::List,
    columns: &[koldstore_migrate::order::CatalogColumn],
    params: pg_sys::ParamListInfo,
) -> ResidualFilters {
    let catalog = QualCatalog { scanrelid, columns };
    let mut filters = ResidualFilters::default();
    for node in list_node_pointers(qual) {
        collect_residual_filters(node.cast::<pg_sys::Expr>(), catalog, params, &mut filters);
    }
    filters
}

/// Returns whether equality clauses cover every primary-key attribute.
///
/// Only PostgreSQL catalog `=` operators with a base-relation [`pg_sys::Var`]
/// and a constant or external prepared-statement parameter qualify. Executor
/// parameters used by parameterized nested-loop paths remain on the
/// conservative merge path because their value may change across rescans.
pub(super) unsafe fn quals_cover_primary_key(
    scanrelid: pg_sys::Index,
    qual: *mut pg_sys::List,
    primary_key_attnums: &[i16],
) -> bool {
    if primary_key_attnums.is_empty() {
        return false;
    }
    let mut covered = BTreeSet::new();
    for node in list_node_pointers(qual) {
        collect_equality_attnums(node.cast::<pg_sys::Expr>(), scanrelid, &mut covered);
    }
    primary_key_attnums
        .iter()
        .all(|attnum| covered.contains(attnum))
}

unsafe fn collect_equality_attnums(
    expr: *mut pg_sys::Expr,
    scanrelid: pg_sys::Index,
    covered: &mut BTreeSet<i16>,
) {
    if expr.is_null() {
        return;
    }
    match (*expr).type_ {
        pg_sys::NodeTag::T_OpExpr => {
            let op_expr = expr.cast::<pg_sys::OpExpr>();
            if cstr_to_str(pg_sys::get_opname((*op_expr).opno)) != Some("=")
                || !operator_is_pg_catalog((*op_expr).opno)
            {
                return;
            }
            let args = list_node_pointers((*op_expr).args);
            if args.len() != 2 {
                return;
            }
            if let Some(attnum) = point_var_attnum(args[0], args[1], scanrelid)
                .or_else(|| point_var_attnum(args[1], args[0], scanrelid))
            {
                covered.insert(attnum);
            }
        }
        pg_sys::NodeTag::T_BoolExpr => {
            let bool_expr = expr.cast::<pg_sys::BoolExpr>();
            if (*bool_expr).boolop != pg_sys::BoolExprType::AND_EXPR {
                return;
            }
            for arg in list_node_pointers((*bool_expr).args) {
                collect_equality_attnums(arg.cast::<pg_sys::Expr>(), scanrelid, covered);
            }
        }
        _ => {}
    }
}

unsafe fn point_var_attnum(
    variable_expr: *mut std::ffi::c_void,
    value_expr: *mut std::ffi::c_void,
    scanrelid: pg_sys::Index,
) -> Option<i16> {
    let variable_expr = unwrap_relabel(variable_expr.cast::<pg_sys::Expr>());
    if variable_expr.is_null() || (*variable_expr).type_ != pg_sys::NodeTag::T_Var {
        return None;
    }
    let var = variable_expr.cast::<pg_sys::Var>();
    let scanrelid = i32::try_from(scanrelid).ok()?;
    if (*var).varattno <= 0 || (*var).varlevelsup != 0 || (*var).varno != scanrelid {
        return None;
    }

    let value_expr = unwrap_relabel(value_expr.cast::<pg_sys::Expr>());
    if value_expr.is_null() {
        return None;
    }
    match (*value_expr).type_ {
        pg_sys::NodeTag::T_Const => Some((*var).varattno),
        pg_sys::NodeTag::T_Param => {
            let param = value_expr.cast::<pg_sys::Param>();
            ((*param).paramkind == pg_sys::ParamKind::PARAM_EXTERN).then_some((*var).varattno)
        }
        _ => None,
    }
}

pub(super) unsafe fn segment_prune_predicates(
    scanrelid: pg_sys::Index,
    qual: *mut pg_sys::List,
    columns: &[koldstore_migrate::order::CatalogColumn],
    params: pg_sys::ParamListInfo,
) -> Vec<SegmentPrunePredicate> {
    let catalog = QualCatalog { scanrelid, columns };
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
    let column_id = column.column_id.get();
    match (opname, reversed) {
        ("=", _) => Some(SegmentPrunePredicate::equality(
            column_id,
            &column.name,
            literal,
        )),
        (">" | ">=", false) | ("<" | "<=", true) => Some(SegmentPrunePredicate::lower_bound(
            column_id,
            &column.name,
            literal,
        )),
        ("<" | "<=", false) | (">" | ">=", true) => Some(SegmentPrunePredicate::upper_bound(
            column_id,
            &column.name,
            literal,
        )),
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
            if let Some(range) = op_expr_range_filter(expr, catalog, params) {
                filters.hot_range.push(range);
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
    let (column, literal) = hot_var_and_literal(args[0], args[1], catalog, params)?;
    Some((column.name.clone(), literal))
}

unsafe fn op_expr_range_filter(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Option<super::hot::HotRangeFilter> {
    let op_expr = expr.cast::<pg_sys::OpExpr>();
    let operator = match cstr_to_str(pg_sys::get_opname((*op_expr).opno))? {
        "<" => super::hot::HotRangeOperator::LessThan,
        "<=" => super::hot::HotRangeOperator::LessThanOrEqual,
        ">" => super::hot::HotRangeOperator::GreaterThan,
        ">=" => super::hot::HotRangeOperator::GreaterThanOrEqual,
        _ => return None,
    };
    if !operator_is_pg_catalog((*op_expr).opno) {
        return None;
    }
    let args = list_node_pointers((*op_expr).args);
    if args.len() != 2 {
        return None;
    }
    if let Some((column, sql_literal)) = hot_var_and_literal(args[0], args[1], catalog, params) {
        return Some(super::hot::HotRangeFilter {
            column: column.name.clone(),
            operator,
            sql_literal,
        });
    }
    let (column, sql_literal) = hot_var_and_literal(args[1], args[0], catalog, params)?;
    Some(super::hot::HotRangeFilter {
        column: column.name.clone(),
        operator: reverse_range_operator(operator),
        sql_literal,
    })
}

const fn reverse_range_operator(
    operator: super::hot::HotRangeOperator,
) -> super::hot::HotRangeOperator {
    match operator {
        super::hot::HotRangeOperator::LessThan => super::hot::HotRangeOperator::GreaterThan,
        super::hot::HotRangeOperator::LessThanOrEqual => {
            super::hot::HotRangeOperator::GreaterThanOrEqual
        }
        super::hot::HotRangeOperator::GreaterThan => super::hot::HotRangeOperator::LessThan,
        super::hot::HotRangeOperator::GreaterThanOrEqual => {
            super::hot::HotRangeOperator::LessThanOrEqual
        }
    }
}

unsafe fn hot_var_and_literal(
    left: *mut std::ffi::c_void,
    right: *mut std::ffi::c_void,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Option<(&koldstore_migrate::order::CatalogColumn, String)> {
    if let Some(column) = var_column(left.cast::<pg_sys::Expr>(), catalog) {
        if hot_filter_operand_types_compatible(
            column,
            pg_sys::exprType(right.cast::<pg_sys::Node>()),
        ) {
            if let Some(literal) = typed_literal_sql(right.cast::<pg_sys::Expr>(), column, params) {
                return Some((column, literal));
            }
        }
    }
    if let Some(column) = var_column(right.cast::<pg_sys::Expr>(), catalog) {
        if hot_filter_operand_types_compatible(
            column,
            pg_sys::exprType(left.cast::<pg_sys::Node>()),
        ) {
            if let Some(literal) = typed_literal_sql(left.cast::<pg_sys::Expr>(), column, params) {
                return Some((column, literal));
            }
        }
    }
    None
}

/// Returns true when a Const/Param OID can be reconstructed as a hot SPI
/// predicate against `column`.
///
/// Exact matches are always allowed. Integer width promotions (`int2`/`int4`
/// literal against a wider integer column) are allowed so untyped numeric
/// literals behave like an explicit cast. Float and other cross-types stay
/// rejected because Datum layouts are not interchangeable for SPI literals.
fn hot_filter_operand_types_compatible(
    column: &koldstore_migrate::order::CatalogColumn,
    literal_oid: pg_sys::Oid,
) -> bool {
    let literal_oid = u32::from(literal_oid);
    if literal_oid == column.pg_type.type_oid() {
        return true;
    }
    let Some(literal_ty) = koldstore_schema::PgType::from_integer_oid(literal_oid) else {
        return false;
    };
    column.pg_type.accepts_integer_equality_literal(literal_ty)
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
    catalog
        .columns
        .iter()
        .find(|column| column.column_id.get() == attno)
}

unsafe fn operator_is_pg_catalog(operator: pg_sys::Oid) -> bool {
    // Windows bindgen may already type this as i32; keep the cast for Linux/macOS.
    #[allow(clippy::unnecessary_cast)]
    let cache_id = pg_sys::SysCacheIdentifier::OPEROID as i32;
    let tuple = pg_sys::SearchSysCache1(cache_id, pg_sys::Datum::from(operator));
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
