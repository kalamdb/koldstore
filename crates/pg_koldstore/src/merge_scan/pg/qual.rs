//! Planner qual walking for prune predicates, residual filters, and indexed checks.

use std::collections::BTreeSet;
use std::ffi::CStr;
use std::os::raw::c_char;

use koldstore_merge::scan::plan::SegmentPrunePredicate;
use pgrx::pg_sys;

use super::literals::{list_node_pointers, literal_json_value, typed_literal_sql, unwrap_relabel};

/// Residual filters extracted from planner quals for Rust merge.
#[derive(Debug, Default)]
pub(super) struct ResidualFilters {
    pub(super) equality: Vec<(String, String)>,
    pub(super) membership: Vec<(String, Vec<String>)>,
    /// Typed SQL equality predicates safe to push into the hot SPI load.
    pub(super) hot_equality: Vec<super::hot::HotEqualityFilter>,
}

pub(super) unsafe fn requested_target_columns(
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

pub(super) unsafe fn residual_filters(
    qual: *mut pg_sys::List,
    columns: &[koldstore_migrate::order::CatalogColumn],
    params: pg_sys::ParamListInfo,
) -> ResidualFilters {
    let mut filters = ResidualFilters::default();
    for node in list_node_pointers(qual) {
        collect_residual_filters(node.cast::<pg_sys::Expr>(), columns, params, &mut filters);
    }
    filters
}

pub(super) unsafe fn segment_prune_predicates(
    qual: *mut pg_sys::List,
    columns: &[koldstore_migrate::order::CatalogColumn],
    params: pg_sys::ParamListInfo,
) -> Vec<SegmentPrunePredicate> {
    list_node_pointers(qual)
        .into_iter()
        .flat_map(|node| {
            segment_prune_node_predicates(node.cast::<pg_sys::Expr>(), columns, params)
        })
        .collect()
}

pub(super) unsafe fn validate_filter_columns_indexed(
    qual: *mut pg_sys::List,
    catalog: &koldstore_migrate::ExistingTableCatalog,
) -> Result<(), String> {
    let indexed = catalog
        .primary_key
        .columns
        .iter()
        .map(String::as_str)
        .chain(catalog.indexed_columns.iter().map(String::as_str))
        .collect::<BTreeSet<_>>();
    for column in filter_column_names(qual, &catalog.columns) {
        if !indexed.contains(column.as_str()) {
            return Err(format!(
                "cold filter column `{column}` is not indexed; koldstore cold reads require WHERE filters on indexed columns"
            ));
        }
    }
    Ok(())
}

unsafe fn filter_column_names(
    qual: *mut pg_sys::List,
    columns: &[koldstore_migrate::order::CatalogColumn],
) -> BTreeSet<String> {
    list_node_pointers(qual)
        .into_iter()
        .flat_map(|node| filter_node_column_names(node.cast::<pg_sys::Expr>(), columns))
        .collect()
}

unsafe fn filter_node_column_names(
    expr: *mut pg_sys::Expr,
    columns: &[koldstore_migrate::order::CatalogColumn],
) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    if expr.is_null() {
        return names;
    }
    match (*expr).type_ {
        pg_sys::NodeTag::T_OpExpr => {
            let op_expr = expr.cast::<pg_sys::OpExpr>();
            for arg in list_node_pointers((*op_expr).args) {
                collect_var_column_names(arg.cast::<pg_sys::Expr>(), columns, &mut names);
            }
        }
        pg_sys::NodeTag::T_ScalarArrayOpExpr => {
            let scalar = expr.cast::<pg_sys::ScalarArrayOpExpr>();
            for arg in list_node_pointers((*scalar).args) {
                collect_var_column_names(arg.cast::<pg_sys::Expr>(), columns, &mut names);
            }
        }
        pg_sys::NodeTag::T_BoolExpr => {
            let bool_expr = expr.cast::<pg_sys::BoolExpr>();
            for arg in list_node_pointers((*bool_expr).args) {
                names.extend(filter_node_column_names(
                    arg.cast::<pg_sys::Expr>(),
                    columns,
                ));
            }
        }
        _ => collect_var_column_names(expr, columns, &mut names),
    }
    names
}

unsafe fn collect_var_column_names(
    expr: *mut pg_sys::Expr,
    columns: &[koldstore_migrate::order::CatalogColumn],
    names: &mut BTreeSet<String>,
) {
    if let Some(column) = var_column(expr, columns) {
        names.insert(column.name.clone());
    }
}

unsafe fn segment_prune_node_predicates(
    expr: *mut pg_sys::Expr,
    columns: &[koldstore_migrate::order::CatalogColumn],
    params: pg_sys::ParamListInfo,
) -> Vec<SegmentPrunePredicate> {
    if expr.is_null() {
        return Vec::new();
    }
    match (*expr).type_ {
        pg_sys::NodeTag::T_OpExpr => segment_prune_op_expr(expr, columns, params)
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
                    segment_prune_node_predicates(node.cast::<pg_sys::Expr>(), columns, params)
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

unsafe fn segment_prune_op_expr(
    expr: *mut pg_sys::Expr,
    columns: &[koldstore_migrate::order::CatalogColumn],
    params: pg_sys::ParamListInfo,
) -> Option<SegmentPrunePredicate> {
    let op_expr = expr.cast::<pg_sys::OpExpr>();
    let opname = cstr_to_str(pg_sys::get_opname((*op_expr).opno))?;
    if !matches!(opname, "=" | "<" | "<=" | ">" | ">=") {
        return None;
    }
    let args = list_node_pointers((*op_expr).args);
    if args.len() != 2 {
        return None;
    }

    if let Some((column, literal)) = var_and_json_literal(args[0], args[1], columns, params) {
        return prune_predicate_from_op(column, literal, opname, false);
    }
    if let Some((column, literal)) = var_and_json_literal(args[1], args[0], columns, params) {
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
    columns: &[koldstore_migrate::order::CatalogColumn],
    params: pg_sys::ParamListInfo,
) -> Option<(&koldstore_migrate::order::CatalogColumn, serde_json::Value)> {
    let column = var_column(column_expr.cast::<pg_sys::Expr>(), columns)?;
    let literal = literal_json_value(literal_expr.cast::<pg_sys::Expr>(), column, params)?;
    Some((column, literal))
}

unsafe fn collect_residual_filters(
    expr: *mut pg_sys::Expr,
    columns: &[koldstore_migrate::order::CatalogColumn],
    params: pg_sys::ParamListInfo,
    filters: &mut ResidualFilters,
) {
    if expr.is_null() {
        return;
    }
    match (*expr).type_ {
        pg_sys::NodeTag::T_OpExpr => {
            if let Some((column, expected, sql_literal)) =
                op_expr_equality_filter(expr, columns, params)
            {
                filters.equality.push((column.clone(), expected));
                filters.hot_equality.push(super::hot::HotEqualityFilter {
                    column,
                    sql_literal,
                });
            }
        }
        pg_sys::NodeTag::T_ScalarArrayOpExpr => {
            if let Some(membership) = scalar_array_membership_filter(expr, columns) {
                filters.membership.push(membership);
            }
        }
        pg_sys::NodeTag::T_BoolExpr => {
            let bool_expr = expr.cast::<pg_sys::BoolExpr>();
            if (*bool_expr).boolop != pg_sys::BoolExprType::AND_EXPR {
                return;
            }
            for arg in list_node_pointers((*bool_expr).args) {
                collect_residual_filters(arg.cast::<pg_sys::Expr>(), columns, params, filters);
            }
        }
        _ => {}
    }
}

unsafe fn op_expr_equality_filter(
    expr: *mut pg_sys::Expr,
    columns: &[koldstore_migrate::order::CatalogColumn],
    params: pg_sys::ParamListInfo,
) -> Option<(String, String, String)> {
    let op_expr = expr.cast::<pg_sys::OpExpr>();
    let opname = cstr_to_str(pg_sys::get_opname((*op_expr).opno))?;
    if opname != "=" {
        return None;
    }
    let args = list_node_pointers((*op_expr).args);
    if args.len() != 2 {
        return None;
    }
    let (column, literal) = equality_var_and_literal(args[0], args[1], columns, params)?;
    let expected = strip_sql_literal(&literal);
    Some((column.name.clone(), expected, literal))
}

unsafe fn scalar_array_membership_filter(
    expr: *mut pg_sys::Expr,
    columns: &[koldstore_migrate::order::CatalogColumn],
) -> Option<(String, Vec<String>)> {
    let scalar = expr.cast::<pg_sys::ScalarArrayOpExpr>();
    if !(*scalar).useOr {
        return None;
    }
    let args = list_node_pointers((*scalar).args);
    if args.len() != 2 {
        return None;
    }
    let column = var_column(args[0].cast::<pg_sys::Expr>(), columns)?;
    let values = array_literal_filter_values(args[1].cast::<pg_sys::Expr>())?;
    if values.is_empty() {
        return None;
    }
    Some((column.name.clone(), values))
}

unsafe fn array_literal_filter_values(expr: *mut pg_sys::Expr) -> Option<Vec<String>> {
    use super::literals::list_node_pointers;
    if expr.is_null() {
        return None;
    }
    match (*expr).type_ {
        pg_sys::NodeTag::T_ArrayExpr => {
            let array = expr.cast::<pg_sys::ArrayExpr>();
            Some(
                list_node_pointers((*array).elements)
                    .into_iter()
                    .filter_map(|node| const_filter_value(node.cast::<pg_sys::Expr>()))
                    .collect(),
            )
        }
        // Planner often folds `IN (1,2,4)` into a single array Const (`{1,2,4}`).
        // Expanding that Const is required; treating the whole array output as one
        // membership value filters every row out.
        pg_sys::NodeTag::T_Const => const_array_filter_values(expr),
        _ => None,
    }
}

/// Expands an array Const into per-element filter strings, or a scalar Const.
unsafe fn const_array_filter_values(expr: *mut pg_sys::Expr) -> Option<Vec<String>> {
    let expr = unwrap_relabel(expr);
    if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_Const {
        return None;
    }
    let konst = expr.cast::<pg_sys::Const>();
    if (*konst).constisnull {
        return None;
    }

    let elmtype = pg_sys::get_element_type((*konst).consttype);
    if elmtype == pg_sys::InvalidOid {
        return const_filter_value(expr).map(|value| vec![value]);
    }

    let mut elmlen: i16 = 0;
    let mut elmbyval = false;
    let mut elmalign: std::os::raw::c_char = 0;
    pg_sys::get_typlenbyvalalign(elmtype, &mut elmlen, &mut elmbyval, &mut elmalign);

    let varlena = (*konst).constvalue.cast_mut_ptr::<pg_sys::varlena>();
    if varlena.is_null() {
        return None;
    }
    let array = pg_sys::pg_detoast_datum(varlena).cast::<pg_sys::ArrayType>();
    if array.is_null() {
        return None;
    }

    let mut elems: *mut pg_sys::Datum = std::ptr::null_mut();
    let mut nulls: *mut bool = std::ptr::null_mut();
    let mut nelems: i32 = 0;
    pg_sys::deconstruct_array(
        array,
        elmtype,
        i32::from(elmlen),
        elmbyval,
        elmalign,
        &mut elems,
        &mut nulls,
        &mut nelems,
    );
    if elems.is_null() || nelems < 0 {
        return None;
    }

    let mut typoutput = pg_sys::InvalidOid;
    let mut typisvarlena = false;
    pg_sys::getTypeOutputInfo(elmtype, &mut typoutput, &mut typisvarlena);

    let mut values = Vec::with_capacity(nelems as usize);
    for index in 0..nelems as usize {
        if !nulls.is_null() && *nulls.add(index) {
            continue;
        }
        let out = pg_sys::OidOutputFunctionCall(typoutput, *elems.add(index));
        if out.is_null() {
            continue;
        }
        if let Ok(text) = CStr::from_ptr(out).to_str() {
            values.push(text.to_string());
        }
        pg_sys::pfree(out.cast());
    }
    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

unsafe fn const_filter_value(expr: *mut pg_sys::Expr) -> Option<String> {
    let expr = unwrap_relabel(expr);
    if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_Const {
        return None;
    }
    let konst = expr.cast::<pg_sys::Const>();
    if (*konst).constisnull {
        return None;
    }
    let mut typoutput = pg_sys::InvalidOid;
    let mut typisvarlena = false;
    pg_sys::getTypeOutputInfo((*konst).consttype, &mut typoutput, &mut typisvarlena);
    let out = pg_sys::OidOutputFunctionCall(typoutput, (*konst).constvalue);
    if out.is_null() {
        return None;
    }
    let text = CStr::from_ptr(out).to_str().ok()?.to_string();
    pg_sys::pfree(out.cast());
    Some(text)
}

unsafe fn equality_var_and_literal(
    left: *mut std::ffi::c_void,
    right: *mut std::ffi::c_void,
    columns: &[koldstore_migrate::order::CatalogColumn],
    params: pg_sys::ParamListInfo,
) -> Option<(&koldstore_migrate::order::CatalogColumn, String)> {
    if let Some(column) = var_column(left.cast::<pg_sys::Expr>(), columns) {
        if let Some(literal) = typed_literal_sql(right.cast::<pg_sys::Expr>(), column, params) {
            return Some((column, literal));
        }
    }
    if let Some(column) = var_column(right.cast::<pg_sys::Expr>(), columns) {
        if let Some(literal) = typed_literal_sql(left.cast::<pg_sys::Expr>(), column, params) {
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

fn strip_sql_literal(literal: &str) -> String {
    if literal.starts_with('\'') && literal.ends_with('\'') && literal.len() >= 2 {
        literal[1..literal.len() - 1].replace("''", "'")
    } else {
        literal.to_string()
    }
}

fn cstr_to_str(value: *const c_char) -> Option<&'static str> {
    if value.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(value).to_str().ok() }
}
