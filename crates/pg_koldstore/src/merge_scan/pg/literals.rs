//! PostgreSQL planner literal extraction for merge-scan filters.

use std::ffi::CStr;
use std::os::raw::c_int;

use koldstore_common::escape_sql_literal;
use koldstore_schema::{PgIntegerArrayOid, PgType};
use pgrx::pg_sys;

pub(super) fn sql_literal(value: &str) -> String {
    escape_sql_literal(value)
}

pub(super) unsafe fn typed_literal_sql(
    expr: *mut pg_sys::Expr,
    column: &koldstore_migrate::order::CatalogColumn,
) -> Option<String> {
    let expr = unwrap_relabel(expr);
    if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_Const {
        return None;
    }
    let konst = expr.cast::<pg_sys::Const>();
    if (*konst).constisnull {
        return None;
    }

    let pg_type = column.pg_type;
    match pg_type {
        PgType::Text | PgType::Numeric | PgType::Uuid | PgType::Jsonb | PgType::TextArray => {
            let text = (*konst).constvalue.cast_mut_ptr::<pg_sys::text>();
            if text.is_null() {
                return None;
            }
            let cstr = pg_sys::text_to_cstring(text);
            if cstr.is_null() {
                return None;
            }
            let value = CStr::from_ptr(cstr).to_str().ok()?;
            let literal = format!("'{}'", sql_literal(value));
            pg_sys::pfree(cstr.cast());
            Some(literal)
        }
        PgType::Bool => match (*konst).consttype.to_u32() {
            16 => Some(((*konst).constvalue.value() != 0).to_string()),
            _ => None,
        },
        PgType::Int2 | PgType::Int4 | PgType::Int8 => pg_type.integer_sql_literal(
            (*konst).constvalue.value() as i64,
        ),
        _ => const_literal_sql(expr),
    }
}

pub(super) unsafe fn scalar_array_filter_sql(
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
        type_name = column.catalog_type_name(),
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
    let array_oid = (*konst).consttype.to_u32();
    let integer_array = PgIntegerArrayOid::from_oid(array_oid)?;
    let element_pg_type = integer_array.element_type();
    let element_oid = integer_array.element_oid();
    let (elem_len, elem_byval, elem_align) = match integer_array {
        PgIntegerArrayOid::Int2 => (2, true, b's' as std::os::raw::c_char),
        PgIntegerArrayOid::Int4 => (4, true, b'i' as std::os::raw::c_char),
        PgIntegerArrayOid::Int8 => (8, true, b'd' as std::os::raw::c_char),
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
        pg_sys::Oid::from(element_oid),
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
        let sql = element_pg_type.integer_sql_literal(value.value() as i64)?;
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
    let pg_type = PgType::from_integer_oid((*konst).consttype.to_u32())?;
    pg_type.integer_sql_literal((*konst).constvalue.value() as i64)
}

pub(super) unsafe fn unwrap_relabel(expr: *mut pg_sys::Expr) -> *mut pg_sys::Expr {
    if expr.is_null() {
        return expr;
    }
    if (*expr).type_ == pg_sys::NodeTag::T_RelabelType {
        let relabel = expr.cast::<pg_sys::RelabelType>();
        (*relabel).arg.cast::<pg_sys::Expr>()
    } else {
        expr
    }
}

pub(super) unsafe fn list_node_pointers(list: *mut pg_sys::List) -> Vec<*mut std::ffi::c_void> {
    if list.is_null() {
        return Vec::new();
    }
    let len = usize::try_from((*list).length).unwrap_or(0);
    (0..len)
        .map(|index| (*(*list).elements.add(index)).ptr_value)
        .collect::<Vec<_>>()
}
