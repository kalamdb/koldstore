//! PostgreSQL planner literal extraction for merge-scan filters.
//!
//! Supports both planner `Const` nodes and bound `PARAM_EXTERN` values so
//! prepared/parameterized queries (`WHERE id = $1`) prune cold segments and
//! push equality into the hot SPI load the same way as literal SQL.

use std::ffi::{CStr, CString};

use koldstore_schema::PgType;
use pgrx::pg_sys;

use super::spi::PgAllocatedCString;

/// Resolves a Const or bound Param into a typed SQL literal for SPI pushdown.
pub(super) unsafe fn typed_literal_sql(
    expr: *mut pg_sys::Expr,
    column: &koldstore_migrate::order::CatalogColumn,
    params: pg_sys::ParamListInfo,
) -> Option<String> {
    let (datum, isnull, _) = const_or_param_datum(expr, params)?;
    if isnull {
        return None;
    }
    datum_typed_sql(datum, column)
}

/// Resolves a Const or bound Param into a JSON value for prune/residual filters.
pub(super) unsafe fn literal_json_value(
    expr: *mut pg_sys::Expr,
    column: &koldstore_migrate::order::CatalogColumn,
    params: pg_sys::ParamListInfo,
) -> Option<serde_json::Value> {
    let (datum, isnull, _) = const_or_param_datum(expr, params)?;
    if isnull {
        return None;
    }
    datum_json_value(datum, column)
}

unsafe fn const_or_param_datum(
    expr: *mut pg_sys::Expr,
    params: pg_sys::ParamListInfo,
) -> Option<(pg_sys::Datum, bool, pg_sys::Oid)> {
    let expr = unwrap_relabel(expr);
    if expr.is_null() {
        return None;
    }
    match (*expr).type_ {
        pg_sys::NodeTag::T_Const => {
            let konst = expr.cast::<pg_sys::Const>();
            Some((
                (*konst).constvalue,
                (*konst).constisnull,
                (*konst).consttype,
            ))
        }
        pg_sys::NodeTag::T_Param => {
            let param = expr.cast::<pg_sys::Param>();
            if (*param).paramkind != pg_sys::ParamKind::PARAM_EXTERN {
                return None;
            }
            let param_id = (*param).paramid;
            if params.is_null() || param_id < 1 || param_id > (*params).numParams {
                return None;
            }
            // Prefer the fetch hook (used by some ParamListInfo owners); otherwise
            // read the inline params[] slot (libpq prepared statements).
            if let Some(fetch) = (*params).paramFetch {
                let mut workspace = pg_sys::ParamExternData::default();
                let fetched = fetch(params, param_id, false, &mut workspace);
                if fetched.is_null() {
                    return None;
                }
                Some(((*fetched).value, (*fetched).isnull, (*fetched).ptype))
            } else {
                let slot = (*params).params.as_slice((*params).numParams as usize);
                let entry = &slot[(param_id - 1) as usize];
                Some((entry.value, entry.isnull, entry.ptype))
            }
        }
        _ => None,
    }
}

unsafe fn datum_typed_sql(
    datum: pg_sys::Datum,
    column: &koldstore_migrate::order::CatalogColumn,
) -> Option<String> {
    let pg_type = column.pg_type;
    match pg_type {
        PgType::Bool => Some((datum.value() != 0).to_string()),
        PgType::Int2 | PgType::Int4 | PgType::Int8 => {
            pg_type.integer_sql_literal(datum.value() as i64)
        }
        PgType::Text
        | PgType::Numeric
        | PgType::Uuid
        | PgType::Jsonb
        | PgType::TextArray
        | PgType::Bytea
        | PgType::Timestamptz
        | PgType::Float4
        | PgType::Float8 => {
            // Use PostgreSQL's real output function: varlena binary formats
            // such as numeric/jsonb/arrays are not compatible with `text`.
            let mut typoutput = pg_sys::InvalidOid;
            let mut typisvarlena = false;
            let oid = column_type_oid(pg_type);
            pg_sys::getTypeOutputInfo(oid, &mut typoutput, &mut typisvarlena);
            let out = pg_sys::OidOutputFunctionCall(typoutput, datum);
            if out.is_null() {
                return None;
            }
            let out = PgAllocatedCString::from_raw(out);
            let text = out.as_c_str().to_str().ok()?.to_string();
            quote_sql_literal(&text)
        }
    }
}

unsafe fn quote_sql_literal(value: &str) -> Option<String> {
    let raw = CString::new(value).ok()?;
    let quoted = pg_sys::quote_literal_cstr(raw.as_ptr());
    if quoted.is_null() {
        return None;
    }
    // PostgreSQL emits the appropriate ordinary or escape-string literal for
    // the active `standard_conforming_strings` mode.
    let literal = CStr::from_ptr(quoted).to_string_lossy().into_owned();
    pg_sys::pfree(quoted.cast());
    Some(literal)
}

unsafe fn datum_json_value(
    datum: pg_sys::Datum,
    column: &koldstore_migrate::order::CatalogColumn,
) -> Option<serde_json::Value> {
    match column.pg_type {
        PgType::Text => {
            let text = datum.cast_mut_ptr::<pg_sys::text>();
            if text.is_null() {
                return None;
            }
            let cstr = pg_sys::text_to_cstring(text);
            if cstr.is_null() {
                return None;
            }
            let cstr = PgAllocatedCString::from_raw(cstr);
            let value = cstr.as_c_str().to_str().ok()?.to_string();
            Some(serde_json::Value::String(value))
        }
        PgType::Uuid => {
            let mut typoutput = pg_sys::InvalidOid;
            let mut typisvarlena = false;
            let oid = column_type_oid(PgType::Uuid);
            pg_sys::getTypeOutputInfo(oid, &mut typoutput, &mut typisvarlena);
            let out = pg_sys::OidOutputFunctionCall(typoutput, datum);
            if out.is_null() {
                return None;
            }
            let out = PgAllocatedCString::from_raw(out);
            let value = out.as_c_str().to_str().ok()?.to_string();
            Some(serde_json::Value::String(value))
        }
        PgType::Bool => Some(serde_json::Value::Bool(datum.value() != 0)),
        PgType::Int2 | PgType::Int4 | PgType::Int8 => Some(serde_json::json!(datum.value() as i64)),
        _ => None,
    }
}

fn column_type_oid(pg_type: PgType) -> pg_sys::Oid {
    pg_sys::Oid::from(pg_type.type_oid())
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
