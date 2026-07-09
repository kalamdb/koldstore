//! PostgreSQL planner literal extraction for merge-scan filters.
//!
//! Supports both planner `Const` nodes and bound `PARAM_EXTERN` values so
//! prepared/parameterized queries (`WHERE id = $1`) prune cold segments and
//! push equality into the hot SPI load the same way as literal SQL.

use std::ffi::CStr;

use koldstore_common::escape_sql_literal;
use koldstore_schema::PgType;
use pgrx::pg_sys;

pub(super) fn sql_literal(value: &str) -> String {
    escape_sql_literal(value)
}

/// Resolves a Const or bound Param into a typed SQL literal for SPI pushdown.
pub(super) unsafe fn typed_literal_sql(
    expr: *mut pg_sys::Expr,
    column: &koldstore_migrate::order::CatalogColumn,
    params: pg_sys::ParamListInfo,
) -> Option<String> {
    let Some((datum, isnull, _)) = const_or_param_datum(expr, params) else {
        return None;
    };
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
    let Some((datum, isnull, _)) = const_or_param_datum(expr, params) else {
        return None;
    };
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
        PgType::Text | PgType::Numeric | PgType::Uuid | PgType::Jsonb | PgType::TextArray => {
            let text = datum.cast_mut_ptr::<pg_sys::text>();
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
        PgType::Bool => Some((datum.value() != 0).to_string()),
        PgType::Int2 | PgType::Int4 | PgType::Int8 => {
            pg_type.integer_sql_literal(datum.value() as i64)
        }
        _ => {
            // Fall back to type output for timestamptz / other typed columns.
            let mut typoutput = pg_sys::InvalidOid;
            let mut typisvarlena = false;
            // Use catalog OID when available; otherwise skip.
            let oid = column_type_oid(pg_type)?;
            pg_sys::getTypeOutputInfo(oid, &mut typoutput, &mut typisvarlena);
            let out = pg_sys::OidOutputFunctionCall(typoutput, datum);
            if out.is_null() {
                return None;
            }
            let text = CStr::from_ptr(out).to_str().ok()?.to_string();
            pg_sys::pfree(out.cast());
            Some(format!("'{}'", sql_literal(&text)))
        }
    }
}

unsafe fn datum_json_value(
    datum: pg_sys::Datum,
    column: &koldstore_migrate::order::CatalogColumn,
) -> Option<serde_json::Value> {
    match column.pg_type {
        PgType::Text | PgType::Uuid => {
            let text = datum.cast_mut_ptr::<pg_sys::text>();
            if text.is_null() {
                return None;
            }
            let cstr = pg_sys::text_to_cstring(text);
            if cstr.is_null() {
                return None;
            }
            let value = CStr::from_ptr(cstr).to_str().ok()?.to_string();
            pg_sys::pfree(cstr.cast());
            Some(serde_json::Value::String(value))
        }
        PgType::Bool => Some(serde_json::Value::Bool(datum.value() != 0)),
        PgType::Int2 | PgType::Int4 | PgType::Int8 => Some(serde_json::json!(datum.value() as i64)),
        _ => None,
    }
}

fn column_type_oid(pg_type: PgType) -> Option<pg_sys::Oid> {
    // Keep in sync with koldstore_schema::PgType OID mapping for output fallback.
    let oid = match pg_type {
        PgType::Timestamptz => 1184u32,
        PgType::Bytea => 17,
        PgType::Float4 => 700,
        PgType::Float8 => 701,
        _ => return None,
    };
    Some(pg_sys::Oid::from(oid))
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
