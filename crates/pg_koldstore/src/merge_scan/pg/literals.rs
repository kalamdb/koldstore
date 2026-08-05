//! PostgreSQL planner literal extraction for merge-scan filters.
//!
//! Supports both planner `Const` nodes and bound `PARAM_EXTERN` values so
//! prepared/parameterized queries (`WHERE id = $1`) prune cold segments and
//! push equality into the hot SPI load the same way as literal SQL.

use std::ffi::{CStr, CString};

use koldstore_schema::PgType;
use pgrx::pg_sys;

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

/// Resolves a Const or bound Param into a Sort Key V1 value for cold prune.
pub(super) unsafe fn literal_sort_key_value(
    expr: *mut pg_sys::Expr,
    column: &koldstore_migrate::order::CatalogColumn,
    params: pg_sys::ParamListInfo,
) -> Option<koldstore_sortkey::SortKeyValue> {
    let (datum, isnull, _) = const_or_param_datum(expr, params)?;
    if isnull {
        return None;
    }
    datum_to_sort_key_value(datum, column)
}

/// Resolves a Const or bound Param into a typed cell for prune/residual filters.
pub(super) unsafe fn literal_cell_value(
    expr: *mut pg_sys::Expr,
    column: &koldstore_migrate::order::CatalogColumn,
    params: pg_sys::ParamListInfo,
) -> Option<koldstore_common::CellValue> {
    let (datum, isnull, _) = const_or_param_datum(expr, params)?;
    if isnull {
        return Some(koldstore_common::CellValue::Null);
    }
    datum_to_cell_value(datum, column)
}

/// Resolves a Const or bound Param into a JSON value for prune/residual filters.
pub(super) unsafe fn literal_json_value(
    expr: *mut pg_sys::Expr,
    column: &koldstore_migrate::order::CatalogColumn,
    params: pg_sys::ParamListInfo,
) -> Option<serde_json::Value> {
    literal_cell_value(expr, column, params).map(|cell| cell.to_json())
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
            let text = cstr_owned_pfree(out)?;
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

/// Converts a non-null Datum to a Sort Key V1 value for cold segment prune.
pub(super) unsafe fn datum_to_sort_key_value(
    datum: pg_sys::Datum,
    column: &koldstore_migrate::order::CatalogColumn,
) -> Option<koldstore_sortkey::SortKeyValue> {
    use koldstore_sortkey::SortKeyValue;

    let sort_type = koldstore_sortkey::SortKeyType::from_type_oid(column.pg_type.type_oid())?;
    match sort_type {
        koldstore_sortkey::SortKeyType::Bool => Some(SortKeyValue::Bool(datum.value() != 0)),
        koldstore_sortkey::SortKeyType::Int2 => {
            let narrowed = i16::try_from(datum.value() as i64).ok()?;
            Some(SortKeyValue::Int2(narrowed))
        }
        koldstore_sortkey::SortKeyType::Int4 => {
            let narrowed = i32::try_from(datum.value() as i64).ok()?;
            Some(SortKeyValue::Int4(narrowed))
        }
        koldstore_sortkey::SortKeyType::Int8 => Some(SortKeyValue::Int8(datum.value() as i64)),
        koldstore_sortkey::SortKeyType::Date => {
            let narrowed = i32::try_from(datum.value() as i64).ok()?;
            Some(SortKeyValue::Date(narrowed))
        }
        koldstore_sortkey::SortKeyType::Timestamp => {
            Some(SortKeyValue::Timestamp(datum.value() as i64))
        }
        koldstore_sortkey::SortKeyType::Timestamptz => {
            Some(SortKeyValue::Timestamptz(datum.value() as i64))
        }
        koldstore_sortkey::SortKeyType::Uuid => {
            let mut typoutput = pg_sys::InvalidOid;
            let mut typisvarlena = false;
            let oid = column_type_oid(PgType::Uuid);
            pg_sys::getTypeOutputInfo(oid, &mut typoutput, &mut typisvarlena);
            let out = pg_sys::OidOutputFunctionCall(typoutput, datum);
            let text = cstr_owned_pfree(out)?;
            let uuid = uuid::Uuid::parse_str(&text).ok()?;
            Some(SortKeyValue::Uuid(uuid))
        }
    }
}

/// Converts a non-null Datum to a typed cell for merge-candidate / prune helpers.
pub(super) unsafe fn datum_to_cell_value(
    datum: pg_sys::Datum,
    column: &koldstore_migrate::order::CatalogColumn,
) -> Option<koldstore_common::CellValue> {
    use koldstore_common::CellValue;

    datum_cell_value(datum, column).or_else(|| {
        datum_json_value_via_output(datum, column).map(|value| CellValue::from_json(&value))
    })
}

/// Converts a non-null Datum to JSON for merge-candidate / prune helpers.
pub(super) unsafe fn datum_to_json_value(
    datum: pg_sys::Datum,
    column: &koldstore_migrate::order::CatalogColumn,
) -> Option<serde_json::Value> {
    datum_to_cell_value(datum, column).map(|cell| cell.to_json())
}

unsafe fn datum_cell_value(
    datum: pg_sys::Datum,
    column: &koldstore_migrate::order::CatalogColumn,
) -> Option<koldstore_common::CellValue> {
    use koldstore_common::CellValue;

    match column.pg_type {
        PgType::Text => {
            let text = datum.cast_mut_ptr::<pg_sys::text>();
            if text.is_null() {
                return None;
            }
            let cstr = pg_sys::text_to_cstring(text);
            let value = cstr_owned_pfree(cstr)?;
            Some(CellValue::Utf8(value))
        }
        PgType::Uuid => {
            let mut typoutput = pg_sys::InvalidOid;
            let mut typisvarlena = false;
            let oid = column_type_oid(PgType::Uuid);
            pg_sys::getTypeOutputInfo(oid, &mut typoutput, &mut typisvarlena);
            let out = pg_sys::OidOutputFunctionCall(typoutput, datum);
            let value = cstr_owned_pfree(out)?;
            Some(CellValue::Utf8(value))
        }
        PgType::Bool => Some(CellValue::Bool(datum.value() != 0)),
        PgType::Int2 => {
            let narrowed = i16::try_from(datum.value() as i64).ok()?;
            Some(CellValue::Int16(narrowed))
        }
        PgType::Int4 => {
            let narrowed = i32::try_from(datum.value() as i64).ok()?;
            Some(CellValue::Int32(narrowed))
        }
        PgType::Int8 => Some(CellValue::Int64(datum.value() as i64)),
        // TimestampTzADT is microseconds since the PostgreSQL epoch — the same
        // unit Sort Key V1 persists for timestamptz bounds.
        PgType::Timestamptz => Some(CellValue::TimestamptzMicros(datum.value() as i64)),
        _ => None,
    }
}

unsafe fn datum_json_value_via_output(
    datum: pg_sys::Datum,
    column: &koldstore_migrate::order::CatalogColumn,
) -> Option<serde_json::Value> {
    let mut typoutput = pg_sys::InvalidOid;
    let mut typisvarlena = false;
    let oid = column_type_oid(column.pg_type);
    pg_sys::getTypeOutputInfo(oid, &mut typoutput, &mut typisvarlena);
    let out = pg_sys::OidOutputFunctionCall(typoutput, datum);
    let text = cstr_owned_pfree(out)?;
    match column.pg_type {
        PgType::Float4 | PgType::Float8 => text
            .parse::<f64>()
            .ok()
            .map(|number| serde_json::json!(number)),
        // Keep numeric textual to preserve scale/precision; float parse would
        // lose exact decimal representation used for bound comparisons.
        PgType::Numeric => Some(serde_json::Value::String(text)),
        _ => Some(serde_json::Value::String(text)),
    }
}

/// Copies a PostgreSQL C string into a Rust `String`, then always `pfree`s it.
pub(super) unsafe fn cstr_owned_pfree(ptr: *mut std::os::raw::c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    let owned = CStr::from_ptr(ptr).to_str().ok().map(str::to_string);
    pg_sys::pfree(ptr.cast());
    owned
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
