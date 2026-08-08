//! Operational PostgreSQL SQL entrypoints.

#[cfg(feature = "pg")]
use koldstore_common::QualifiedTableName;

/// Operator status for one managed table, including database async-mirror health.
///
/// SQL contract: `koldstore.table_status(table_name regclass) → jsonb`.
///
/// Returns table storage / hot / mirror / cold / jobs fields plus `async_mirror`
/// (WAL tip vs applied LSN, slot, retention). This is the single public operator
/// status entrypoint.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "table_status", schema = "koldstore", security_definer)]
pub fn table_status_pg(table_name: pgrx::PgRelation) -> pgrx::JsonB {
    table_status_pg_impl(table_name.oid())
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("table status failed: {error}"))
}

#[cfg(feature = "pg")]
fn table_status_pg_impl(table_oid: pgrx::pg_sys::Oid) -> Result<serde_json::Value, String> {
    let mut value = table_status_fields(table_oid)?;
    let async_mirror = crate::mirror::status::async_mirror_status_value()
        .unwrap_or_else(|error| serde_json::json!({ "error": error, "healthy": false }));
    if let Some(obj) = value.as_object_mut() {
        obj.insert("async_mirror".to_string(), async_mirror);
    }
    Ok(value)
}

/// Table-only status fields (no async mirror). Used by [`table_status_pg_impl`].
#[cfg(feature = "pg")]
fn table_status_fields(table_oid: pgrx::pg_sys::Oid) -> Result<serde_json::Value, String> {
    use pgrx::datum::DatumWithOid;

    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table = QualifiedTableName::parse(&relation).map_err(|error| error.to_string())?;
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = QualifiedTableName::from_table_name(&snapshot.mirror_relation);
    let plan = koldstore_flush::ops::table_status_plan(&table, &mirror)
        .map_err(|error| error.to_string())?;
    let json = crate::merge_scan::pg::with_custom_scan_disabled(|| {
        crate::spi::select_one::<String>(&plan.statement, &[DatumWithOid::from(table_oid)])
    })
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "table status lookup returned no rows".to_string())?;
    serde_json::from_str(&json).map_err(|error| error.to_string())
}
