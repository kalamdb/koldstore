//! Operational PostgreSQL SQL entrypoints.

#[cfg(feature = "pg")]
use koldstore_common::QualifiedTableName;

/// Describes a managed table's storage, mirror, cold segments, and jobs.
///
/// SQL contract: `koldstore.describe_table(table_name regclass)`.
///
/// Status is table-wide. User-scoped tables still report aggregate hot/mirror/
/// cold counters across scopes; per-scope session filtering belongs to DML and
/// merge scan via `koldstore.user_id`, not this operator view.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "describe_table", schema = "koldstore", security_definer)]
pub fn describe_table_pg(table_name: pgrx::PgRelation) -> pgrx::JsonB {
    describe_table_pg_impl(table_name.oid())
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("describe table failed: {error}"))
}

#[cfg(feature = "pg")]
fn describe_table_pg_impl(table_oid: pgrx::pg_sys::Oid) -> Result<serde_json::Value, String> {
    use pgrx::datum::DatumWithOid;

    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table = QualifiedTableName::parse(&relation).map_err(|error| error.to_string())?;
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = QualifiedTableName::from_table_name(&snapshot.mirror_relation);
    let plan = koldstore_flush::ops::describe_table_plan(&table, &mirror)
        .map_err(|error| error.to_string())?;
    let json = crate::merge_scan::pg::with_custom_scan_disabled(|| {
        crate::spi::select_one::<String>(&plan.statement, &[DatumWithOid::from(table_oid)])
    })
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "describe table lookup returned no rows".to_string())?;
    serde_json::from_str(&json).map_err(|error| error.to_string())
}
