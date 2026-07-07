//! Operational PostgreSQL SQL entrypoints.

#[cfg(feature = "pg")]
use koldstore_common::{QualifiedTableName, ScopeKey};

/// Describes a managed table's storage, mirror, cold segments, and jobs.
///
/// SQL contract: `koldstore.describe_table(regclass, scope_key text default null)`.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "describe_table", schema = "koldstore", security_definer)]
pub fn describe_table_pg(
    table_name: pgrx::pg_sys::Oid,
    scope_key: pgrx::default!(Option<&str>, "NULL"),
) -> pgrx::JsonB {
    describe_table_pg_impl(table_name, scope_key)
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("describe table failed: {error}"))
}

#[cfg(feature = "pg")]
fn describe_table_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    scope_key: Option<&str>,
) -> Result<serde_json::Value, String> {
    use pgrx::datum::DatumWithOid;

    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table = QualifiedTableName::parse(&relation).map_err(|error| error.to_string())?;
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = QualifiedTableName::from_table_name(&snapshot.mirror_relation);
    let scope_key = scope_key
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
        .map(ScopeKey::new)
        .transpose()
        .map_err(|error| error.to_string())?;
    let scope_key_arg = scope_key
        .as_ref()
        .map(ScopeKey::as_str)
        .unwrap_or("")
        .to_string();
    let plan = koldstore_flush::ops::describe_table_plan(&table, &mirror, scope_key)
        .map_err(|error| error.to_string())?;
    let json = crate::merge_scan::pg::with_custom_scan_disabled(|| {
        crate::spi::select_one::<String>(
            &plan.statement,
            &[
                DatumWithOid::from(table_oid),
                DatumWithOid::from(scope_key_arg.as_str()),
            ],
        )
    })
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "describe table lookup returned no rows".to_string())?;
    serde_json::from_str(&json).map_err(|error| error.to_string())
}
