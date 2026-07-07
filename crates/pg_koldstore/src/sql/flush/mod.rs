//! PostgreSQL flush SQL entrypoints.

pub use koldstore_flush::ops::*;

#[cfg(feature = "pg")]
mod execute;
#[cfg(feature = "pg")]
mod jobs;
#[cfg(feature = "pg")]
mod segments;
#[cfg(feature = "pg")]
mod stats;
#[cfg(feature = "pg")]
mod write;

#[cfg(feature = "pg")]
use koldstore_common::{ScopeKey, TableName};

/// Enqueues a flush job through the SQL API.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "enqueue_flush_job", schema = "koldstore", security_definer)]
pub fn enqueue_flush_job_pg(
    table_oid: pgrx::pg_sys::Oid,
    scope_key: Option<&str>,
    force: bool,
) -> i64 {
    enqueue_flush_job_pg_impl(table_oid, scope_key, force)
        .unwrap_or_else(|error| pgrx::error!("enqueue flush job failed: {error}"))
}

#[cfg(feature = "pg")]
fn enqueue_flush_job_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    scope_key: Option<&str>,
    force: bool,
) -> Result<i64, String> {
    use pgrx::datum::DatumWithOid;

    let table_name = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table_name = TableName::parse(&table_name).map_err(|error| error.to_string())?;
    let scope_key = scope_key
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
        .map(ScopeKey::new)
        .transpose()
        .map_err(|error| error.to_string())?;
    let scope_key_arg = scope_key
        .as_ref()
        .map(ScopeKey::as_str)
        .map(ToString::to_string);
    let plan = enqueue_flush_job_plan(flush_table_request(table_name, scope_key, force), None)
        .map_err(|error| error.to_string())?;

    let inserted = crate::spi::update_one::<pgrx::Uuid>(
        &plan.statement,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(scope_key_arg.as_deref()),
            DatumWithOid::from(Option::<i64>::None),
            DatumWithOid::from(force),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(if inserted.is_some() { 1 } else { 0 })
}

/// Enqueues a segment recovery job through the SQL API.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "recover_segments", schema = "koldstore", security_definer)]
pub fn recover_segments_pg(table_oid: pgrx::pg_sys::Oid, dry_run: bool) -> i64 {
    recover_segments_pg_impl(table_oid, dry_run)
        .unwrap_or_else(|error| pgrx::error!("recover segments failed: {error}"))
}

#[cfg(feature = "pg")]
fn recover_segments_pg_impl(table_oid: pgrx::pg_sys::Oid, dry_run: bool) -> Result<i64, String> {
    use pgrx::datum::DatumWithOid;

    let table_name = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table_name = TableName::parse(&table_name).map_err(|error| error.to_string())?;
    let plan =
        recover_segments_plan(Some(table_name), dry_run).map_err(|error| error.to_string())?;
    let inserted = crate::spi::update_one::<pgrx::Uuid>(
        &plan.statement,
        &[DatumWithOid::from(table_oid), DatumWithOid::from(dry_run)],
    )
    .map_err(|error| error.to_string())?;
    Ok(if inserted.is_some() { 1 } else { 0 })
}

/// Flushes one managed table scope from SQL.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "flush_table", schema = "koldstore", security_definer)]
pub fn flush_table_pg(table_name: pgrx::pg_sys::Oid) -> pgrx::Uuid {
    execute::flush_table_pg_impl(table_name)
        .unwrap_or_else(|error| pgrx::error!("flush table failed: {error}"))
}
