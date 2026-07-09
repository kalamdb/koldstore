//! PostgreSQL flush SQL entrypoints and SPI adapters.

pub use koldstore_flush::ops::*;

#[cfg(feature = "pg")]
pub(crate) mod counters;
#[cfg(feature = "pg")]
mod execute;
#[cfg(feature = "pg")]
mod jobs;
#[cfg(feature = "pg")]
mod mirror_fetch;
#[cfg(feature = "pg")]
mod spi;

#[cfg(feature = "pg")]
use koldstore_common::TableName;

/// Enqueues a flush job through the SQL API.
///
/// SQL contract:
/// `koldstore.enqueue_flush_job(table_name regclass, force boolean default false)`.
///
/// Flush jobs are table-wide (`scope_key = ''`), matching `flush_table`. Per-user
/// partitioning for user-scoped tables is owned by manage-time `scope_column` /
/// session `koldstore.user_id`, not by this enqueue argument.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "enqueue_flush_job", schema = "koldstore", security_definer)]
pub fn enqueue_flush_job_pg(
    table_name: pgrx::pg_sys::Oid,
    force: pgrx::default!(bool, false),
) -> i64 {
    enqueue_flush_job_pg_impl(table_name, force)
        .unwrap_or_else(|error| pgrx::error!("enqueue flush job failed: {error}"))
}

#[cfg(feature = "pg")]
fn enqueue_flush_job_pg_impl(table_oid: pgrx::pg_sys::Oid, force: bool) -> Result<i64, String> {
    use pgrx::datum::DatumWithOid;

    let table_name = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table_name = TableName::parse(&table_name).map_err(|error| error.to_string())?;
    let plan = enqueue_flush_job_plan(flush_table_request(table_name, None, force), None)
        .map_err(|error| error.to_string())?;

    let inserted = crate::spi::update_one::<pgrx::Uuid>(
        &plan.statement,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(Option::<&str>::None),
            DatumWithOid::from(Option::<i64>::None),
            DatumWithOid::from(force),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(if inserted.is_some() { 1 } else { 0 })
}

/// Discovers and recovers orphaned segment objects through the SQL API.
///
/// SQL contract:
/// `koldstore.recover_segments(table_name regclass, dry_run boolean default false)`.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "recover_segments", schema = "koldstore", security_definer)]
pub fn recover_segments_pg(
    table_name: pgrx::pg_sys::Oid,
    dry_run: pgrx::default!(bool, false),
) -> i64 {
    recover_segments_pg_impl(table_name, dry_run)
        .unwrap_or_else(|error| pgrx::error!("recover segments failed: {error}"))
}

#[cfg(feature = "pg")]
fn recover_segments_pg_impl(table_oid: pgrx::pg_sys::Oid, dry_run: bool) -> Result<i64, String> {
    use std::collections::HashSet;

    use koldstore_flush::recovery::{
        apply_recovery_plan, discover_orphan_objects, plan_recovery_actions,
    };
    use koldstore_manifest::{
        relative_manifest_path, table_object_prefix, try_load_manifest_with_client,
    };

    let relation = crate::catalog::resolve::relation_context(table_oid)?;
    let storage = crate::catalog::resolve::active_flush_storage_context(table_oid)?;
    let client = koldstore_storage::open_client_from_catalog_fields(
        &storage.storage_type,
        &storage.base_path,
        &storage.credentials,
        &storage.config,
    )
    .map_err(|error| error.to_string())?;
    let prefix = table_object_prefix(&relation.namespace, &relation.name);
    let manifest_path = relative_manifest_path(&relation.namespace, &relation.name);
    let mut referenced = HashSet::from([manifest_path.clone()]);
    if let Some(manifest) = try_load_manifest_with_client(&client, &manifest_path)? {
        referenced.extend(
            manifest
                .segments
                .into_iter()
                .map(|segment| format!("{prefix}/{}", segment.path.trim_start_matches('/'))),
        );
    }
    let objects = discover_orphan_objects(&client, &prefix, &referenced)?;
    let recovery = plan_recovery_actions(objects);
    let count = i64::try_from(recovery.actions.len()).map_err(|error| error.to_string())?;
    if !dry_run {
        apply_recovery_plan(&client, &recovery)?;
    }
    Ok(count)
}

/// Flushes one managed table scope from SQL.
///
/// SQL contract: `koldstore.flush_table(table_name regclass)`.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "flush_table", schema = "koldstore", security_definer)]
pub fn flush_table_pg(table_name: pgrx::pg_sys::Oid) -> pgrx::Uuid {
    execute::flush_table_pg_impl(table_name)
        .unwrap_or_else(|error| pgrx::error!("flush table failed: {error}"))
}
