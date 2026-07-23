//! PostgreSQL flush SQL entrypoints and SPI adapters.

pub use koldstore_flush::ops::*;

#[cfg(feature = "pg")]
pub(crate) mod counters;
#[cfg(feature = "pg")]
pub(crate) mod execute;
#[cfg(feature = "pg")]
pub(crate) mod jobs;
#[cfg(feature = "pg")]
mod mirror_fetch;
#[cfg(feature = "pg")]
pub(crate) mod spi;

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
/// Also expires stale `pending` catalog rows older than
/// `koldstore.pending_segment_ttl_seconds` (quarantine object + delete row).
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
        apply_recovery_plan, discover_orphan_objects, plan_recovery_actions, ObjectPath,
        RecoveryAction, RecoveryStep,
    };
    use koldstore_manifest::{
        relative_manifest_path, table_object_prefix, try_load_manifest_with_client,
        CatalogManifestSegmentRow,
    };
    use pgrx::datum::DatumWithOid;

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
    // Include pending + active so in-flight uploads are not quarantined early.
    let catalog_segments =
        koldstore_catalog::queries::plan_publishable_cold_segments_for_manifest_json()
            .map_err(|error| error.to_string())?;
    let catalog_json =
        crate::spi::select_one::<String>(&catalog_segments, &[DatumWithOid::from(table_oid)])
            .map_err(|error| error.to_string())?
            .unwrap_or_else(|| "[]".to_string());
    let catalog_rows: Vec<CatalogManifestSegmentRow> =
        serde_json::from_str(&catalog_json).map_err(|error| error.to_string())?;
    referenced.extend(catalog_rows.into_iter().map(|row| row.object_path));

    let ttl_seconds = crate::guc::pending_segment_ttl_seconds();
    let expired_plan = koldstore_catalog::queries::plan_expired_pending_segment_paths()
        .map_err(|error| error.to_string())?;
    let expired_json = crate::spi::select_one::<String>(
        &expired_plan,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(ttl_seconds),
        ],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or_else(|| "[]".to_string());
    let expired_paths: Vec<String> =
        serde_json::from_str(&expired_json).map_err(|error| error.to_string())?;

    // Expired pending paths are removed from the referenced set so LIST recovery
    // can quarantine them; catalog rows are deleted after object actions.
    for path in &expired_paths {
        referenced.remove(path);
    }

    let objects = discover_orphan_objects(&client, &prefix, &referenced)?;
    let mut recovery = plan_recovery_actions(objects);

    // Explicitly plan quarantine for expired pending objects even if LIST missed them.
    for path in &expired_paths {
        if recovery
            .actions
            .iter()
            .any(|step| step.path.as_str() == path.as_str())
        {
            continue;
        }
        let Ok(object_path) = ObjectPath::parse(path) else {
            continue;
        };
        recovery.actions.push(RecoveryStep {
            path: object_path,
            manifest_referenced: false,
            action: RecoveryAction::QuarantineFinal,
        });
    }

    let count = i64::try_from(recovery.actions.len()).map_err(|error| error.to_string())?;
    if !dry_run {
        apply_recovery_plan(&client, &recovery)?;
        if !expired_paths.is_empty() {
            let delete_plan = koldstore_catalog::queries::plan_delete_expired_pending_segments()
                .map_err(|error| error.to_string())?;
            crate::spi::update(
                &delete_plan,
                &[
                    DatumWithOid::from(table_oid),
                    DatumWithOid::from(ttl_seconds),
                ],
            )
            .map_err(|error| error.to_string())?;
        }
    }
    Ok(count)
}

/// Flushes one managed table scope from SQL.
///
/// SQL contract:
/// `koldstore.flush_table(table_name regclass, force boolean default false)`.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "flush_table", schema = "koldstore", security_definer)]
pub fn flush_table_pg(
    table_name: pgrx::pg_sys::Oid,
    force: pgrx::default!(bool, false),
) -> pgrx::Uuid {
    execute::flush_table_pg_impl(table_name, force)
        .unwrap_or_else(|error| pgrx::error!("flush table failed: {error}"))
}

/// Lists KoldStore jobs for operator / UI polling.
///
/// SQL contract:
/// `koldstore.list_jobs(statuses jsonb default null, job_types jsonb default null, table_name regclass default null)`.
///
/// `statuses` / `job_types` are optional JSON arrays of strings, for example
/// `'["running","pending"]'::jsonb`. Returns a JSON array of job objects.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "list_jobs", schema = "koldstore", security_definer)]
pub fn list_jobs_pg(
    statuses: pgrx::default!(Option<pgrx::JsonB>, "NULL"),
    job_types: pgrx::default!(Option<pgrx::JsonB>, "NULL"),
    table_name: pgrx::default!(Option<pgrx::pg_sys::Oid>, "NULL"),
) -> pgrx::JsonB {
    let statuses = statuses.map(|value| value.0);
    let job_types = job_types.map(|value| value.0);
    jobs::list_jobs_json(statuses, job_types, table_name)
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("list jobs failed: {error}"))
}

/// Requests cooperative cancel for one job.
///
/// SQL contract: `koldstore.cancel_job(job_id uuid) → boolean`
/// (`true` when an active job was signalled).
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "cancel_job", schema = "koldstore", security_definer)]
pub fn cancel_job_pg(job_id: pgrx::Uuid) -> bool {
    let job_id = crate::spi::uuid_from_pgrx(job_id);
    jobs::request_cancel_job(job_id)
        .unwrap_or_else(|error| pgrx::error!("cancel job failed: {error}"))
}

/// Requests cooperative cancel for all active jobs on a table.
///
/// SQL contract: `koldstore.cancel_table_jobs(table_name regclass) → bigint`
/// (number of jobs signalled or hard-cancelled).
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "cancel_table_jobs", schema = "koldstore", security_definer)]
pub fn cancel_table_jobs_pg(table_name: pgrx::pg_sys::Oid) -> i64 {
    jobs::request_cancel_table_jobs(table_name)
        .unwrap_or_else(|error| pgrx::error!("cancel table jobs failed: {error}"))
}
