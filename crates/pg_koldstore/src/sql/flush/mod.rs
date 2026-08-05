//! PostgreSQL flush SQL entrypoints and SPI adapters.

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

/// Enqueues a flush job through the SQL API and returns its UUID.
///
/// SQL contract:
/// `koldstore.enqueue_flush_job(table_name regclass, force boolean default false) → uuid`.
///
/// Same durable-queue contract as [`flush_table_pg`]: inserts a pending job or
/// returns the existing active job id, upgrading pending force intent when
/// requested. Does not spawn executors (use `flush_table` for that).
///
/// Flush jobs are table-wide (`scope_key = ''`), matching `flush_table`. Per-user
/// partitioning for user-scoped tables is owned by manage-time `scope_column` /
/// session `koldstore.user_id`, not by this enqueue argument.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "enqueue_flush_job", schema = "koldstore", security_definer)]
pub fn enqueue_flush_job_pg(
    table_name: pgrx::PgRelation,
    force: pgrx::default!(bool, false),
) -> pgrx::Uuid {
    enqueue_flush_job_pg_impl(table_name.oid(), force)
        .unwrap_or_else(|error| pgrx::error!("enqueue flush job failed: {error}"))
}

#[cfg(feature = "pg")]
fn enqueue_flush_job_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> Result<pgrx::Uuid, String> {
    jobs::enqueue_or_lookup_flush_job(table_oid, force).map_err(|error| error.to_string())
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
    table_name: pgrx::PgRelation,
    dry_run: pgrx::default!(bool, false),
) -> i64 {
    recover_segments_pg_impl(table_name.oid(), dry_run)
        .unwrap_or_else(|error| pgrx::error!("recover segments failed: {error}"))
}

#[cfg(feature = "pg")]
fn recover_segments_pg_impl(table_oid: pgrx::pg_sys::Oid, dry_run: bool) -> Result<i64, String> {
    use std::collections::HashSet;

    use koldstore_flush::recovery::{
        apply_recovery_plan, discover_orphan_objects, plan_recovery_actions, ObjectPath,
        RecoveryAction, RecoveryStep,
    };
    use koldstore_manifest::{try_load_manifest_with_client, CatalogManifestSegmentRow};
    use koldstore_storage::{
        join_object_key, manifest_object_key, render_regular_table_prefix, PathTemplate,
    };
    use pgrx::datum::DatumWithOid;

    let relation = crate::catalog::resolve::relation_context(table_oid)?;
    let storage = crate::catalog::resolve::active_flush_storage_context(table_oid)?;
    let client = crate::object_store::open_managed_object_store_client(
        &storage.storage_type,
        &storage.base_path,
        &storage.credentials,
        &storage.config,
    )
    .map_err(|error| error.to_string())?;
    let prefix = render_regular_table_prefix(
        &PathTemplate::new(&storage.regular_path_tmpl),
        &relation.namespace,
        &relation.name,
    )?;
    let manifest_path = manifest_object_key(&prefix);
    let mut referenced = HashSet::from([manifest_path.clone()]);
    if let Some(manifest) = try_load_manifest_with_client(&client, &manifest_path)? {
        referenced.extend(
            manifest
                .shards
                .iter()
                .map(|shard| join_object_key(&prefix, &shard.path)),
        );
        referenced.extend(
            manifest
                .segments
                .into_iter()
                .map(|segment| join_object_key(&prefix, &segment.path)),
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
    referenced.extend(
        catalog_rows
            .into_iter()
            .map(|row| join_object_key(&prefix, &row.path)),
    );

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
        referenced.remove(&join_object_key(&prefix, path));
    }

    let objects = discover_orphan_objects(&client, &prefix, &referenced)?;
    let mut recovery = plan_recovery_actions(objects);

    // Explicitly plan quarantine for expired pending objects even if LIST missed them.
    for relative_path in &expired_paths {
        let path = join_object_key(&prefix, relative_path);
        if recovery
            .actions
            .iter()
            .any(|step| step.path.as_str() == path.as_str())
        {
            continue;
        }
        let Ok(object_path) = ObjectPath::parse(&path) else {
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
/// `koldstore.flush_table(table_name regclass, force boolean default false) → uuid`.
///
/// Enqueues (or reuses) a durable flush job and returns its UUID immediately.
/// With `koldstore.flush_execution = 'queue'` (default), a one-shot executor is
/// spawned best-effort. With `'inline'`, the calling backend runs the flush
/// after enqueue (required for `#[pg_test]` SPI transactions).
///
/// Fails immediately in inline mode when another backend holds this table's
/// session flush lock (`flush already in progress`). Nested inline does not
/// take the database apply/slot lock until finalize; callers must drain WAL
/// first (`wait_for_async_mirror` / prior apply).
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "flush_table", schema = "koldstore", security_definer)]
pub fn flush_table_pg(
    table_name: pgrx::PgRelation,
    force: pgrx::default!(bool, false),
) -> pgrx::Uuid {
    execute::flush_table_pg_impl(table_name.oid(), force)
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
    table_name: pgrx::default!(Option<pgrx::PgRelation>, "NULL"),
) -> pgrx::JsonB {
    let statuses = statuses.map(|value| value.0);
    let job_types = job_types.map(|value| value.0);
    let table_oid = table_name.as_ref().map(pgrx::PgRelation::oid);
    jobs::list_jobs_json(statuses, job_types, table_oid)
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
pub fn cancel_table_jobs_pg(table_name: pgrx::PgRelation) -> i64 {
    jobs::request_cancel_table_jobs(table_name.oid())
        .unwrap_or_else(|error| pgrx::error!("cancel table jobs failed: {error}"))
}

/// Purges aged terminal jobs in a small batch.
///
/// SQL contract: `koldstore.purge_old_jobs(batch_limit int default 100) → bigint`
///
/// Uses `koldstore.job_retention_days` (0 disables). Never deletes jobs still
/// referenced by `pending` cold segments.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "purge_old_jobs", schema = "koldstore", security_definer)]
pub fn purge_old_jobs_pg(batch_limit: pgrx::default!(i32, 100)) -> i64 {
    let retention_days = crate::guc::job_retention_days();
    jobs::purge_old_jobs(retention_days, batch_limit)
        .unwrap_or_else(|error| pgrx::error!("purge old jobs failed: {error}"))
}
