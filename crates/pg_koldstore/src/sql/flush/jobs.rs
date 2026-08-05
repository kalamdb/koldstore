//! Flush job lifecycle SPI adapters.

use koldstore_flush::{
    plan_abandon_running_flush_jobs, plan_cancel_jobs_for_drop, plan_clear_table_cancel_request,
    plan_flush_cancel_requested, plan_insert_inline_flush_job, plan_list_jobs,
    plan_list_running_flush_table_oids, plan_lookup_active_inline_flush_job,
    plan_mark_inline_flush_job_cancelled, plan_mark_inline_flush_job_completed,
    plan_mark_inline_flush_job_completed_after_cancel, plan_mark_inline_flush_job_failed,
    plan_mark_inline_flush_job_running, plan_request_cancel_job, plan_request_cancel_table_jobs,
    plan_update_inline_flush_job_progress,
};

#[derive(serde::Deserialize)]
struct PendingFlushJobWire {
    id: String,
    #[serde(default)]
    force: bool,
}

pub(super) fn ensure_flush_job(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> crate::error::PgResult<(uuid::Uuid, bool)> {
    use pgrx::datum::DatumWithOid;

    // Caller must hold the table-job lock. Any durable `running` row here has no
    // live owner (crash / abandoned statement) — mark error so uniqueness clears.
    abandon_running_flush_jobs(table_oid)?;

    let lookup = plan_lookup_active_inline_flush_job()
        .map_err(crate::error::PgAdapterError::from_display)?;
    let existing = crate::spi::select_one::<String>(&lookup, &[DatumWithOid::from(table_oid)])?
        .filter(|value| !value.is_empty());
    if let Some(existing) = existing {
        let wire: PendingFlushJobWire = serde_json::from_str(&existing)?;
        return Ok((uuid::Uuid::parse_str(&wire.id)?, force || wire.force));
    }

    let job_id = uuid::Uuid::new_v4();
    let insert =
        plan_insert_inline_flush_job().map_err(crate::error::PgAdapterError::from_display)?;
    crate::spi::update(
        &insert,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(force),
        ],
    )?;
    Ok((job_id, force))
}

pub(crate) fn abandon_running_flush_jobs(
    table_oid: pgrx::pg_sys::Oid,
) -> crate::error::PgResult<u64> {
    use pgrx::datum::DatumWithOid;

    let statement =
        plan_abandon_running_flush_jobs().map_err(crate::error::PgAdapterError::from_display)?;
    let rows = crate::spi::update(&statement, &[DatumWithOid::from(table_oid)])?;
    Ok(rows.rows_affected)
}

/// Reclaims durable `running` flush jobs whose table-job lock is free.
pub(crate) fn reclaim_orphan_running_flush_jobs() -> crate::error::PgResult<u64> {
    let statement =
        plan_list_running_flush_table_oids().map_err(crate::error::PgAdapterError::from_display)?;
    let json =
        crate::spi::select_one::<String>(&statement, &[])?.unwrap_or_else(|| "[]".to_string());
    let oids: Vec<i64> = serde_json::from_str(&json)?;
    let mut abandoned = 0_u64;
    for oid_i64 in oids {
        let table_oid = pgrx::pg_sys::Oid::from(u32::try_from(oid_i64).unwrap_or(0));
        if table_oid == pgrx::pg_sys::InvalidOid {
            continue;
        }
        if crate::sql::job_lock::try_lock_table_job(table_oid)? {
            abandoned = abandoned.saturating_add(abandon_running_flush_jobs(table_oid)?);
        }
    }
    Ok(abandoned)
}

pub(super) fn mark_flush_job_running(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
    progress_total: i64,
) -> crate::error::PgResult<()> {
    use pgrx::datum::DatumWithOid;

    let statement =
        plan_mark_inline_flush_job_running().map_err(crate::error::PgAdapterError::from_display)?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(progress_total),
        ],
    )?;
    Ok(())
}

/// Progress fields written to an inline flush job between phases/waves.
pub(super) struct FlushJobProgressUpdate<'a> {
    pub rows_flushed: i64,
    pub batches_completed: i32,
    pub checkpoint_seq: i64,
    pub phase: &'a str,
    pub progress_total: i64,
}

/// Persists mid-flush progress for operator visibility (`list_jobs` / job row).
pub(super) fn update_flush_job_progress(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
    progress: FlushJobProgressUpdate<'_>,
) -> crate::error::PgResult<()> {
    use pgrx::datum::DatumWithOid;

    let statement = plan_update_inline_flush_job_progress()
        .map_err(crate::error::PgAdapterError::from_display)?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(progress.rows_flushed),
            DatumWithOid::from(progress.batches_completed),
            DatumWithOid::from(progress.checkpoint_seq),
            DatumWithOid::from(progress.phase),
            DatumWithOid::from(progress.progress_total),
        ],
    )?;
    Ok(())
}

pub(super) fn mark_flush_job_completed(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
    rows_flushed: i64,
    checkpoint_seq: i64,
    batches_completed: i32,
) -> crate::error::PgResult<()> {
    use pgrx::datum::DatumWithOid;

    let statement = plan_mark_inline_flush_job_completed()
        .map_err(crate::error::PgAdapterError::from_display)?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(rows_flushed),
            DatumWithOid::from(checkpoint_seq),
            DatumWithOid::from(batches_completed),
        ],
    )?;
    clear_table_cancel_request(table_oid)?;
    Ok(())
}

pub(super) fn mark_flush_job_failed(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
    error_trace: &str,
) -> crate::error::PgResult<()> {
    use pgrx::datum::DatumWithOid;

    let statement =
        plan_mark_inline_flush_job_failed().map_err(crate::error::PgAdapterError::from_display)?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(error_trace),
        ],
    )?;
    clear_table_cancel_request(table_oid)?;
    Ok(())
}

/// Lists jobs as a JSON array for `koldstore.list_jobs`.
pub(crate) fn list_jobs_json(
    statuses: Option<serde_json::Value>,
    job_types: Option<serde_json::Value>,
    table_oid: Option<pgrx::pg_sys::Oid>,
) -> crate::error::PgResult<serde_json::Value> {
    use pgrx::datum::DatumWithOid;

    let statement = plan_list_jobs().map_err(crate::error::PgAdapterError::from_display)?;
    let text = crate::spi::select_one::<String>(
        &statement,
        &[
            DatumWithOid::from(statuses.map(pgrx::JsonB)),
            DatumWithOid::from(job_types.map(pgrx::JsonB)),
            DatumWithOid::from(table_oid),
        ],
    )?
    .unwrap_or_else(|| "[]".to_string());
    serde_json::from_str(&text).map_err(crate::error::PgAdapterError::from)
}

pub(super) fn flush_cancel_requested(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
) -> crate::error::PgResult<bool> {
    use pgrx::datum::DatumWithOid;

    let statement =
        plan_flush_cancel_requested().map_err(crate::error::PgAdapterError::from_display)?;
    Ok(crate::spi::select_one::<bool>(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
        ],
    )?
    .unwrap_or(false))
}

pub(super) fn mark_flush_job_cancelled(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
) -> crate::error::PgResult<()> {
    use pgrx::datum::DatumWithOid;

    let statement = plan_mark_inline_flush_job_cancelled()
        .map_err(crate::error::PgAdapterError::from_display)?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
        ],
    )?;
    clear_table_cancel_request(table_oid)?;
    Ok(())
}

pub(super) fn mark_flush_job_completed_after_cancel(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
    rows_flushed: i64,
    checkpoint_seq: i64,
    batches_completed: i32,
) -> crate::error::PgResult<()> {
    use pgrx::datum::DatumWithOid;

    let statement = plan_mark_inline_flush_job_completed_after_cancel()
        .map_err(crate::error::PgAdapterError::from_display)?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(rows_flushed),
            DatumWithOid::from(checkpoint_seq),
            DatumWithOid::from(batches_completed),
        ],
    )?;
    clear_table_cancel_request(table_oid)?;
    Ok(())
}

fn clear_table_cancel_request(table_oid: pgrx::pg_sys::Oid) -> crate::error::PgResult<()> {
    use pgrx::datum::DatumWithOid;

    let statement =
        plan_clear_table_cancel_request().map_err(crate::error::PgAdapterError::from_display)?;
    crate::spi::update(&statement, &[DatumWithOid::from(table_oid)])?;
    Ok(())
}

/// Requests cancel for one job. Returns true when a row was updated.
pub(crate) fn request_cancel_job(job_id: uuid::Uuid) -> crate::error::PgResult<bool> {
    use pgrx::datum::DatumWithOid;

    let statement =
        plan_request_cancel_job().map_err(crate::error::PgAdapterError::from_display)?;
    let updated = crate::spi::update_one::<String>(
        &statement,
        &[DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id))],
    )?;
    Ok(updated.is_some())
}

/// Requests cancel for all active jobs on a table. Returns affected row count.
pub(crate) fn request_cancel_table_jobs(
    table_oid: pgrx::pg_sys::Oid,
) -> crate::error::PgResult<i64> {
    use pgrx::datum::DatumWithOid;

    let statement =
        plan_request_cancel_table_jobs().map_err(crate::error::PgAdapterError::from_display)?;
    Ok(crate::spi::update_one::<i64>(&statement, &[DatumWithOid::from(table_oid)])?.unwrap_or(0))
}

/// DROP/unmanage: cancel pending hard, signal running soft. Returns touched count.
pub(crate) fn cancel_jobs_for_drop(table_oid: pgrx::pg_sys::Oid) -> crate::error::PgResult<i64> {
    use pgrx::datum::DatumWithOid;

    let statement =
        plan_cancel_jobs_for_drop().map_err(crate::error::PgAdapterError::from_display)?;
    Ok(crate::spi::update_one::<i64>(&statement, &[DatumWithOid::from(table_oid)])?.unwrap_or(0))
}
