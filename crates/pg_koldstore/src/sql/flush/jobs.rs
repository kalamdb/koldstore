//! Flush job lifecycle SPI adapters.

use koldstore_common::{SeqId, TableName, TableOid};
use koldstore_flush::{
    flush_table_request, plan_cancel_jobs_for_drop, plan_clear_table_cancel_request,
    plan_count_pending_flush_jobs, plan_enqueue_or_lookup_flush_job, plan_flush_cancel_requested,
    plan_insert_flush_job, plan_list_jobs, plan_list_running_flush_table_oids,
    plan_lookup_active_flush_job, plan_mark_flush_job_cancelled, plan_mark_flush_job_completed,
    plan_mark_flush_job_completed_after_cancel, plan_mark_flush_job_failed,
    plan_mark_flush_job_running, plan_purge_old_jobs, plan_reclaim_running_flush_jobs,
    plan_request_cancel_job, plan_request_cancel_table_jobs, plan_select_pending_flush_candidate,
    plan_update_flush_job_progress, DEFAULT_PURGE_BATCH_LIMIT,
};

#[derive(serde::Deserialize)]
struct PendingFlushJobWire {
    id: String,
    #[serde(default)]
    force: bool,
}

#[derive(serde::Deserialize)]
struct PendingFlushCandidateWire {
    table_oid: i64,
    #[serde(default)]
    force: bool,
}

/// Enqueues a flush job or returns the existing active job UUID.
///
/// When `force = true`, upgrades an existing pending job's payload force intent.
pub(crate) fn enqueue_or_lookup_flush_job(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> crate::error::PgResult<pgrx::Uuid> {
    use pgrx::datum::DatumWithOid;

    let table_name = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table_name =
        TableName::parse(&table_name).map_err(crate::error::PgAdapterError::from_display)?;
    let plan = plan_enqueue_or_lookup_flush_job(flush_table_request(table_name, None, force), None)
        .map_err(crate::error::PgAdapterError::from_display)?;
    let job_id = crate::spi::update_one::<pgrx::Uuid>(
        &plan.statement,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(Option::<&str>::None),
            DatumWithOid::from(Option::<i64>::None),
            DatumWithOid::from(force),
        ],
    )?
    .ok_or_else(|| {
        crate::error::PgAdapterError::from_display("enqueue flush job returned no active job id")
    })?;
    Ok(job_id)
}

/// Looks up a committed active flush job UUID without inserting.
///
/// Used when the session table lock is held by another backend so enqueue must
/// not wait on that backend's uncommitted jobs-row / unique-index conflict.
pub(crate) fn lookup_active_flush_job_uuid(
    table_oid: pgrx::pg_sys::Oid,
) -> crate::error::PgResult<Option<pgrx::Uuid>> {
    use pgrx::datum::DatumWithOid;

    let lookup =
        plan_lookup_active_flush_job().map_err(crate::error::PgAdapterError::from_display)?;
    let existing = crate::spi::select_one::<String>(&lookup, &[DatumWithOid::from(table_oid)])?
        .filter(|value| !value.is_empty());
    let Some(existing) = existing else {
        return Ok(None);
    };
    let wire: PendingFlushJobWire = serde_json::from_str(&existing)?;
    Ok(Some(crate::spi::uuid_to_pgrx(uuid::Uuid::parse_str(
        &wire.id,
    )?)))
}

/// Selects one due pending flush candidate for a one-shot executor.
///
/// Returns a pg-free [`TableOid`]; convert to `pg_sys::Oid` only at SPI / lock edges.
pub(crate) fn select_pending_flush_candidate() -> crate::error::PgResult<Option<(TableOid, bool)>> {
    let statement = plan_select_pending_flush_candidate()
        .map_err(crate::error::PgAdapterError::from_display)?;
    let json = crate::spi::select_one::<String>(&statement, &[])?.unwrap_or_default();
    if json.is_empty() {
        return Ok(None);
    }
    let wire: PendingFlushCandidateWire = serde_json::from_str(&json)?;
    let raw = u32::try_from(wire.table_oid).unwrap_or(0);
    let Ok(table_oid) = TableOid::new(raw) else {
        return Ok(None);
    };
    Ok(Some((table_oid, wire.force)))
}

/// Counts due pending flush jobs for executor spawn budgeting.
pub(crate) fn count_pending_flush_jobs() -> crate::error::PgResult<i64> {
    let statement =
        plan_count_pending_flush_jobs().map_err(crate::error::PgAdapterError::from_display)?;
    Ok(crate::spi::select_one::<i64>(&statement, &[])?.unwrap_or(0))
}

pub(super) fn ensure_flush_job(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> crate::error::PgResult<(uuid::Uuid, bool)> {
    use pgrx::datum::DatumWithOid;

    // Caller must hold the session table-job lock. Any durable `running` row
    // here has no live owner — reclaim to pending so uniqueness clears and the
    // same job can be resumed.
    reclaim_running_flush_jobs(table_oid)?;

    let lookup =
        plan_lookup_active_flush_job().map_err(crate::error::PgAdapterError::from_display)?;
    let existing = crate::spi::select_one::<String>(&lookup, &[DatumWithOid::from(table_oid)])?
        .filter(|value| !value.is_empty());
    if let Some(existing) = existing {
        let wire: PendingFlushJobWire = serde_json::from_str(&existing)?;
        return Ok((uuid::Uuid::parse_str(&wire.id)?, force || wire.force));
    }

    let job_id = uuid::Uuid::new_v4();
    let insert = plan_insert_flush_job().map_err(crate::error::PgAdapterError::from_display)?;
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

pub(crate) fn reclaim_running_flush_jobs(
    table_oid: pgrx::pg_sys::Oid,
) -> crate::error::PgResult<u64> {
    use pgrx::datum::DatumWithOid;

    let statement =
        plan_reclaim_running_flush_jobs().map_err(crate::error::PgAdapterError::from_display)?;
    let rows = crate::spi::update(&statement, &[DatumWithOid::from(table_oid)])?;
    Ok(rows.rows_affected)
}

/// Reclaims durable `running` flush jobs whose session table-job lock is free.
pub(crate) fn reclaim_orphan_running_flush_jobs() -> crate::error::PgResult<u64> {
    let statement =
        plan_list_running_flush_table_oids().map_err(crate::error::PgAdapterError::from_display)?;
    let json =
        crate::spi::select_one::<String>(&statement, &[])?.unwrap_or_else(|| "[]".to_string());
    let oids: Vec<i64> = serde_json::from_str(&json)?;
    let mut reclaimed = 0_u64;
    for oid_i64 in oids {
        let table_oid = pgrx::pg_sys::Oid::from(u32::try_from(oid_i64).unwrap_or(0));
        if table_oid == pgrx::pg_sys::InvalidOid {
            continue;
        }
        let Some(guard) = crate::sql::job_lock::TableJobLockGuard::try_lock(table_oid)? else {
            continue;
        };
        reclaimed = reclaimed.saturating_add(reclaim_running_flush_jobs(table_oid)?);
        guard.unlock();
    }
    Ok(reclaimed)
}

/// Marks a flush job running and returns the attempt token that fences mutations.
///
/// `target_seq` is the fixed job watermark (`None` / unset when no mirror rows).
pub(super) fn mark_flush_job_running(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
    progress_total: i64,
    target_seq: Option<SeqId>,
) -> crate::error::PgResult<uuid::Uuid> {
    use pgrx::datum::DatumWithOid;

    let attempt_token = uuid::Uuid::new_v4();
    let statement =
        plan_mark_flush_job_running().map_err(crate::error::PgAdapterError::from_display)?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(crate::spi::uuid_to_pgrx(attempt_token)),
            DatumWithOid::from(progress_total),
            DatumWithOid::from(target_seq.map(SeqId::get).unwrap_or(0)),
        ],
    )?;
    Ok(attempt_token)
}

/// Progress fields written to a flush job between phases/passes.
pub(super) struct FlushJobProgressUpdate<'a> {
    pub attempt_token: uuid::Uuid,
    pub rows_flushed: i64,
    pub batches_completed: i32,
    /// Last flushed seq watermark; `None` when unset (0 in catalog).
    pub checkpoint_seq: Option<SeqId>,
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

    let statement =
        plan_update_flush_job_progress().map_err(crate::error::PgAdapterError::from_display)?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(crate::spi::uuid_to_pgrx(progress.attempt_token)),
            DatumWithOid::from(progress.rows_flushed),
            DatumWithOid::from(progress.batches_completed),
            DatumWithOid::from(progress.checkpoint_seq.map(SeqId::get).unwrap_or(0)),
            DatumWithOid::from(progress.phase),
            DatumWithOid::from(progress.progress_total),
        ],
    )?;
    Ok(())
}

pub(super) fn mark_flush_job_completed(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
    attempt_token: uuid::Uuid,
    rows_flushed: i64,
    checkpoint_seq: Option<SeqId>,
    batches_completed: i32,
) -> crate::error::PgResult<()> {
    use pgrx::datum::DatumWithOid;

    let statement =
        plan_mark_flush_job_completed().map_err(crate::error::PgAdapterError::from_display)?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(crate::spi::uuid_to_pgrx(attempt_token)),
            DatumWithOid::from(rows_flushed),
            DatumWithOid::from(checkpoint_seq.map(SeqId::get).unwrap_or(0)),
            DatumWithOid::from(batches_completed),
        ],
    )?;
    clear_table_cancel_request(table_oid)?;
    Ok(())
}

pub(super) fn mark_flush_job_failed(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
    attempt_token: uuid::Uuid,
    error_trace: &str,
) -> crate::error::PgResult<()> {
    use pgrx::datum::DatumWithOid;

    let statement =
        plan_mark_flush_job_failed().map_err(crate::error::PgAdapterError::from_display)?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(crate::spi::uuid_to_pgrx(attempt_token)),
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
    attempt_token: uuid::Uuid,
) -> crate::error::PgResult<()> {
    use pgrx::datum::DatumWithOid;

    let statement =
        plan_mark_flush_job_cancelled().map_err(crate::error::PgAdapterError::from_display)?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(crate::spi::uuid_to_pgrx(attempt_token)),
        ],
    )?;
    clear_table_cancel_request(table_oid)?;
    Ok(())
}

pub(super) fn mark_flush_job_completed_after_cancel(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
    attempt_token: uuid::Uuid,
    rows_flushed: i64,
    checkpoint_seq: Option<SeqId>,
    batches_completed: i32,
) -> crate::error::PgResult<()> {
    use pgrx::datum::DatumWithOid;

    let statement = plan_mark_flush_job_completed_after_cancel()
        .map_err(crate::error::PgAdapterError::from_display)?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(crate::spi::uuid_to_pgrx(attempt_token)),
            DatumWithOid::from(rows_flushed),
            DatumWithOid::from(checkpoint_seq.map(SeqId::get).unwrap_or(0)),
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

/// Deletes a batch of aged terminal jobs. Returns deleted count.
///
/// Skips jobs still referenced by `pending` cold segments. Caller should pass
/// `retention_days > 0`; `batch_limit` is clamped to at least 1.
pub(crate) fn purge_old_jobs(retention_days: i32, batch_limit: i32) -> crate::error::PgResult<i64> {
    use pgrx::datum::DatumWithOid;

    if retention_days <= 0 {
        return Ok(0);
    }
    let batch_limit = batch_limit.max(1);
    let statement = plan_purge_old_jobs().map_err(crate::error::PgAdapterError::from_display)?;
    Ok(crate::spi::update_one::<i64>(
        &statement,
        &[
            DatumWithOid::from(retention_days),
            DatumWithOid::from(batch_limit),
        ],
    )?
    .unwrap_or(0))
}

/// Coordinator tick helper: purge using GUC retention and the default batch size.
pub(crate) fn purge_old_jobs_tick() -> crate::error::PgResult<i64> {
    let retention_days = crate::guc::job_retention_days();
    if retention_days <= 0 {
        return Ok(0);
    }
    purge_old_jobs(retention_days, DEFAULT_PURGE_BATCH_LIMIT)
}
