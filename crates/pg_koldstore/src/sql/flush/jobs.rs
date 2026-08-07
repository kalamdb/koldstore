//! Flush job lifecycle SPI adapters.

use koldstore_common::{SeqId, TableName};
use koldstore_flush::{
    flush_table_request, plan_cancel_jobs_for_drop, plan_clear_table_cancel_request,
    plan_enqueue_or_lookup_flush_job, plan_flush_cancel_requested, plan_insert_flush_job,
    plan_list_jobs, plan_mark_flush_job_cancelled, plan_mark_flush_job_completed,
    plan_mark_flush_job_completed_after_cancel, plan_mark_flush_job_failed,
    plan_mark_flush_job_running, plan_purge_old_jobs, plan_reclaim_running_flush_jobs,
    plan_request_cancel_job, plan_request_cancel_table_jobs, plan_update_flush_job_progress,
};

const ORPHAN_RECOVERY_PAGE: i64 = 64;

#[derive(Debug, Clone, Copy)]
struct ActiveFlushJob {
    id: uuid::Uuid,
    force: bool,
    running: bool,
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

/// Enqueues only when flush work is due (or an active job already exists).
///
/// Enqueue has no hidden recovery or worker-registration side effects. The job
/// row is the durable request; a transaction-local queue dirty bit wakes the
/// cluster supervisor only after this transaction commits. Rollback publishes
/// nothing. Crash/orphan reclamation belongs to DB maintenance recovery.
pub(crate) fn enqueue_flush_job_if_due(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> crate::error::PgResult<Option<pgrx::Uuid>> {
    let job_id = if let Some(existing) = lookup_active_flush_job(table_oid)? {
        if force && !existing.force {
            // Upgrade pending force intent while preserving the same active UUID.
            Some(enqueue_or_lookup_flush_job(table_oid, true)?)
        } else {
            Some(crate::spi::uuid_to_pgrx(existing.id))
        }
    } else {
        if !force {
            let estimate = super::spi::flush_progress_total_estimate(table_oid, false)
                .map_err(crate::error::PgAdapterError::from_display)?;
            if estimate <= 0 {
                return Ok(None);
            }
        }
        Some(enqueue_or_lookup_flush_job(table_oid, force)?)
    };

    if job_id.is_some() {
        crate::worker::wake::mark_flush_queue_pending();
    }
    Ok(job_id)
}

/// Looks up a committed active flush job without inserting.
///
/// Keep this path typed end-to-end. It runs for ordinary queue enqueue/claim, so
/// building JSON in PostgreSQL, converting it to text, parsing JSON in Rust, and
/// then parsing the UUID is unnecessary allocator/CPU work.
fn lookup_active_flush_job(
    table_oid: pgrx::pg_sys::Oid,
) -> crate::error::PgResult<Option<ActiveFlushJob>> {
    use pgrx::datum::DatumWithOid;

    pgrx::Spi::connect(|client| {
        let table = client
            .select(
                r#"
SELECT id,
       COALESCE((payload->>'force')::boolean, false) AS force,
       status = 'running' AS running
FROM koldstore.jobs
WHERE table_oid = $1::oid
  AND scope_key = ''
  AND job_type = 'flush'
  AND status IN ('pending', 'running')
ORDER BY updated_at, id
LIMIT 1
"#,
                Some(1),
                &[DatumWithOid::from(table_oid)],
            )
            .map_err(crate::error::PgAdapterError::from_display)?;
        let Some(row) = table.first().get::<pgrx::Uuid>(1).map_err(
            crate::error::PgAdapterError::from_display,
        )? else {
            return Ok(None);
        };
        let first = table.first();
        let force = first
            .get::<bool>(2)
            .map_err(crate::error::PgAdapterError::from_display)?
            .unwrap_or(false);
        let running = first
            .get::<bool>(3)
            .map_err(crate::error::PgAdapterError::from_display)?
            .unwrap_or(false);
        Ok(Some(ActiveFlushJob {
            id: crate::spi::uuid_from_pgrx(row),
            force,
            running,
        }))
    })
}

/// Looks up a committed active flush job UUID without inserting.
pub(crate) fn lookup_active_flush_job_uuid(
    table_oid: pgrx::pg_sys::Oid,
) -> crate::error::PgResult<Option<pgrx::Uuid>> {
    Ok(lookup_active_flush_job(table_oid)?.map(|job| crate::spi::uuid_to_pgrx(job.id)))
}

/// Returns whether any due pending flush job exists. Recovery only needs a
/// boolean dispatch decision; avoid counting the entire due queue.
pub(crate) fn has_due_pending_flush_jobs() -> crate::error::PgResult<bool> {
    Ok(pgrx::Spi::get_one::<bool>(
        "SELECT EXISTS (\
           SELECT 1 FROM koldstore.jobs \
           WHERE job_type = 'flush' \
             AND status = 'pending' \
             AND available_at <= clock_timestamp()\
         )",
    )
    .map_err(crate::error::PgAdapterError::from_display)?
    .unwrap_or(false))
}

pub(super) fn ensure_flush_job(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> crate::error::PgResult<(uuid::Uuid, bool)> {
    use pgrx::datum::DatumWithOid;

    // The caller owns the session table-job lock. A `running` row therefore has
    // no live executor owner and may be returned to pending. The overwhelmingly
    // common `pending` path performs no reclaim write at all.
    if let Some(existing) = lookup_active_flush_job(table_oid)? {
        if existing.running {
            reclaim_running_flush_jobs(table_oid)?;
        }
        return Ok((existing.id, force || existing.force));
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

/// Reclaims a bounded page of durable `running` jobs whose session table lock is free.
pub(crate) fn reclaim_orphan_running_flush_jobs() -> crate::error::PgResult<u64> {
    use pgrx::datum::DatumWithOid;

    let oids = pgrx::Spi::connect(|client| -> Result<Vec<pgrx::pg_sys::Oid>, String> {
        let table = client
            .select(
                "SELECT DISTINCT table_oid::oid \
                 FROM koldstore.jobs \
                 WHERE job_type = 'flush' AND status = 'running' \
                 ORDER BY table_oid \
                 LIMIT $1",
                // The SQL LIMIT is the recovery bound. Some(1) here previously
                // reduced every recovery pass to a single orphan table.
                None,
                &[DatumWithOid::from(ORPHAN_RECOVERY_PAGE)],
            )
            .map_err(|error| error.to_string())?;
        let mut out = Vec::with_capacity(ORPHAN_RECOVERY_PAGE as usize);
        for row in table {
            if let Some(oid) = row
                .get::<pgrx::pg_sys::Oid>(1)
                .map_err(|error| error.to_string())?
            {
                out.push(oid);
            }
        }
        Ok(out)
    })
    .map_err(crate::error::PgAdapterError::from_display)?;

    let mut reclaimed = 0_u64;
    for table_oid in oids {
        let Some(guard) = crate::sql::job_lock::TableJobLockGuard::try_lock(table_oid)? else {
            continue;
        };
        reclaimed = reclaimed.saturating_add(reclaim_running_flush_jobs(table_oid)?);
        guard.unlock();
    }
    Ok(reclaimed)
}

/// Marks a flush job running and returns the attempt token that fences mutations.
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
    let updated = crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(crate::spi::uuid_to_pgrx(attempt_token)),
            DatumWithOid::from(progress_total),
            DatumWithOid::from(target_seq.map(SeqId::get).unwrap_or(0)),
        ],
    )?;
    require_attempt_update("claim running", updated.rows_affected)?;
    Ok(attempt_token)
}

/// Progress fields written to a flush job between phases/passes.
pub(super) struct FlushJobProgressUpdate<'a> {
    pub attempt_token: uuid::Uuid,
    pub rows_flushed: i64,
    pub batches_completed: i32,
    pub checkpoint_seq: Option<SeqId>,
    pub phase: &'a str,
    pub progress_total: i64,
}

/// Persists mid-flush progress for operator visibility.
pub(super) fn update_flush_job_progress(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
    progress: FlushJobProgressUpdate<'_>,
) -> crate::error::PgResult<()> {
    use pgrx::datum::DatumWithOid;

    let statement =
        plan_update_flush_job_progress().map_err(crate::error::PgAdapterError::from_display)?;
    let updated = crate::spi::update(
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
    require_attempt_update("progress", updated.rows_affected)
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
    let updated = crate::spi::update(
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
    require_attempt_update("complete", updated.rows_affected)?;
    clear_table_cancel_request(table_oid)
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
    let updated = crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(crate::spi::uuid_to_pgrx(attempt_token)),
            DatumWithOid::from(error_trace),
        ],
    )?;
    require_attempt_update("fail", updated.rows_affected)?;
    clear_table_cancel_request(table_oid)
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
    let updated = crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(crate::spi::uuid_to_pgrx(attempt_token)),
        ],
    )?;
    require_attempt_update("cancel", updated.rows_affected)?;
    clear_table_cancel_request(table_oid)
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
    let updated = crate::spi::update(
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
    require_attempt_update("complete after cancel", updated.rows_affected)?;
    clear_table_cancel_request(table_oid)
}

fn require_attempt_update(operation: &str, rows_affected: u64) -> crate::error::PgResult<()> {
    if rows_affected == 1 {
        return Ok(());
    }
    Err(crate::error::PgAdapterError::from_display(format!(
        "flush job attempt lost ownership during {operation}: expected 1 fenced row, affected {rows_affected}"
    )))
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

/// Requests cancel for all active jobs on one table. Returns affected row count.
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
