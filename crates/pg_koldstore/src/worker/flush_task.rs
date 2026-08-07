//! Lightweight database-local auto-flush scheduling.
//!
//! RowLimit is scheduled directly from WAL-applied counter bumps for only the
//! tables that changed. Broad scans are reserved for explicit configuration /
//! recovery reconciliation and time policies. Enqueue publishes a post-commit
//! flush generation; only the cluster supervisor may register heavy executors.

use std::time::{SystemTime, UNIX_EPOCH};

use koldstore_common::{FlushPolicy, ManageTableOptions};
use koldstore_flush::{plan_select_auto_flush_candidate_tables, scheduler_should_flush_parsed};

const AUTO_FLUSH_PAGE_LIMIT: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlushTickResult {
    pub completed: bool,
    /// Exact earliest future `OlderThan` wake for the current database.
    /// `None` means time alone cannot make any current table flushable.
    pub next_timed_wake_at_ms: Option<i64>,
}

/// Evaluates one RowLimit table using the mirror count already produced by the
/// WAL-apply counter bump and durably enqueues the job in the same transaction.
///
/// This is intentionally O(1) in database table count. It is called once per
/// touched table, not once per managed table, and bypasses
/// `enqueue_flush_job_if_due` because eligibility is already known from the
/// post-bump counter. That avoids a duplicate progress/stat selection.
pub(crate) fn schedule_row_limit_after_counter(
    table_oid: pgrx::pg_sys::Oid,
    mirror_row_count: i64,
) -> Result<bool, String> {
    let Some(options) = crate::sql::flush::spi::active_manage_options(table_oid)? else {
        return Ok(false);
    };
    if !options.auto_flush_enabled() || !options.flush_enabled() {
        return Ok(false);
    }
    if !matches!(options.flush_policy(), Some(FlushPolicy::RowLimit { .. })) {
        return Ok(false);
    }
    if !scheduler_should_flush_parsed(&options, mirror_row_count.max(0)) {
        return Ok(false);
    }

    let job_id = crate::sql::flush::jobs::enqueue_or_lookup_flush_job(table_oid, false)
        .map_err(|error| error.to_string())?;
    // Transaction-local dirty tracking publishes only if the apply transaction
    // commits. Therefore counter bump + durable job + supervisor hint have no
    // crash window where the counter commits but scheduling is forgotten.
    crate::worker::wake::mark_flush_queue_pending();
    pgrx::log!(
        "koldstore auto-flush: touched RowLimit table_oid={} enqueued job={} mirror_rows={}",
        table_oid.to_u32(),
        crate::spi::uuid_from_pgrx(job_id),
        mirror_row_count
    );
    Ok(true)
}

fn select_due_auto_flush_tables() -> Result<(Vec<u32>, bool, Option<i64>), String> {
    pgrx::Spi::connect(|client| -> Result<(Vec<u32>, bool, Option<i64>), String> {
        let statement =
            plan_select_auto_flush_candidate_tables().map_err(|error| error.to_string())?;
        let table = client
            .select(&statement.sql, None, &[])
            .map_err(|error| error.to_string())?;

        let now_ms = unix_now_ms();
        let mut due_tables = Vec::with_capacity(AUTO_FLUSH_PAGE_LIMIT);
        let mut more_due = false;
        let mut next_timed_wake_at_ms: Option<i64> = None;
        for row in table {
            let oid: pgrx::pg_sys::Oid = row
                .get(1)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "missing table_oid".to_string())?;
            let options_text: String = row
                .get(2)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "missing options".to_string())?;
            let catalog_pending: i64 = row.get(3).map_err(|error| error.to_string())?.unwrap_or(0);
            let parsed = ManageTableOptions::from_json_str(&options_text);
            let policy = parsed.flush_policy();
            let (_, mirror_delta) = crate::row_counter_cache::pending_deltas(oid);
            let pending = catalog_pending.saturating_add(mirror_delta).max(0);
            let due = match policy.as_ref() {
                Some(FlushPolicy::OlderThan { .. }) => {
                    crate::sql::flush::spi::resolve_flush_stats(oid, false)
                        .map(|selection| selection.stats.row_count > 0)?
                }
                _ => scheduler_should_flush_parsed(&parsed, pending),
            };

            if due {
                if due_tables.len() >= AUTO_FLUSH_PAGE_LIMIT {
                    more_due = true;
                    break;
                }
                due_tables.push(oid.to_u32());
                continue;
            }

            let Some(policy @ FlushPolicy::OlderThan { .. }) = policy.as_ref() else {
                continue;
            };
            let Some(deadline_ms) =
                super::timed_policy::next_older_than_due_at_ms(oid, policy)?
            else {
                continue;
            };
            // A threshold already in the past but still not flushable means the
            // row-count/file-size floor is not satisfied by the current mirror;
            // time cannot fix it. Future DML will wake maintenance instead of a
            // zero-delay supervisor spin.
            if deadline_ms <= now_ms {
                continue;
            }
            next_timed_wake_at_ms = Some(
                next_timed_wake_at_ms
                    .map(|current| current.min(deadline_ms))
                    .unwrap_or(deadline_ms),
            );
        }
        Ok((due_tables, more_due, next_timed_wake_at_ms))
    })
}

/// Explicit diagnostic hook used by in-server tests. Production recovery is
/// owned by the maintenance worker, not hidden inside ordinary queue enqueue.
#[pgrx::pg_extern(
    name = "internal_run_flush_scheduler_tick",
    schema = "koldstore",
    security_definer
)]
pub fn run_flush_scheduler_tick_pg() -> bool {
    let reclaimed = crate::sql::flush::jobs::reclaim_orphan_running_flush_jobs()
        .unwrap_or_else(|error| pgrx::error!("flush recovery tick failed: {error}"));
    if reclaimed > 0 {
        pgrx::log!("koldstore diagnostic scheduler reclaimed {reclaimed} orphan job(s)");
    }
    run_flush_scheduler_tick()
        .map(|result| result.completed)
        .unwrap_or_else(|error| pgrx::error!("flush scheduler tick failed: {error}"))
}

/// Broad reconciliation used for configuration changes/startup/recovery.
/// Normal WAL RowLimit scheduling does not come through this scan.
pub(crate) fn run_flush_scheduler_tick() -> Result<FlushTickResult, String> {
    let (due_tables, more_due, next_timed_wake_at_ms) = select_due_auto_flush_tables()?;
    let mut completed = false;

    for table_oid in due_tables {
        let oid = pgrx::pg_sys::Oid::from(table_oid);
        let Some(job_id) = crate::sql::flush::jobs::enqueue_flush_job_if_due(oid, false)
            .map_err(|error| error.to_string())?
        else {
            continue;
        };
        pgrx::log!(
            "koldstore auto-flush: enqueued table_oid={} job={}",
            table_oid,
            crate::spi::uuid_from_pgrx(job_id)
        );

        if crate::guc::flush_execution_mode() == crate::settings::FlushExecutionMode::Inline {
            let Some(guard) = crate::sql::job_lock::TableJobLockGuard::try_lock(oid)? else {
                pgrx::log!(
                    "koldstore auto-flush: table_oid={} already owned; leaving durable job queued",
                    table_oid
                );
                continue;
            };
            let completed_job =
                crate::sql::flush::execute::flush_table_with_session_lock(oid, false, guard)?;
            completed |= flush_job_completed(completed_job)?;
        }
    }

    if more_due {
        // Another bounded page is already due. Publish a new maintenance
        // generation instead of keeping this worker alive scanning indefinitely.
        crate::worker::wake::mark_schedule_pending();
    }

    Ok(FlushTickResult {
        completed,
        next_timed_wake_at_ms,
    })
}

fn flush_job_completed(job_id: pgrx::Uuid) -> Result<bool, String> {
    use pgrx::datum::DatumWithOid;

    let statement =
        koldstore_flush::plan_flush_job_is_completed().map_err(|error| error.to_string())?;
    crate::spi::select_one::<bool>(&statement, &[DatumWithOid::from(job_id)])
        .map(|value| value.unwrap_or(false))
        .map_err(|error| error.to_string())
}

fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
