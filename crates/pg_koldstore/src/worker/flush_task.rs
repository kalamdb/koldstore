//! Lightweight database-local auto-flush scheduling.
//!
//! The ephemeral maintenance worker evaluates a bounded page of managed tables
//! and durably enqueues every due table in that page. Enqueue itself publishes
//! the post-commit flush generation; only the cluster supervisor may register
//! heavy one-shot flush executors.

use koldstore_common::ManageTableOptions;
use koldstore_flush::{plan_select_auto_flush_candidate_tables, scheduler_should_flush_parsed};

const AUTO_FLUSH_PAGE_LIMIT: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlushTickResult {
    pub had_due_table: bool,
    pub completed: bool,
}

fn select_due_auto_flush_tables() -> Result<(Vec<u32>, bool), String> {
    pgrx::Spi::connect(|client| -> Result<(Vec<u32>, bool), String> {
        let statement =
            plan_select_auto_flush_candidate_tables().map_err(|error| error.to_string())?;
        let table = client
            .select(&statement.sql, None, &[])
            .map_err(|error| error.to_string())?;

        let mut due_tables = Vec::new();
        let mut more_due = false;
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
            let (_, mirror_delta) = crate::row_counter_cache::pending_deltas(oid);
            let pending = catalog_pending.saturating_add(mirror_delta).max(0);
            let due = match parsed.flush_policy() {
                Some(koldstore_common::FlushPolicy::OlderThan { .. }) => {
                    crate::sql::flush::spi::resolve_flush_stats(oid, false)
                        .map(|selection| selection.stats.row_count > 0)?
                }
                _ => scheduler_should_flush_parsed(&parsed, pending),
            };
            if !due {
                continue;
            }
            if due_tables.len() >= AUTO_FLUSH_PAGE_LIMIT {
                more_due = true;
                break;
            }
            due_tables.push(oid.to_u32());
        }
        Ok((due_tables, more_due))
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

/// Evaluates auto-flush eligibility and durably enqueues all due tables in one
/// bounded page. Queue enqueue marks FLUSH_QUEUE_DIRTY transaction-locally, so
/// there is intentionally no worker-spawn or retry logic in this function.
pub(crate) fn run_flush_scheduler_tick() -> Result<FlushTickResult, String> {
    let (due_tables, more_due) = select_due_auto_flush_tables()?;
    let mut had_due_table = false;
    let mut completed = false;

    for table_oid in due_tables {
        let oid = pgrx::pg_sys::Oid::from(table_oid);
        let Some(job_id) = crate::sql::flush::jobs::enqueue_flush_job_if_due(oid, false)
            .map_err(|error| error.to_string())?
        else {
            continue;
        };
        had_due_table = true;
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
        crate::worker::wake::mark_schedule_pending();
    }

    Ok(FlushTickResult {
        had_due_table,
        completed,
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
