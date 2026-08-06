//! Flush scheduler for the shared database worker loop.
//!
//! Hot path: reclaim orphans, enqueue at most one due auto-flush job, then spawn
//! one-shot flush executors up to `koldstore.max_parallel_flush_jobs`. The
//! coordinator never runs Parquet encode/upload itself.

use koldstore_common::ManageTableOptions;
use koldstore_flush::{
    plan_database_has_auto_flush_tables, plan_select_auto_flush_candidate_tables,
    scheduler_should_flush_parsed,
};

/// Outcome of one built-in flush-scheduler evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlushTickResult {
    /// True when a due auto-flush table was selected (worker should stay alive).
    pub had_due_table: bool,
    /// True when a flush job finished as `completed` in this tick (inline only).
    pub completed: bool,
}

/// Selects the first due auto-flush table, if any.
///
/// Stops scanning as soon as one candidate passes policy (at most one enqueue
/// per tick). Skips tables with an active `running` flush job and cools down
/// recent `error` jobs for 60 seconds.
fn select_first_due_auto_flush_table() -> Result<Option<u32>, String> {
    pgrx::Spi::connect(|client| -> Result<Option<u32>, String> {
        let statement =
            plan_select_auto_flush_candidate_tables().map_err(|error| error.to_string())?;
        let table = client
            .select(&statement.sql, None, &[])
            .map_err(|error| error.to_string())?;

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
            if due {
                return Ok(Some(oid.to_u32()));
            }
        }
        Ok(None)
    })
}

/// Returns whether this database still needs a database worker for auto-flush.
pub(crate) fn database_has_auto_flush_tables() -> Result<bool, String> {
    let statement = plan_database_has_auto_flush_tables().map_err(|error| error.to_string())?;
    pgrx::Spi::get_one::<bool>(&statement.sql)
        .map_err(|error| error.to_string())
        .map(|value| value.unwrap_or(false))
}

/// Runs one flush-scheduler tick in the current backend (tests / diagnostics).
///
/// SQL contract: `koldstore.internal_run_flush_scheduler_tick() → boolean`
/// (`true` when a flush job completed inline; normally `false` in queue mode).
#[pgrx::pg_extern(
    name = "internal_run_flush_scheduler_tick",
    schema = "koldstore",
    security_definer
)]
pub fn run_flush_scheduler_tick_pg() -> bool {
    run_flush_scheduler_tick()
        .map(|result| result.completed)
        .unwrap_or_else(|error| pgrx::error!("flush scheduler tick failed: {error}"))
}

/// Evaluates auto-flush eligibility, enqueues at most one job, and runs or spawns work.
///
/// Production (`flush_execution=queue`): enqueue + spawn one-shot executors.
/// Test/SPI (`flush_execution=inline`): enqueue then run flush in this backend.
pub(crate) fn run_flush_scheduler_tick() -> Result<FlushTickResult, String> {
    // Clear durable `running` rows left without an owner so auto-flush is not
    // permanently blocked; reclaim resumes the same durable job as pending.
    let reclaimed = crate::sql::flush::jobs::reclaim_orphan_running_flush_jobs()?;
    if reclaimed > 0 {
        pgrx::log!("koldstore flush scheduler: reclaimed {reclaimed} stuck running flush job(s)");
    }

    // Bounded retention: drop aged terminal jobs (never pending-segment owners).
    match crate::sql::flush::jobs::purge_old_jobs_tick() {
        Ok(purged) if purged > 0 => {
            pgrx::log!("koldstore flush scheduler: purged {purged} aged terminal job(s)");
        }
        Ok(_) => {}
        Err(error) => {
            pgrx::log!("koldstore flush scheduler: job retention purge failed: {error}");
        }
    }

    let mut had_due_table = false;
    let mut completed = false;

    if let Some(table_oid) = select_first_due_auto_flush_table()? {
        had_due_table = true;
        let oid = pgrx::pg_sys::Oid::from(table_oid);
        let job_id = crate::sql::flush::jobs::enqueue_flush_job_if_due(oid, false)
            .map_err(|error| error.to_string())?;
        let Some(job_id) = job_id else {
            pgrx::log!(
                "koldstore flush scheduler: skip table_oid={} (no due work)",
                table_oid
            );
            return Ok(FlushTickResult {
                had_due_table: true,
                completed: false,
            });
        };
        pgrx::log!(
            "koldstore flush scheduler: enqueued table_oid={} job={}",
            table_oid,
            crate::spi::uuid_from_pgrx(job_id)
        );

        if crate::guc::flush_execution_mode() == crate::settings::FlushExecutionMode::Inline {
            // Non-blocking: a mid-flight flush owns the lock.
            let Some(guard) = crate::sql::job_lock::TableJobLockGuard::try_lock(oid)? else {
                pgrx::log!(
                    "koldstore flush scheduler: skipping table_oid={} (flush already running)",
                    table_oid
                );
                return Ok(FlushTickResult {
                    had_due_table: true,
                    completed: false,
                });
            };
            let job_id =
                crate::sql::flush::execute::flush_table_with_session_lock(oid, false, guard)?;
            completed = flush_job_completed(job_id)?;
        }
    }

    if crate::guc::flush_execution_mode() == crate::settings::FlushExecutionMode::Queue {
        let spawned = super::spawn_flush_executors_for_pending_work()?;
        if spawned > 0 {
            pgrx::log!("koldstore flush scheduler: spawned {spawned} flush executor(s)");
            had_due_table = true;
        }
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
