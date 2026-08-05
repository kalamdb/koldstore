//! Flush scheduler for the shared database worker loop.
//!
//! Hot path: one catalog scan that stops at the first due table, then
//! `flush_table` (which ensures/claims the job). No separate enqueue SPI.
//!
//! Concurrent ticks never wait on an in-flight flush: if the table job lock is
//! held (or a durable `running` flush job exists), the tick is skipped.

use koldstore_common::ManageTableOptions;
use koldstore_flush::{
    plan_database_has_auto_flush_tables, plan_select_auto_flush_candidate_tables,
    scheduler_should_flush_parsed,
};
use pgrx::datum::DatumWithOid;

/// Outcome of one built-in flush-scheduler evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlushTickResult {
    /// True when a due auto-flush table was selected (worker should stay alive).
    pub had_due_table: bool,
    /// True when the flush job finished as `completed`.
    pub completed: bool,
}

/// Selects the first due auto-flush table, if any.
///
/// Stops scanning as soon as one candidate passes policy (at most one flush
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

fn flush_job_completed(job_id: pgrx::Uuid) -> Result<bool, String> {
    pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (\
           SELECT 1 FROM koldstore.jobs \
           WHERE id = $1::uuid AND status = 'completed'\
         )",
        &[DatumWithOid::from(job_id)],
    )
    .map_err(|error| error.to_string())
    .map(|value| value.unwrap_or(false))
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
/// (`true` when a flush job completed).
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

/// Evaluates auto-flush eligibility and runs at most one `flush_table`.
///
/// If another backend already holds the table flush lock, this tick is skipped
/// immediately (no wait, no second concurrent flush).
pub(crate) fn run_flush_scheduler_tick() -> Result<FlushTickResult, String> {
    // Clear durable `running` rows left without an owner so auto-flush is not
    // permanently blocked (Phase D crash hygiene; no lease claimer).
    let abandoned = crate::sql::flush::jobs::reclaim_orphan_running_flush_jobs()?;
    if abandoned > 0 {
        pgrx::log!("koldstore flush scheduler: abandoned {abandoned} stuck running flush job(s)");
    }

    let Some(table_oid) = select_first_due_auto_flush_table()? else {
        return Ok(FlushTickResult {
            had_due_table: false,
            completed: false,
        });
    };
    let oid = pgrx::pg_sys::Oid::from(table_oid);
    // Non-blocking: a mid-flight flush (worker or manual) owns the lock.
    if !crate::sql::job_lock::try_lock_table_job(oid)? {
        pgrx::log!(
            "koldstore flush scheduler: skipping table_oid={} (flush already running)",
            table_oid
        );
        return Ok(FlushTickResult {
            had_due_table: true,
            completed: false,
        });
    }
    // `flush_table` re-acquires the same xact lock (reentrant) and ensures/claims
    // the job; no separate enqueue SPI.
    let job_id = crate::sql::flush::execute::flush_table_pg_impl(oid, false)?;
    Ok(FlushTickResult {
        had_due_table: true,
        completed: flush_job_completed(job_id)?,
    })
}
