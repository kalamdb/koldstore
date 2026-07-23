//! Flush scheduler for the shared database worker loop.
//!
//! Hot path: one catalog scan that stops at the first due table, then
//! `flush_table` (which ensures/claims the job). No separate enqueue SPI.
//!
//! Concurrent ticks never wait on an in-flight flush: if the table job lock is
//! held (or a durable `running` flush job exists), the tick is skipped.

use koldstore_common::ManageTableOptions;
use koldstore_flush::scheduler_should_flush_parsed;
use pgrx::datum::DatumWithOid;
use serde_json::Value;

/// Shared catalog predicates for managed tables the built-in scheduler may flush.
const AUTO_FLUSH_TABLE_PREDICATE: &str = r#"
s.active
  AND (
    COALESCE((s.options->>'hot_row_limit')::bigint, 0) > 0
    OR s.options->'flush_policy'->>'type' IN ('row_limit', 'older_than')
  )
  AND COALESCE((s.options->>'auto_flush')::boolean, true)
"#;

/// Outcome of one built-in flush-scheduler evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlushTickResult {
    /// True when a due auto-flush table was selected (worker should stay alive).
    pub had_due_table: bool,
    /// True when the flush job finished as `completed`.
    pub completed: bool,
    /// True when a due table was selected but skipped because a flush is already
    /// running (advisory lock held or durable `running` job).
    pub skipped_busy: bool,
}

/// Selects the first due auto-flush table, if any.
///
/// Stops scanning as soon as one candidate passes policy (at most one flush
/// per tick). Skips tables with an active `running` flush job and cools down
/// recent `error` jobs for 60 seconds.
fn select_first_due_auto_flush_table() -> Result<Option<u32>, String> {
    pgrx::Spi::connect(|client| -> Result<Option<u32>, String> {
        let sql = format!(
            r#"
SELECT s.table_oid::oid,
       COALESCE(s.options, '{{}}'::jsonb)::text,
       COALESCE(m.mirror_row_count, 0)::bigint
FROM koldstore.schemas s
LEFT JOIN koldstore.manifest m
  ON m.table_oid = s.table_oid
 AND m.scope_key = ''
WHERE {AUTO_FLUSH_TABLE_PREDICATE}
  AND NOT EXISTS (
        SELECT 1
        FROM koldstore.jobs j
        WHERE j.table_oid = s.table_oid
          AND j.job_type = 'flush'
          AND j.status = 'running'
      )
  AND NOT EXISTS (
        SELECT 1
        FROM koldstore.jobs j
        WHERE j.table_oid = s.table_oid
          AND j.job_type = 'flush'
          AND j.status = 'error'
          AND j.updated_at > now() - interval '60 seconds'
      )
ORDER BY s.created_at DESC, s.table_oid DESC
"#
        );
        let table = client
            .select(&sql, None, &[])
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
            let options: Value = serde_json::from_str(&options_text)
                .unwrap_or_else(|_| Value::Object(Default::default()));
            let parsed = ManageTableOptions::from_value(&options);
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
    let sql = format!(
        r#"
SELECT EXISTS (
    SELECT 1
    FROM koldstore.schemas s
    WHERE {AUTO_FLUSH_TABLE_PREDICATE}
)
"#
    );
    pgrx::Spi::get_one::<bool>(&sql)
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
            skipped_busy: false,
        });
    };
    let oid = pgrx::pg_sys::Oid::from(table_oid);
    // Non-blocking: a mid-flight flush (worker or manual) owns the lock.
    if !crate::sql::job_lock_pg::try_lock_table_job(oid)? {
        pgrx::log!(
            "koldstore flush scheduler: skipping table_oid={} (flush already running)",
            table_oid
        );
        return Ok(FlushTickResult {
            had_due_table: true,
            completed: false,
            skipped_busy: true,
        });
    }
    // `flush_table` re-acquires the same xact lock (reentrant) and ensures/claims
    // the job; no separate enqueue SPI.
    let job_id = crate::sql::flush::execute::flush_table_pg_impl(oid, false)?;
    Ok(FlushTickResult {
        had_due_table: true,
        completed: flush_job_completed(job_id)?,
        skipped_busy: false,
    })
}
