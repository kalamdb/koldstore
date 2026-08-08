//! Lightweight database-local auto-flush scheduling.
//!
//! Normal WAL apply evaluates policy only for tables whose counters changed:
//! RowLimit uses the post-bump mirror count and OlderThan uses one bounded seq
//! scan that either enqueues now or publishes an exact post-commit deadline.
//! Broad catalog scans are reserved for explicit configuration/startup/recovery
//! reconciliation. Only the cluster supervisor registers heavy executors.
//!
//! `OlderThan` interval arithmetic stays here (SPI + `make_interval`); pure
//! due/next-due classification lives in `koldstore-flush`.

use koldstore_common::{
    minimum_id_at_unix_millis, unix_millis_from_id, unix_now_ms, FlushPolicy, ManageTableOptions,
    MoveAfter,
};
use koldstore_flush::{
    evaluate_older_than_scan, plan_select_auto_flush_candidate_tables,
    scheduler_should_flush_parsed, OlderThanEvaluation,
};
use pgrx::datum::DatumWithOid;

const AUTO_FLUSH_SCAN_LIMIT: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlushTickResult {
    pub completed: bool,
    /// Exact earliest future `OlderThan` wake for the current database.
    /// `None` means time alone cannot make any current table flushable.
    pub next_timed_wake_at_ms: Option<i64>,
}

/// Evaluates the auto-flush policy for one WAL-touched table in the same
/// transaction that persisted its post-apply counters.
///
/// RowLimit is O(1) using `mirror_row_count`. OlderThan performs one bounded
/// index walk and either enqueues immediately or records an exact transaction-
/// local deadline. The deadline reaches shared memory only after COMMIT, so an
/// aborted WAL-apply transaction cannot leave a false clock wake behind.
pub(crate) fn schedule_policy_after_counter(
    table_oid: pgrx::pg_sys::Oid,
    mirror_row_count: i64,
) -> Result<bool, String> {
    let Some(options) = crate::sql::flush::spi::active_manage_options(table_oid)? else {
        return Ok(false);
    };
    if !options.auto_flush_enabled() || !options.flush_enabled() {
        return Ok(false);
    }
    let Some(policy) = options.flush_policy() else {
        return Ok(false);
    };

    let due = match &policy {
        FlushPolicy::RowLimit { .. } => {
            scheduler_should_flush_parsed(&options, mirror_row_count.max(0))
        }
        FlushPolicy::OlderThan { .. } => {
            let evaluation = evaluate_older_than(table_oid, &policy)?;
            if let Some(deadline_ms) = evaluation
                .next_due_at_ms
                .filter(|deadline_ms| *deadline_ms > unix_now_ms())
            {
                crate::worker::wake::mark_maintenance_deadline_pending(deadline_ms);
            }
            evaluation.due
        }
        // Reserved policy; execution fails closed elsewhere — never auto-enqueue.
        FlushPolicy::Filter { .. } => false,
    };
    if !due {
        return Ok(false);
    }

    // Eligibility has already been proven from the same transaction's state;
    // avoid `enqueue_flush_job_if_due`, which would repeat progress/stat work.
    let job_id = crate::sql::flush::jobs::enqueue_or_lookup_flush_job(table_oid, false)
        .map_err(|error| error.to_string())?;
    crate::worker::wake::mark_flush_queue_pending();
    pgrx::log!(
        "koldstore auto-flush: touched table_oid={} enqueued job={} mirror_rows={}",
        table_oid.to_u32(),
        crate::spi::uuid_from_pgrx(job_id),
        mirror_row_count
    );
    Ok(true)
}

fn select_due_auto_flush_tables() -> Result<(Option<u32>, bool, Option<i64>), String> {
    pgrx::Spi::connect(
        |client| -> Result<(Option<u32>, bool, Option<i64>), String> {
            let statement =
                plan_select_auto_flush_candidate_tables().map_err(|error| error.to_string())?;
            let table = client
                .select(&statement.sql, None, &[])
                .map_err(|error| error.to_string())?;

            let now_ms = unix_now_ms();
            let mut selected: Option<u32> = None;
            let mut more_due = false;
            let mut next_timed_wake_at_ms: Option<i64> = None;
            let mut scanned = 0_usize;
            for row in table {
                if scanned >= AUTO_FLUSH_SCAN_LIMIT {
                    // Unscanned candidates may still be due; ask for another tick.
                    more_due = true;
                    break;
                }
                scanned = scanned.saturating_add(1);

                let oid: pgrx::pg_sys::Oid = row
                    .get(1)
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "missing table_oid".to_string())?;
                let options_text: String = row
                    .get(2)
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "missing options".to_string())?;
                let catalog_pending: i64 =
                    row.get(3).map_err(|error| error.to_string())?.unwrap_or(0);
                let parsed = ManageTableOptions::from_json_str(&options_text);
                let policy = parsed.flush_policy();
                let (_, mirror_delta) = crate::row_counter_cache::pending_deltas(oid);
                let pending = catalog_pending.saturating_add(mirror_delta).max(0);

                let due = match policy.as_ref() {
                    Some(policy @ FlushPolicy::OlderThan { .. }) => {
                        let evaluation = evaluate_older_than(oid, policy)?;
                        if let Some(deadline_ms) = evaluation
                            .next_due_at_ms
                            .filter(|deadline_ms| *deadline_ms > now_ms)
                        {
                            next_timed_wake_at_ms = Some(
                                next_timed_wake_at_ms
                                    .map(|current| current.min(deadline_ms))
                                    .unwrap_or(deadline_ms),
                            );
                        }
                        evaluation.due
                    }
                    Some(FlushPolicy::RowLimit { .. }) => {
                        scheduler_should_flush_parsed(&parsed, pending)
                    }
                    _ => false,
                };

                if due {
                    // Contract: enqueue at most one auto-flush job per scheduler tick.
                    // Continue scanning so OlderThan deadlines and `more_due` stay accurate.
                    if selected.is_none() {
                        selected = Some(oid.to_u32());
                    } else {
                        more_due = true;
                    }
                }
            }
            Ok((selected, more_due, next_timed_wake_at_ms))
        },
    )
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

/// Broad reconciliation used only when policy/configuration or recovery state
/// may have changed without a fresh WAL counter bump.
///
/// Enqueues at most one auto-flush job per tick (docs/architecture contract).
/// Remaining due tables publish a schedule-dirty wake for a later tick.
pub(crate) fn run_flush_scheduler_tick() -> Result<FlushTickResult, String> {
    let (selected, more_due, next_timed_wake_at_ms) = select_due_auto_flush_tables()?;
    let mut completed = false;

    if let Some(table_oid) = selected {
        let oid = pgrx::pg_sys::Oid::from(table_oid);
        if let Some(job_id) = crate::sql::flush::jobs::enqueue_flush_job_if_due(oid, false)
            .map_err(|error| error.to_string())?
        {
            pgrx::log!(
                "koldstore auto-flush reconciliation: enqueued table_oid={} job={}",
                table_oid,
                crate::spi::uuid_from_pgrx(job_id)
            );

            if crate::guc::flush_execution_mode() == crate::settings::FlushExecutionMode::Inline {
                if let Some(guard) = crate::sql::job_lock::TableJobLockGuard::try_lock(oid)? {
                    let completed_job = crate::sql::flush::execute::flush_table_with_session_lock(
                        oid, false, guard,
                    )?;
                    completed = flush_job_completed(completed_job)?;
                } else {
                    pgrx::log!(
                        "koldstore auto-flush: table_oid={} already owned; leaving durable job queued",
                        table_oid
                    );
                }
            }
        }
    }

    if more_due {
        // Another due table remains. Publish a new maintenance generation
        // instead of flushing multiple tables in one reconciliation tick.
        crate::worker::wake::mark_schedule_pending();
    }

    Ok(FlushTickResult {
        completed,
        next_timed_wake_at_ms,
    })
}

fn flush_job_completed(job_id: pgrx::Uuid) -> Result<bool, String> {
    let statement =
        koldstore_flush::plan_flush_job_is_completed().map_err(|error| error.to_string())?;
    crate::spi::select_one::<bool>(&statement, &[DatumWithOid::from(job_id)])
        .map(|value| value.unwrap_or(false))
        .map_err(|error| error.to_string())
}

/// Evaluates one `OlderThan` policy with a single bounded mirror seq-index scan.
///
/// The scan is capped by `max_rows_per_flush`, so eligibility never materializes
/// the whole mirror. Pure due/next-due classification lives in `koldstore-flush`;
/// this adapter owns SPI + PostgreSQL interval arithmetic.
fn evaluate_older_than(
    table_oid: pgrx::pg_sys::Oid,
    policy: &FlushPolicy,
) -> Result<OlderThanEvaluation, String> {
    let FlushPolicy::OlderThan {
        age,
        min_flush_rows,
        max_rows_per_file,
        max_rows_per_flush,
    } = policy
    else {
        return Ok(OlderThanEvaluation::NOT_APPLICABLE);
    };

    let required_rows = (*min_flush_rows).max(*max_rows_per_file).max(1);
    if required_rows > *max_rows_per_flush || *max_rows_per_flush == 0 {
        return Ok(OlderThanEvaluation::NOT_APPLICABLE);
    }
    let scan_rows = i64::try_from(*max_rows_per_flush)
        .map_err(|_| "OlderThan max_rows_per_flush exceeds PostgreSQL bigint range".to_string())?;
    let required_rows_i64 = i64::try_from(required_rows)
        .map_err(|_| "OlderThan minimum batch exceeds PostgreSQL bigint range".to_string())?;

    let cutoff_ms = subtract_move_after_from_now_ms(*age)?;
    let Some(cutoff_seq) = minimum_id_at_unix_millis(cutoff_ms) else {
        return Ok(OlderThanEvaluation::NOT_APPLICABLE);
    };

    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = snapshot.mirror_relation.quoted();

    let (eligible_count, threshold_count, threshold_seq) = pgrx::Spi::connect(|client| {
        let sql = format!(
            r#"
WITH oldest AS MATERIALIZED (
    SELECT seq
    FROM {mirror}
    ORDER BY seq
    LIMIT $1
),
eligible AS (
    SELECT count(*)::bigint AS row_count
    FROM oldest
    WHERE seq < $2
),
threshold_rows AS (
    SELECT seq
    FROM oldest
    ORDER BY seq
    LIMIT $3
)
SELECT (SELECT row_count FROM eligible)::bigint,
       (SELECT count(*)::bigint FROM threshold_rows),
       (SELECT max(seq)::bigint FROM threshold_rows)
"#
        );
        let row = client
            .select(
                &sql,
                None,
                &[
                    DatumWithOid::from(scan_rows),
                    DatumWithOid::from(cutoff_seq),
                    DatumWithOid::from(required_rows_i64),
                ],
            )
            .map_err(|error| error.to_string())?
            .first();
        let eligible_count = row
            .get::<i64>(1)
            .map_err(|error| error.to_string())?
            .unwrap_or(0);
        let threshold_count = row
            .get::<i64>(2)
            .map_err(|error| error.to_string())?
            .unwrap_or(0);
        let threshold_seq = row.get::<i64>(3).map_err(|error| error.to_string())?;
        Ok::<_, String>((eligible_count, threshold_count, threshold_seq))
    })?;

    let threshold_due_at_ms = match threshold_seq {
        Some(seq) if threshold_count >= required_rows_i64 => {
            let threshold_ms = unix_millis_from_id(seq)
                .ok_or_else(|| format!("invalid mirror Snowflake sequence {seq}"))?;
            Some(add_move_after_ms(threshold_ms, *age)?)
        }
        _ => None,
    };

    Ok(evaluate_older_than_scan(
        eligible_count,
        threshold_count,
        required_rows_i64,
        *min_flush_rows,
        *max_rows_per_file,
        threshold_due_at_ms,
    ))
}

fn subtract_move_after_from_now_ms(age: MoveAfter) -> Result<i64, String> {
    pgrx::Spi::get_one_with_args::<i64>(
        "SELECT floor(extract(epoch FROM (\
             statement_timestamp() - \
             make_interval(\
                 months => $1::int, \
                 days => $2::int, \
                 secs => $3::double precision / 1000000.0\
             )\
         )) * 1000)::bigint",
        &[
            DatumWithOid::from(age.months),
            DatumWithOid::from(age.days),
            DatumWithOid::from(age.microseconds),
        ],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "failed to compute OlderThan cutoff timestamp".to_string())
}

/// Adds the persisted PostgreSQL interval with PostgreSQL's own calendar rules.
///
/// Months/days deliberately stay out of Rust duration arithmetic: `MoveAfter`
/// preserves native interval components, so PostgreSQL remains authoritative for
/// calendar-month and DST semantics.
fn add_move_after_ms(timestamp_ms: i64, age: MoveAfter) -> Result<i64, String> {
    pgrx::Spi::get_one_with_args::<i64>(
        "SELECT floor(extract(epoch FROM (\
             to_timestamp($1::double precision / 1000.0) + \
             make_interval(\
                 months => $2::int, \
                 days => $3::int, \
                 secs => $4::double precision / 1000000.0\
             )\
         )) * 1000)::bigint",
        &[
            DatumWithOid::from(timestamp_ms),
            DatumWithOid::from(age.months),
            DatumWithOid::from(age.days),
            DatumWithOid::from(age.microseconds),
        ],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "failed to compute OlderThan next due timestamp".to_string())
}
