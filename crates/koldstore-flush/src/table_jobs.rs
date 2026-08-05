//! SQL plans for `koldstore.flush_table` job lifecycle.
//!
//! Job mutations are fenced by `attempt_token` so a reclaimed job cannot be
//! mutated by a stale executor. Cross-session cancel uses
//! `koldstore.table_cancel_requests` so peers do not block on the jobs row lock
//! held during an in-flight flush. Enqueue-and-return lives in `ops`; executors
//! claim via session table locks and these plans.

use koldstore_common::SqlStatement;
use thiserror::Error;

use crate::jobs_sql::ACTIVE_FLUSH_JOB_CONFLICT_PREDICATE;

/// Flush job `phase` values written to `koldstore.jobs`.
pub mod flush_phase {
    /// Job row inserted, not yet started.
    pub const PENDING: &str = "pending";
    /// Job claimed; preparing / selecting work.
    pub const CLAIMED: &str = "claimed";
    /// Selecting mirror rows for a pass.
    pub const SELECTING: &str = "selecting";
    /// Encoding and uploading cold segments.
    pub const WRITING: &str = "writing";
    /// Publishing manifest / activating pending segments.
    pub const ACTIVATING: &str = "activating";
    /// Pruning hot/mirror rows after activate.
    pub const PRUNING: &str = "pruning";
    /// Terminal success.
    pub const FINISHED: &str = "finished";
    /// Terminal failure.
    pub const FAILED: &str = "failed";
}

/// Flush job planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TableFlushJobError {
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
}

/// Plans lookup of the active pending/running flush job for a table.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_lookup_active_flush_job() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::read_with_params(
        "lookup active flush job",
        &format!(
            r#"
SELECT COALESCE((
    SELECT jsonb_build_object(
        'id', id::text,
        'force', COALESCE((payload->>'force')::boolean, false)
    )::text
    FROM koldstore.jobs
    WHERE table_oid = $1::oid
      AND scope_key = ''
      AND {ACTIVE_FLUSH_JOB_CONFLICT_PREDICATE}
    ORDER BY updated_at, id
    LIMIT 1
), '')
"#
        ),
        [koldstore_common::SqlParamType::Oid],
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans reclaim of a stuck `running` flush job when this backend holds the
/// session table-job lock (previous owner crashed or left without a terminal
/// status). The same durable job returns to `pending` so a later claim resumes
/// it instead of inserting a replacement row.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_reclaim_running_flush_jobs() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "reclaim running flush jobs",
        r#"
UPDATE koldstore.jobs
SET status = 'pending',
    phase = 'pending',
    attempt_token = NULL,
    available_at = now(),
    error_trace = COALESCE(error_trace, 'reclaimed: left running without an owner'),
    payload = payload || jsonb_build_object('reclaimed', true),
    updated_at = now()
WHERE table_oid = $1::oid
  AND scope_key = ''
  AND job_type = 'flush'
  AND status = 'running'
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans listing table oids that still have a durable `running` flush job.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_list_running_flush_table_oids() -> std::result::Result<SqlStatement, TableFlushJobError>
{
    SqlStatement::read(
        "list running flush table oids",
        r#"
SELECT COALESCE(
    jsonb_agg(table_oid::bigint ORDER BY table_oid),
    '[]'::jsonb
)::text
FROM (
    SELECT DISTINCT table_oid
    FROM koldstore.jobs
    WHERE job_type = 'flush'
      AND status = 'running'
) t
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans insertion of a new flush job with a caller-provided id.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_insert_flush_job() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "insert flush job",
        r#"
INSERT INTO koldstore.jobs (
    id,
    table_oid,
    scope_key,
    job_type,
    status,
    phase,
    payload
)
VALUES (
    $1::uuid,
    $2::oid,
    '',
    'flush',
    'pending',
    'pending',
    jsonb_build_object('force', $3::boolean)
)
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans the running transition for a flush job.
///
/// `$3` is the new `attempt_token`. `$4` is the fixed `progress_total` estimate.
/// `$5` is the fixed job watermark (`flush_seq_upper_bound` / target_seq).
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_mark_flush_job_running() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "mark flush job running",
        r#"
UPDATE koldstore.jobs
SET status = 'running',
    phase = 'claimed',
    attempts = attempts + 1,
    attempt_token = $3::uuid,
    progress_current = 0,
    progress_total = $4::bigint,
    progress_unit = 'rows',
    flush_seq_upper_bound = COALESCE(flush_seq_upper_bound, $5::bigint),
    started_at = COALESCE(started_at, clock_timestamp()),
    available_at = now(),
    payload = payload || jsonb_build_object('started_at', COALESCE(payload->'started_at', to_jsonb(clock_timestamp()))),
    updated_at = now()
WHERE id = $1::uuid
  AND table_oid = $2::oid
  AND job_type = 'flush'
  AND status IN ('pending', 'running')
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans a running-progress update for a flush job.
///
/// `$3` attempt_token, `$4` rows flushed, `$5` batches, `$6` checkpoint,
/// `$7` phase, `$8` progress_total (unchanged estimate).
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_update_flush_job_progress() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "update flush job progress",
        r#"
UPDATE koldstore.jobs
SET phase = $7::text,
    rows_processed = $4::bigint,
    rows_flushed = $4::bigint,
    batches_completed = $5::integer,
    checkpoint_seq = $6::bigint,
    progress_current = $4::bigint,
    progress_total = GREATEST($8::bigint, $4::bigint),
    progress_unit = 'rows',
    updated_at = now()
WHERE id = $1::uuid
  AND table_oid = $2::oid
  AND job_type = 'flush'
  AND status = 'running'
  AND attempt_token = $3::uuid
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans completion of a flush job.
///
/// `$3` is attempt_token, `$4` total rows flushed, `$5` checkpoint seq
/// watermark, and `$6` is the number of Parquet segment batches written.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_mark_flush_job_completed() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "mark flush job completed",
        r#"
UPDATE koldstore.jobs
SET status = 'completed',
    phase = 'finished',
    rows_processed = $4::bigint,
    rows_flushed = $4::bigint,
    checkpoint_seq = $5::bigint,
    batches_completed = $6::integer,
    progress_current = $4::bigint,
    progress_total = GREATEST(progress_total, $4::bigint),
    progress_unit = 'rows',
    finished_at = clock_timestamp(),
    payload = payload || jsonb_build_object(
        'duration_ms',
        GREATEST(
            0,
            (EXTRACT(EPOCH FROM (
                clock_timestamp() - COALESCE(
                    started_at,
                    (payload->>'started_at')::timestamptz,
                    created_at
                )
            )) * 1000)::bigint
        )
    ),
    updated_at = now()
WHERE id = $1::uuid
  AND table_oid = $2::oid
  AND job_type = 'flush'
  AND status = 'running'
  AND attempt_token = $3::uuid
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans failure recording for a flush job.
///
/// `$3` is attempt_token, `$4` is the error trace.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_mark_flush_job_failed() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "mark flush job failed",
        r#"
UPDATE koldstore.jobs
SET status = 'error',
    phase = 'failed',
    error_trace = $4::text,
    finished_at = clock_timestamp(),
    payload = payload || jsonb_build_object(
        'duration_ms',
        GREATEST(
            0,
            (EXTRACT(EPOCH FROM (
                clock_timestamp() - COALESCE(
                    started_at,
                    (payload->>'started_at')::timestamptz,
                    created_at
                )
            )) * 1000)::bigint
        )
    ),
    updated_at = now()
WHERE id = $1::uuid
  AND table_oid = $2::oid
  AND job_type = 'flush'
  AND status = 'running'
  AND attempt_token = $3::uuid
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans a filtered jobs listing for operator / UI polling.
///
/// `$1` optional statuses jsonb array (e.g. `["running","pending"]`),
/// `$2` optional job_types jsonb array, `$3` optional table oid.
/// NULL arguments mean "no filter".
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_list_jobs() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::read_with_params(
        "list koldstore jobs",
        r#"
SELECT COALESCE(
    (
        SELECT jsonb_agg(job_row ORDER BY (job_row->>'updated_at') DESC, job_row->>'id')
        FROM (
            SELECT jsonb_build_object(
                'id', id::text,
                'table_oid', table_oid,
                'scope_key', scope_key,
                'job_type', job_type,
                'status', status,
                'phase', phase,
                'rows_processed', rows_processed,
                'rows_flushed', rows_flushed,
                'batches_completed', batches_completed,
                'progress_current', progress_current,
                'progress_total', progress_total,
                'progress_unit', progress_unit,
                'checkpoint_seq', checkpoint_seq,
                'attempts', attempts,
                'attempt_token', attempt_token,
                'error_trace', error_trace,
                'payload', payload,
                'available_at', available_at,
                'started_at', started_at,
                'finished_at', finished_at,
                'created_at', created_at,
                'updated_at', updated_at
            ) AS job_row
            FROM koldstore.jobs
            WHERE ($1::jsonb IS NULL OR status IN (
                    SELECT jsonb_array_elements_text($1::jsonb)
                ))
              AND ($2::jsonb IS NULL OR job_type IN (
                    SELECT jsonb_array_elements_text($2::jsonb)
                ))
              AND ($3::oid IS NULL OR table_oid = $3::oid)
            ORDER BY updated_at DESC, id
            LIMIT 200
        ) listed
    ),
    '[]'::jsonb
)::text
"#,
        [
            koldstore_common::SqlParamType::Jsonb,
            koldstore_common::SqlParamType::Jsonb,
            koldstore_common::SqlParamType::Oid,
        ],
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans cooperative cancel for one job.
///
/// Always records a table-level cancel request (visible to a running flush without
/// contending for the jobs row lock). Pending/running jobs that are not locked are
/// updated via `FOR UPDATE SKIP LOCKED`.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_request_cancel_job() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "request cancel job",
        r#"
WITH target AS (
    SELECT id, table_oid, status
    FROM koldstore.jobs
    WHERE id = $1::uuid
      AND status IN ('pending', 'running')
),
req AS (
    INSERT INTO koldstore.table_cancel_requests (table_oid, requested_at)
    SELECT table_oid, now() FROM target
    ON CONFLICT (table_oid) DO UPDATE SET requested_at = excluded.requested_at
    RETURNING table_oid
),
unlocked AS (
    SELECT j.id, j.status
    FROM koldstore.jobs j
    JOIN target t ON t.id = j.id
    FOR UPDATE OF j SKIP LOCKED
),
cancelled_pending AS (
    UPDATE koldstore.jobs j
    SET status = 'cancelled',
        phase = 'cancelled',
        cancel_requested_at = COALESCE(cancel_requested_at, now()),
        updated_at = now()
    FROM unlocked u
    WHERE j.id = u.id
      AND u.status = 'pending'
    RETURNING j.id
),
signalled_running AS (
    UPDATE koldstore.jobs j
    SET cancel_requested_at = COALESCE(cancel_requested_at, now()),
        updated_at = now()
    FROM unlocked u
    WHERE j.id = u.id
      AND u.status = 'running'
    RETURNING j.id
)
SELECT COALESCE(
    (SELECT id::text FROM cancelled_pending LIMIT 1),
    (SELECT id::text FROM signalled_running LIMIT 1),
    (SELECT $1::uuid::text FROM req LIMIT 1)
)
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans cooperative cancel for all active jobs on one table.
///
/// Upserts `table_cancel_requests` and updates unlocked active job rows.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_request_cancel_table_jobs() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "request cancel table jobs",
        r#"
WITH req AS (
    INSERT INTO koldstore.table_cancel_requests (table_oid, requested_at)
    VALUES ($1::oid, now())
    ON CONFLICT (table_oid) DO UPDATE SET requested_at = excluded.requested_at
    RETURNING table_oid
),
unlocked AS (
    SELECT j.id, j.status
    FROM koldstore.jobs j
    WHERE j.table_oid = $1::oid
      AND j.status IN ('pending', 'running')
    FOR UPDATE OF j SKIP LOCKED
),
cancelled_pending AS (
    UPDATE koldstore.jobs j
    SET status = 'cancelled',
        phase = 'cancelled',
        cancel_requested_at = COALESCE(cancel_requested_at, now()),
        updated_at = now()
    FROM unlocked u
    WHERE j.id = u.id
      AND u.status = 'pending'
    RETURNING j.id
),
signalled_running AS (
    UPDATE koldstore.jobs j
    SET cancel_requested_at = COALESCE(cancel_requested_at, now()),
        updated_at = now()
    FROM unlocked u
    WHERE j.id = u.id
      AND u.status = 'running'
    RETURNING j.id
)
SELECT (
    (SELECT count(*) FROM cancelled_pending)
  + (SELECT count(*) FROM signalled_running)
  + (SELECT count(*) FROM req)
)::bigint
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans hard-cancel of pending jobs and cancel-request for running jobs on DROP/unmanage.
///
/// Pending rows become `cancelled` immediately when unlocked. Running rows are
/// signalled via `table_cancel_requests` (and `cancel_requested_at` when the jobs
/// row is not locked by the owner).
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_cancel_jobs_for_drop() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "cancel jobs for drop",
        r#"
WITH req AS (
    INSERT INTO koldstore.table_cancel_requests (table_oid, requested_at)
    VALUES ($1::oid, now())
    ON CONFLICT (table_oid) DO UPDATE SET requested_at = excluded.requested_at
    RETURNING table_oid
),
pending AS (
    SELECT j.id
    FROM koldstore.jobs j
    WHERE j.table_oid = $1::oid
      AND j.status = 'pending'
    FOR UPDATE OF j SKIP LOCKED
),
cancelled_pending AS (
    UPDATE koldstore.jobs j
    SET status = 'cancelled',
        phase = 'cancelled',
        cancel_requested_at = COALESCE(cancel_requested_at, now()),
        updated_at = now()
    FROM pending p
    WHERE j.id = p.id
    RETURNING j.id
),
running AS (
    SELECT j.id
    FROM koldstore.jobs j
    WHERE j.table_oid = $1::oid
      AND j.status = 'running'
    FOR UPDATE OF j SKIP LOCKED
),
signalled_running AS (
    UPDATE koldstore.jobs j
    SET cancel_requested_at = COALESCE(cancel_requested_at, now()),
        updated_at = now()
    FROM running r
    WHERE j.id = r.id
    RETURNING j.id
)
SELECT (
    (SELECT count(*) FROM cancelled_pending)
  + (SELECT count(*) FROM signalled_running)
  + (SELECT count(*) FROM req)
)::bigint
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans a cancel-flag poll for a running flush job.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_flush_cancel_requested() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::read_with_params(
        "flush cancel requested",
        r#"
SELECT EXISTS (
    SELECT 1
    FROM koldstore.table_cancel_requests
    WHERE table_oid = $2::oid
)
OR COALESCE((
    SELECT cancel_requested_at IS NOT NULL
    FROM koldstore.jobs
    WHERE id = $1::uuid
      AND table_oid = $2::oid
      AND job_type = 'flush'
), false)
"#,
        [
            koldstore_common::SqlParamType::Uuid,
            koldstore_common::SqlParamType::Oid,
        ],
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans clearing the table-level cancel request after a job finishes.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_clear_table_cancel_request() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "clear table cancel request",
        "DELETE FROM koldstore.table_cancel_requests WHERE table_oid = $1::oid",
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans terminal cancel for a flush that stopped before publish.
///
/// `$3` is attempt_token.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_mark_flush_job_cancelled() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "mark flush job cancelled",
        r#"
UPDATE koldstore.jobs
SET status = 'cancelled',
    phase = 'cancelled',
    finished_at = clock_timestamp(),
    payload = payload || jsonb_build_object(
        'duration_ms',
        GREATEST(
            0,
            (EXTRACT(EPOCH FROM (
                clock_timestamp() - COALESCE(
                    started_at,
                    (payload->>'started_at')::timestamptz,
                    created_at
                )
            )) * 1000)::bigint
        )
    ),
    updated_at = now()
WHERE id = $1::uuid
  AND table_oid = $2::oid
  AND job_type = 'flush'
  AND status = 'running'
  AND attempt_token = $3::uuid
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans completion after a late cancel (publish already happened).
///
/// `$3` is attempt_token, `$4` rows, `$5` checkpoint, `$6` batches.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_mark_flush_job_completed_after_cancel(
) -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "mark flush completed after cancel",
        r#"
UPDATE koldstore.jobs
SET status = 'completed',
    phase = 'finished',
    rows_processed = $4::bigint,
    rows_flushed = $4::bigint,
    checkpoint_seq = $5::bigint,
    batches_completed = $6::integer,
    progress_current = $4::bigint,
    progress_total = GREATEST(progress_total, $4::bigint),
    progress_unit = 'rows',
    finished_at = clock_timestamp(),
    payload = payload || jsonb_build_object(
        'cancel_requested_after_publish', true,
        'duration_ms',
        GREATEST(
            0,
            (EXTRACT(EPOCH FROM (
                clock_timestamp() - COALESCE(
                    started_at,
                    (payload->>'started_at')::timestamptz,
                    created_at
                )
            )) * 1000)::bigint
        )
    ),
    updated_at = now()
WHERE id = $1::uuid
  AND table_oid = $2::oid
  AND job_type = 'flush'
  AND status = 'running'
  AND attempt_token = $3::uuid
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{
        flush_phase, plan_insert_flush_job, plan_list_jobs, plan_mark_flush_job_cancelled,
        plan_mark_flush_job_completed, plan_mark_flush_job_failed, plan_mark_flush_job_running,
        plan_reclaim_running_flush_jobs, plan_request_cancel_job, plan_update_flush_job_progress,
    };

    #[test]
    fn flush_job_insert_persists_requested_force_value() {
        let statement = plan_insert_flush_job().unwrap();

        assert!(statement
            .sql
            .contains("jsonb_build_object('force', $3::boolean)"));
    }

    #[test]
    fn flush_job_running_stamps_attempt_token_and_progress_total() {
        let statement = plan_mark_flush_job_running().unwrap();

        assert!(
            statement.sql.contains("attempt_token = $3::uuid"),
            "expected attempt_token stamp, got:\n{}",
            statement.sql
        );
        assert!(statement.sql.contains("progress_total = $4::bigint"));
        assert!(statement
            .sql
            .contains("flush_seq_upper_bound = COALESCE(flush_seq_upper_bound, $5::bigint)"));
        assert!(statement
            .sql
            .contains("started_at = COALESCE(started_at, clock_timestamp())"));
        assert!(statement
            .sql
            .contains(&format!("phase = '{}'", flush_phase::CLAIMED)));
    }

    #[test]
    fn flush_job_terminal_states_persist_duration_ms() {
        for statement in [
            plan_mark_flush_job_completed().unwrap(),
            plan_mark_flush_job_failed().unwrap(),
        ] {
            assert!(
                statement.sql.contains("'duration_ms'"),
                "expected duration_ms in {}",
                statement.operation
            );
            assert!(
                statement.sql.contains("attempt_token = $3::uuid"),
                "expected attempt fencing in {}",
                statement.operation
            );
            assert!(
                statement.sql.contains("finished_at = clock_timestamp()"),
                "expected finished_at in {}",
                statement.operation
            );
        }
    }

    #[test]
    fn flush_job_completed_persists_batches_completed() {
        let statement = plan_mark_flush_job_completed().unwrap();
        assert!(
            statement.sql.contains("batches_completed = $6::integer"),
            "expected batches_completed bind in {}",
            statement.sql
        );
    }

    #[test]
    fn flush_job_progress_updates_batches_and_phase_while_running() {
        let statement = plan_update_flush_job_progress().unwrap();
        assert!(statement.sql.contains("batches_completed = $5::integer"));
        assert!(statement.sql.contains("phase = $7::text"));
        assert!(statement.sql.contains("progress_current = $4::bigint"));
        assert!(statement.sql.contains("status = 'running'"));
        assert!(statement.sql.contains("attempt_token = $3::uuid"));
        assert!(!statement.sql.contains("checkpoint_commit_seq"));
    }

    #[test]
    fn list_jobs_plan_filters_status_type_and_table() {
        let statement = plan_list_jobs().unwrap();
        assert!(statement
            .sql
            .contains("jsonb_array_elements_text($1::jsonb)"));
        assert!(statement
            .sql
            .contains("jsonb_array_elements_text($2::jsonb)"));
        assert!(statement.sql.contains("table_oid = $3::oid"));
        assert!(statement.sql.contains("progress_current"));
        assert!(statement.sql.contains("'attempt_token', attempt_token"));
    }

    #[test]
    fn cancel_job_plan_sets_cancel_requested_at() {
        let statement = plan_request_cancel_job().unwrap();
        assert!(statement.sql.contains("table_cancel_requests"));
        assert!(statement.sql.contains("FOR UPDATE OF j SKIP LOCKED"));
        assert!(statement.sql.contains("cancel_requested_at = COALESCE"));
    }

    #[test]
    fn cancelled_flush_plan_sets_cancelled_status() {
        let statement = plan_mark_flush_job_cancelled().unwrap();
        assert!(statement.sql.contains("status = 'cancelled'"));
        assert!(statement.sql.contains("phase = 'cancelled'"));
        assert!(statement.sql.contains("attempt_token = $3::uuid"));
    }

    #[test]
    fn reclaim_running_flush_plan_returns_to_pending() {
        let statement = plan_reclaim_running_flush_jobs().unwrap();
        assert!(statement.sql.contains("status = 'pending'"));
        assert!(statement.sql.contains("attempt_token = NULL"));
        assert!(statement.sql.contains("reclaimed"));
        assert!(statement.sql.contains("status = 'running'"));
        assert!(!statement.sql.contains("status = 'error'"));
    }
}
