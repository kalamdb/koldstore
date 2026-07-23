//! SQL plans for synchronous `koldstore.flush_table` job lifecycle.
//!
//! Inline flush runs in the caller's SPI transaction. Cross-session cancel uses
//! `koldstore.table_cancel_requests` so peers do not block on the jobs row lock
//! held for the duration of the flush statement.

use koldstore_common::SqlStatement;
use thiserror::Error;

/// Flush job `phase` values written to `koldstore.jobs`.
pub mod flush_phase {
    /// Job row inserted, not yet started.
    pub const PENDING: &str = "pending";
    /// Job claimed; preparing / selecting work.
    pub const CLAIMED: &str = "claimed";
    /// Selecting mirror rows for a wave.
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

/// Inline flush job planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TableFlushJobError {
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
}

/// Plans lookup of the active pending/running inline flush job for a table.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_lookup_active_inline_flush_job() -> std::result::Result<SqlStatement, TableFlushJobError>
{
    SqlStatement::read_with_params(
        "lookup active inline flush job",
        r#"
SELECT COALESCE((
    SELECT jsonb_build_object(
        'id', id::text,
        'force', COALESCE((payload->>'force')::boolean, false)
    )::text
    FROM koldstore.jobs
    WHERE table_oid = $1::oid
      AND scope_key = ''
      AND job_type = 'flush'
      AND status IN ('pending', 'running')
    ORDER BY updated_at, id
    LIMIT 1
), '')
"#,
        [koldstore_common::SqlParamType::Oid],
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans abandonment of a stuck `running` flush job when this backend holds the
/// table-job lock (previous owner crashed or left without a terminal status).
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_abandon_running_flush_jobs() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "abandon running flush jobs",
        r#"
UPDATE koldstore.jobs
SET status = 'error',
    phase = 'failed',
    error_trace = 'abandoned: left running without an owner',
    payload = payload || jsonb_build_object('abandoned', true),
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

/// Plans insertion of a new inline flush job with a caller-provided id.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_insert_inline_flush_job() -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "insert inline flush job",
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

/// Plans the running transition for an inline flush job.
///
/// `$3` is the fixed `progress_total` estimate for the job (rows at catch-up
/// watermark). Pass `0` when unknown.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_mark_inline_flush_job_running() -> std::result::Result<SqlStatement, TableFlushJobError>
{
    SqlStatement::write(
        "mark inline flush job running",
        r#"
UPDATE koldstore.jobs
SET status = 'running',
    phase = 'claimed',
    attempts = CASE WHEN attempts = 0 THEN 1 ELSE attempts END,
    progress_current = 0,
    progress_total = $3::bigint,
    progress_unit = 'rows',
    payload = payload || jsonb_build_object('started_at', COALESCE(payload->'started_at', to_jsonb(now()))),
    updated_at = now()
WHERE id = $1::uuid
  AND table_oid = $2::oid
  AND job_type = 'flush'
  AND status IN ('pending', 'running')
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans a running-progress update for an inline flush job.
///
/// `$3` rows flushed, `$4` batches, `$5`/`$6` checkpoints, `$7` phase,
/// `$8` progress_total (unchanged estimate).
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_update_inline_flush_job_progress(
) -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "update inline flush job progress",
        r#"
UPDATE koldstore.jobs
SET phase = $7::text,
    rows_processed = $3::bigint,
    rows_flushed = $3::bigint,
    batches_completed = $4::integer,
    checkpoint_seq = $5::bigint,
    checkpoint_commit_seq = $6::bigint,
    progress_current = $3::bigint,
    progress_total = GREATEST($8::bigint, $3::bigint),
    progress_unit = 'rows',
    updated_at = now()
WHERE id = $1::uuid
  AND table_oid = $2::oid
  AND job_type = 'flush'
  AND status = 'running'
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans completion of an inline flush job.
///
/// `$3` is total rows flushed, `$4`/`$5` are checkpoint seq watermarks, and
/// `$6` is the number of Parquet segment batches written in this job.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_mark_inline_flush_job_completed(
) -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "mark inline flush job completed",
        r#"
UPDATE koldstore.jobs
SET status = 'completed',
    phase = 'finished',
    rows_processed = $3::bigint,
    rows_flushed = $3::bigint,
    checkpoint_seq = $4::bigint,
    checkpoint_commit_seq = $5::bigint,
    batches_completed = $6::integer,
    progress_current = $3::bigint,
    progress_total = GREATEST(progress_total, $3::bigint),
    progress_unit = 'rows',
    payload = payload || jsonb_build_object(
        'duration_ms',
        GREATEST(
            0,
            (EXTRACT(EPOCH FROM (
                now() - COALESCE(
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
  AND status IN ('pending', 'running')
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans failure recording for an inline flush job.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_mark_inline_flush_job_failed() -> std::result::Result<SqlStatement, TableFlushJobError>
{
    SqlStatement::write(
        "mark inline flush job failed",
        r#"
UPDATE koldstore.jobs
SET status = 'error',
    phase = 'failed',
    error_trace = $3::text,
    payload = payload || jsonb_build_object(
        'duration_ms',
        GREATEST(
            0,
            (EXTRACT(EPOCH FROM (
                now() - COALESCE(
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
  AND status IN ('pending', 'running')
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
                'checkpoint_commit_seq', checkpoint_commit_seq,
                'attempts', attempts,
                'error_trace', error_trace,
                'payload', payload,
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
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_mark_inline_flush_job_cancelled(
) -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "mark inline flush job cancelled",
        r#"
UPDATE koldstore.jobs
SET status = 'cancelled',
    phase = 'cancelled',
    payload = payload || jsonb_build_object(
        'duration_ms',
        GREATEST(
            0,
            (EXTRACT(EPOCH FROM (
                now() - COALESCE(
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
  AND status IN ('pending', 'running')
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans completion after a late cancel (publish already happened).
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_mark_inline_flush_job_completed_after_cancel(
) -> std::result::Result<SqlStatement, TableFlushJobError> {
    SqlStatement::write(
        "mark inline flush completed after cancel",
        r#"
UPDATE koldstore.jobs
SET status = 'completed',
    phase = 'finished',
    rows_processed = $3::bigint,
    rows_flushed = $3::bigint,
    checkpoint_seq = $4::bigint,
    checkpoint_commit_seq = $5::bigint,
    batches_completed = $6::integer,
    progress_current = $3::bigint,
    progress_total = GREATEST(progress_total, $3::bigint),
    progress_unit = 'rows',
    payload = payload || jsonb_build_object(
        'cancel_requested_after_publish', true,
        'duration_ms',
        GREATEST(
            0,
            (EXTRACT(EPOCH FROM (
                now() - COALESCE(
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
  AND status IN ('pending', 'running')
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{
        flush_phase, plan_abandon_running_flush_jobs, plan_insert_inline_flush_job, plan_list_jobs,
        plan_mark_inline_flush_job_cancelled, plan_mark_inline_flush_job_completed,
        plan_mark_inline_flush_job_failed, plan_mark_inline_flush_job_running,
        plan_request_cancel_job, plan_update_inline_flush_job_progress,
    };

    #[test]
    fn inline_flush_job_insert_persists_requested_force_value() {
        let statement = plan_insert_inline_flush_job().unwrap();

        assert!(statement
            .sql
            .contains("jsonb_build_object('force', $3::boolean)"));
    }

    #[test]
    fn inline_flush_job_running_stamps_started_at_and_progress_total() {
        let statement = plan_mark_inline_flush_job_running().unwrap();

        assert!(
            statement.sql.contains(
                "jsonb_build_object('started_at', COALESCE(payload->'started_at', to_jsonb(now())))"
            ),
            "expected idempotent started_at stamp, got:\n{}",
            statement.sql
        );
        assert!(statement.sql.contains("progress_total = $3::bigint"));
        assert!(statement
            .sql
            .contains(&format!("phase = '{}'", flush_phase::CLAIMED)));
    }

    #[test]
    fn inline_flush_job_terminal_states_persist_duration_ms() {
        for statement in [
            plan_mark_inline_flush_job_completed().unwrap(),
            plan_mark_inline_flush_job_failed().unwrap(),
        ] {
            assert!(
                statement.sql.contains("'duration_ms'"),
                "expected duration_ms in {}",
                statement.operation
            );
            assert!(
                statement.sql.contains("payload->>'started_at'"),
                "expected started_at-based duration in {}",
                statement.operation
            );
        }
    }

    #[test]
    fn inline_flush_job_completed_persists_batches_completed() {
        let statement = plan_mark_inline_flush_job_completed().unwrap();
        assert!(
            statement.sql.contains("batches_completed = $6::integer"),
            "expected batches_completed bind in {}",
            statement.sql
        );
    }

    #[test]
    fn inline_flush_job_progress_updates_batches_and_phase_while_running() {
        let statement = plan_update_inline_flush_job_progress().unwrap();
        assert!(statement.sql.contains("batches_completed = $4::integer"));
        assert!(statement.sql.contains("phase = $7::text"));
        assert!(statement.sql.contains("progress_current = $3::bigint"));
        assert!(statement.sql.contains("status = 'running'"));
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
        let statement = plan_mark_inline_flush_job_cancelled().unwrap();
        assert!(statement.sql.contains("status = 'cancelled'"));
        assert!(statement.sql.contains("phase = 'cancelled'"));
    }

    #[test]
    fn abandon_running_flush_plan_marks_error() {
        let statement = plan_abandon_running_flush_jobs().unwrap();
        assert!(statement.sql.contains("status = 'error'"));
        assert!(statement.sql.contains("abandoned"));
        assert!(statement.sql.contains("status = 'running'"));
    }
}
