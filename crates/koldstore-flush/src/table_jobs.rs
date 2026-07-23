//! SQL plans for synchronous `koldstore.flush_table` job lifecycle.
//!
//! These plans intentionally omit lease guards because inline flush runs in the
//! caller's SPI transaction. Worker-oriented lease plans live in `ops.rs`.

use koldstore_common::SqlStatement;
use thiserror::Error;

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
    phase = 'writing',
    attempts = CASE WHEN attempts = 0 THEN 1 ELSE attempts END,
    payload = payload || jsonb_build_object('started_at', COALESCE(payload->'started_at', to_jsonb(now()))),
    last_heartbeat_at = now(),
    updated_at = now()
WHERE id = $1::uuid
  AND table_oid = $2::oid
  AND job_type = 'flush'
  AND status IN ('pending', 'running')
"#,
    )
    .map_err(|error| TableFlushJobError::Sql(error.to_string()))
}

/// Plans a running-progress heartbeat for an inline flush job.
///
/// Used between catch-up waves so `batches_completed` / `rows_flushed` advance
/// while status remains `running`.
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
SET phase = 'writing',
    rows_processed = $3::bigint,
    rows_flushed = $3::bigint,
    batches_completed = $4::integer,
    checkpoint_seq = $5::bigint,
    checkpoint_commit_seq = $6::bigint,
    last_heartbeat_at = now(),
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
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = now(),
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
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = now(),
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
        plan_insert_inline_flush_job, plan_mark_inline_flush_job_completed,
        plan_mark_inline_flush_job_failed, plan_mark_inline_flush_job_running,
        plan_update_inline_flush_job_progress,
    };

    #[test]
    fn inline_flush_job_insert_persists_requested_force_value() {
        let statement = plan_insert_inline_flush_job().unwrap();

        assert!(statement
            .sql
            .contains("jsonb_build_object('force', $3::boolean)"));
    }

    #[test]
    fn inline_flush_job_running_stamps_started_at() {
        let statement = plan_mark_inline_flush_job_running().unwrap();

        assert!(statement
            .sql
            .contains("jsonb_build_object('started_at', now())"));
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
    fn inline_flush_job_progress_updates_batches_while_running() {
        let statement = plan_update_inline_flush_job_progress().unwrap();
        assert!(statement.sql.contains("batches_completed = $4::integer"));
        assert!(statement.sql.contains("status = 'running'"));
    }
}
