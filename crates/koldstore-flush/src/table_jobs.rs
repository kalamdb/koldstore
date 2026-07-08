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
    jsonb_build_object('force', false)
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

/// Plans completion of an inline flush job.
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
