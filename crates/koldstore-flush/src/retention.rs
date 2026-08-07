//! Terminal job retention / purge SQL plans.
//!
//! Deletes aged `completed` / `cancelled` / `error` rows from `koldstore.jobs`
//! in small batches. Never removes a job still referenced by a `pending`
//! cold segment (`writer_job_id`), so recovery can still claim ownership.

use koldstore_common::{SqlParamType, SqlStatement};
use thiserror::Error;

/// Default batch size for coordinator / operator purge ticks.
pub const DEFAULT_PURGE_BATCH_LIMIT: i32 = 100;

/// Job retention planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum JobRetentionError {
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
}

/// Plans a batched delete of terminal jobs older than `retention_days`.
///
/// Parameters:
/// 1. `retention_days` (`integer`) — age threshold; caller skips when `0`
/// 2. `batch_limit` (`integer`) — max rows deleted per invocation
///
/// Returns deleted row count as `bigint`.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_purge_old_jobs() -> Result<SqlStatement, JobRetentionError> {
    SqlStatement::write_with_params(
        "purge old jobs",
        r#"
WITH candidates AS (
    SELECT j.id
    FROM koldstore.jobs j
    WHERE j.status IN ('completed', 'cancelled', 'error')
      AND j.finished_at IS NOT NULL
      AND j.finished_at < now() - ($1::integer * interval '1 day')
      AND NOT EXISTS (
          SELECT 1
          FROM koldstore.cold_segments cs
          WHERE cs.writer_job_id = j.id
            AND cs.status = 'pending'
      )
    ORDER BY j.finished_at ASC, j.id ASC
    LIMIT $2::integer
    FOR UPDATE OF j SKIP LOCKED
),
deleted AS (
    DELETE FROM koldstore.jobs j
    USING candidates c
    WHERE j.id = c.id
    RETURNING j.id
)
SELECT count(*)::bigint FROM deleted
"#,
        [SqlParamType::Integer, SqlParamType::Integer],
    )
    .map_err(|error| JobRetentionError::Sql(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{plan_purge_old_jobs, DEFAULT_PURGE_BATCH_LIMIT};

    #[test]
    fn purge_plan_targets_terminal_statuses_with_finished_at() {
        let statement = plan_purge_old_jobs().unwrap();
        assert!(statement
            .sql
            .contains("status IN ('completed', 'cancelled', 'error')"));
        assert!(statement.sql.contains("finished_at IS NOT NULL"));
        assert!(statement
            .sql
            .contains("finished_at < now() - ($1::integer * interval '1 day')"));
        assert!(statement.sql.contains("LIMIT $2::integer"));
        assert!(statement.sql.contains("FOR UPDATE OF j SKIP LOCKED"));
        assert_eq!(DEFAULT_PURGE_BATCH_LIMIT, 100);
    }

    #[test]
    fn purge_plan_never_deletes_jobs_with_pending_segments() {
        let statement = plan_purge_old_jobs().unwrap();
        assert!(statement.sql.contains("koldstore.cold_segments"));
        assert!(statement.sql.contains("writer_job_id = j.id"));
        assert!(statement.sql.contains("cs.status = 'pending'"));
        assert!(statement.sql.contains("NOT EXISTS"));
        assert!(statement.sql.contains("DELETE FROM koldstore.jobs"));
        assert!(statement.sql.contains("count(*)::bigint"));
    }
}
