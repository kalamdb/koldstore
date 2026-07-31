//! Built-in auto-flush eligibility helpers (PostgreSQL-free).
//!
//! Owns the catalog SQL predicates and candidate-table plans used by the
//! database worker flush tick. SPI execution stays in `pg_koldstore`.

use koldstore_common::{FlushPolicy, ManageTableOptions, SqlParamType, SqlStatement};
use serde_json::Value;
use thiserror::Error;

use crate::policy::policy_flush_row_count;

/// Shared catalog predicates for managed tables the built-in scheduler may flush.
pub const AUTO_FLUSH_TABLE_PREDICATE: &str = r#"
s.active
  AND (
    COALESCE((s.options->>'hot_row_limit')::bigint, 0) > 0
    OR s.options->'flush_policy'->>'type' IN ('row_limit', 'older_than')
  )
  AND COALESCE((s.options->>'auto_flush')::boolean, true)
"#;

/// Auto-flush SQL planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AutoFlushPlanError {
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
}

/// Plans a scan of auto-flush candidate tables with options and mirror counts.
///
/// Callers evaluate [`scheduler_should_flush_parsed`] (or OlderThan SPI) per row
/// and stop at the first due table.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_select_auto_flush_candidate_tables() -> Result<SqlStatement, AutoFlushPlanError> {
    SqlStatement::read(
        "select auto-flush candidate tables",
        &format!(
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
        ),
    )
    .map_err(|error| AutoFlushPlanError::Sql(error.to_string()))
}

/// Plans whether this database still has any auto-flush-eligible managed table.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_database_has_auto_flush_tables() -> Result<SqlStatement, AutoFlushPlanError> {
    SqlStatement::read(
        "database has auto-flush tables",
        &format!(
            r#"
SELECT EXISTS (
    SELECT 1
    FROM koldstore.schemas s
    WHERE {AUTO_FLUSH_TABLE_PREDICATE}
)
"#
        ),
    )
    .map_err(|error| AutoFlushPlanError::Sql(error.to_string()))
}

/// Plans OlderThan eligibility: count and max seq among mirror rows below a cutoff.
///
/// Bind parameters:
/// - `$1` exclusive upper `seq` bound (snowflake cutoff)
/// - `$2` max rows to consider for this flush
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_older_than_eligible_mirror_rows(
    mirror_quoted: &str,
) -> Result<SqlStatement, AutoFlushPlanError> {
    SqlStatement::read_with_params(
        "older-than eligible mirror rows",
        &format!(
            "SELECT count(*)::bigint, max(seq)::bigint FROM (SELECT seq FROM {mirror_quoted} WHERE seq < $1 ORDER BY seq LIMIT $2) eligible"
        ),
        [SqlParamType::BigInt, SqlParamType::BigInt],
    )
    .map_err(|error| AutoFlushPlanError::Sql(error.to_string()))
}

/// Returns whether the scheduler should enqueue/run a flush for these options.
#[must_use]
pub fn scheduler_should_flush(options: &Value, pending_rows: i64) -> bool {
    scheduler_should_flush_parsed(&ManageTableOptions::from_value(options), pending_rows)
}

/// Same as [`scheduler_should_flush`] after options are already decoded once.
#[must_use]
pub fn scheduler_should_flush_parsed(options: &ManageTableOptions, pending_rows: i64) -> bool {
    if !options.auto_flush_enabled() || !options.flush_enabled() {
        return false;
    }
    let Some(policy) = options.flush_policy() else {
        return false;
    };
    policy_needs_flush(&policy, pending_rows)
}

/// Returns whether a decoded flush policy would move any rows for `pending_rows`.
#[must_use]
fn policy_needs_flush(policy: &FlushPolicy, pending_rows: i64) -> bool {
    policy_flush_row_count(pending_rows, policy) > 0
}

#[cfg(test)]
mod tests {
    use super::{
        plan_database_has_auto_flush_tables, plan_older_than_eligible_mirror_rows,
        plan_select_auto_flush_candidate_tables, scheduler_should_flush,
        AUTO_FLUSH_TABLE_PREDICATE,
    };
    use serde_json::json;

    fn row_limit_options(hot_row_limit: u64, min_flush_rows: u64) -> serde_json::Value {
        json!({
            "flush_policy": {
                "type": "row_limit",
                "hot_row_limit": hot_row_limit,
                "min_flush_rows": min_flush_rows,
                "max_rows_per_file": 1000,
                "max_rows_per_flush": 10_000
            }
        })
    }

    #[test]
    fn scheduler_skips_auto_flush_false() {
        let mut options = row_limit_options(10, 1);
        options
            .as_object_mut()
            .unwrap()
            .insert("auto_flush".into(), json!(false));
        assert!(!scheduler_should_flush(&options, 100));
    }

    #[test]
    fn scheduler_flushes_when_over_hot_limit() {
        let options = row_limit_options(10, 1);
        assert!(scheduler_should_flush(&options, 20));
        assert!(!scheduler_should_flush(&options, 10));
    }

    #[test]
    fn scheduler_skips_when_excess_below_min_flush_rows() {
        let options = row_limit_options(10, 100);
        assert!(!scheduler_should_flush(&options, 50));
    }

    #[test]
    fn scheduler_flushes_when_excess_meets_min_flush_rows() {
        let options = row_limit_options(10, 100);
        assert!(scheduler_should_flush(&options, 200));
    }

    #[test]
    fn scheduler_skips_missing_or_disabled_flush_policy() {
        assert!(!scheduler_should_flush(&json!({}), 1_000));
        assert!(!scheduler_should_flush(
            &json!({
                "flush_policy": {
                    "type": "row_limit",
                    "hot_row_limit": 0,
                    "min_flush_rows": 1,
                    "max_rows_per_file": 1000,
                    "max_rows_per_flush": 10_000
                }
            }),
            1_000
        ));
    }

    #[test]
    fn auto_flush_sql_plans_embed_shared_predicate() {
        let candidates = plan_select_auto_flush_candidate_tables().unwrap();
        assert!(candidates.sql.contains("auto_flush"));
        assert!(candidates.sql.contains(AUTO_FLUSH_TABLE_PREDICATE.trim()));
        let exists = plan_database_has_auto_flush_tables().unwrap();
        assert!(exists.sql.contains(AUTO_FLUSH_TABLE_PREDICATE.trim()));
        let older = plan_older_than_eligible_mirror_rows("\"koldstore\".\"items__cl\"").unwrap();
        assert!(older.sql.contains("seq < $1"));
        assert!(older.sql.contains("LIMIT $2"));
    }
}
