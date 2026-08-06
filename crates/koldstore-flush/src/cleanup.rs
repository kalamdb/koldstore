//! Hot cleanup after manifest commit.
//!
//! Production flush prune uses [`plan_seq_range_cleanup`] (`seq <= max_seq`).
//! JSON selected-set cleanup was retired; do not revive it for the live path.

use koldstore_mirror::{quoted_pk_columns, MirrorRelation};
use thiserror::Error;

use koldstore_common::{QualifiedTableName, SqlParamType, SqlStatement};

/// Clean-schema cleanup planning result.
pub type CleanupResult<T> = Result<T, CleanupError>;

/// Clean-schema cleanup planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CleanupError {
    /// Primary key is needed to fence cleanup.
    #[error("clean-schema cleanup requires at least one primary-key column")]
    MissingPrimaryKey,
    /// Identifier is unsafe to quote.
    #[error("invalid cleanup identifier `{0}`")]
    InvalidIdentifier(String),
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
}

impl From<koldstore_mirror::MirrorError> for CleanupError {
    fn from(error: koldstore_mirror::MirrorError) -> Self {
        match error {
            koldstore_mirror::MirrorError::MissingPrimaryKey => Self::MissingPrimaryKey,
            koldstore_mirror::MirrorError::InvalidColumn(name) => Self::InvalidIdentifier(name),
            other => Self::Spi(other.to_string()),
        }
    }
}

/// Planned clean-schema cleanup statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanSchemaCleanupPlan {
    /// Source user table.
    pub table: QualifiedTableName,
    /// Table-specific mirror table.
    pub mirror_table: QualifiedTableName,
    /// Parameterized cleanup statement.
    pub statement: SqlStatement,
}

/// Returns whether cleanup may remove live hot rows (manifest already committed).
#[must_use]
pub const fn cleanup_allowed(manifest_committed: bool) -> bool {
    manifest_committed
}

/// Returns whether a tombstone should be retained after cleanup.
#[must_use]
pub const fn retain_tombstone(cold_may_contain_pk: bool) -> bool {
    cold_may_contain_pk
}

/// Plans cleanup for a contiguous oldest-by-`seq` flush without per-row JSON.
///
/// PERFORMANCE: Policy and force flushes select a seq prefix (`seq <= max_seq`).
/// Cleanup can delete that prefix directly from the mirror (optionally filtered
/// by mirror op codes) and join hot deletes from the removed set — no
/// `jsonb_to_recordset` materialization of every flushed PK.
///
/// Bind parameters:
/// - `$1` inclusive mirror `seq` upper bound
///
/// # Errors
///
/// Returns an error when the primary key is empty, identifiers are unsafe, or
/// statement metadata cannot be prepared.
pub fn plan_seq_range_cleanup(
    table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key_columns: &[String],
    mirror_ops: Option<&[i16]>,
) -> CleanupResult<CleanSchemaCleanupPlan> {
    if primary_key_columns.is_empty() {
        return Err(CleanupError::MissingPrimaryKey);
    }

    let primary_key: Vec<&str> = primary_key_columns.iter().map(String::as_str).collect();
    let mirror = mirror_table
        .as_table_name()
        .map(MirrorRelation::new)
        .map_err(|error| CleanupError::Spi(error.to_string()))?;
    let pk_columns = quoted_pk_columns(&primary_key)?;
    let returning_columns = pk_columns
        .iter()
        .map(|column| format!("mirror.{column}"))
        .chain(["mirror.\"seq\"".to_string(), "mirror.\"op\"".to_string()])
        .collect::<Vec<_>>()
        .join(", ");
    let mut mirror_where = vec!["mirror.\"seq\" <= $1::bigint".to_string()];
    if let Some(ops) = mirror_ops {
        if let Some(op_clause) = crate::jobs_sql::mirror_ops_where_clause(ops) {
            mirror_where.push(op_clause);
        }
    }
    let hot_join = pk_columns
        .iter()
        .map(|column| format!("hot.{column} = removed_mirror.{column}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        r#"
WITH removed_mirror AS (
    DELETE FROM {mirror} AS mirror
    WHERE {mirror_where}
    RETURNING {returning_columns}
),
deleted_hot AS (
    DELETE FROM ONLY {table} AS hot
    USING removed_mirror
    WHERE removed_mirror."op" IN (1, 2)
      AND {hot_join}
    RETURNING 1
)
SELECT
    (SELECT count(*)::bigint FROM removed_mirror) AS mirror_pruned,
    (SELECT count(*)::bigint FROM deleted_hot) AS hot_pruned
"#,
        mirror = mirror.quoted(),
        mirror_where = mirror_where.join(" AND "),
        returning_columns = returning_columns,
        table = table.quoted(),
        hot_join = hot_join,
    );
    let statement = SqlStatement::write_with_params(
        "seq-range clean schema flush cleanup",
        &sql,
        [SqlParamType::BigInt],
    )
    .map_err(|error| CleanupError::Spi(error.to_string()))?;

    Ok(CleanSchemaCleanupPlan {
        table: table.clone(),
        mirror_table: mirror_table.clone(),
        statement,
    })
}
