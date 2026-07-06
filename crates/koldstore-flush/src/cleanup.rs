//! Hot cleanup after manifest commit.

use koldstore_mirror::{
    mirror_delete_using_selected_sql, quoted_pk_columns, selected_record_columns, MirrorRelation,
};
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
    /// Parameterized cleanup statement. `$1` is a JSON selected-set.
    pub statement: SqlStatement,
}

/// Planned hot cleanup behavior after a flush attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HotCleanupPlan {
    /// Whether live hot rows may be removed.
    pub remove_live_hot_rows: bool,
    /// Whether a hot tombstone must remain to mask older cold rows.
    pub retain_tombstone: bool,
}

/// Returns whether cleanup may remove live hot rows.
#[must_use]
pub fn cleanup_allowed(manifest_committed: bool) -> bool {
    manifest_committed
}

/// Returns whether a tombstone should be retained after cleanup.
#[must_use]
pub const fn retain_tombstone(cold_may_contain_pk: bool) -> bool {
    cold_may_contain_pk
}

/// Plans hot cleanup after manifest commit.
#[must_use]
pub const fn plan_hot_cleanup(
    manifest_committed: bool,
    cold_may_contain_pk: bool,
) -> HotCleanupPlan {
    HotCleanupPlan {
        remove_live_hot_rows: manifest_committed,
        retain_tombstone: retain_tombstone(cold_may_contain_pk),
    }
}

/// Plans cleanup for a committed clean-schema flush selected set.
///
/// `$1::jsonb` is expected to be an array of objects containing primary-key
/// columns plus `seq` and `op`. Cleanup removes only mirror rows that still
/// match the flushed `seq`; live selected rows may be removed from the base
/// table, while delete marker rows have no base row to remove.
///
/// # Errors
///
/// Returns an error when the primary key is empty, identifiers are unsafe, or
/// statement metadata cannot be prepared.
pub fn plan_clean_schema_cleanup(
    table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key_columns: &[String],
) -> CleanupResult<CleanSchemaCleanupPlan> {
    if primary_key_columns.is_empty() {
        return Err(CleanupError::MissingPrimaryKey);
    }

    let primary_key: Vec<&str> = primary_key_columns.iter().map(String::as_str).collect();
    let mirror = mirror_table
        .as_table_name()
        .map(MirrorRelation::new)
        .map_err(|error| CleanupError::Spi(error.to_string()))?;
    let record_columns = selected_record_columns(&primary_key)?;
    let pk_columns = quoted_pk_columns(&primary_key)?;
    let mirror_delete = mirror_delete_using_selected_sql(&mirror, &primary_key)?;
    let hot_join = pk_columns
        .iter()
        .map(|column| format!("hot.{column}::text = selected.{column}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        r#"
WITH selected AS (
    SELECT *
    FROM jsonb_to_recordset($1::jsonb) AS selected({record_columns})
),
removed_mirror AS (
    {mirror_delete}
    RETURNING mirror."seq"
)
DELETE FROM ONLY {table} AS hot
USING selected, removed_mirror
WHERE selected."op" IN (1, 2)
  AND {hot_join}
"#,
        table = table.quoted(),
    );
    let statement =
        SqlStatement::write_with_params("clean schema flush cleanup", &sql, [SqlParamType::Jsonb])
            .map_err(|error| CleanupError::Spi(error.to_string()))?;

    Ok(CleanSchemaCleanupPlan {
        table: table.clone(),
        mirror_table: mirror_table.clone(),
        statement,
    })
}
