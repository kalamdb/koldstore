//! Latest-state change-feed SQL helpers.

use koldstore_common::{
    scope::scope_predicate_sql, MirrorChange, QualifiedTableName, ScopeKey, SeqId, SqlStatement,
};
use koldstore_mirror::{
    plan_select_mirror_rows_after_seq_with_params, statement::mirror_to_sql, MirrorRelation,
    SqlParamType,
};
use thiserror::Error;

use crate::{changes_since as merge_changes_since, ChangeCursor, ChangeGap};

/// Default changes_since limit.
pub const DEFAULT_CHANGE_LIMIT: i32 = 1000;

/// Change-feed helper error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ChangeFeedError {
    /// `limit_rows` must be greater than zero.
    #[error("limit_rows must be positive")]
    InvalidLimit,
    /// Requested cursor is older than retained changes.
    #[error(transparent)]
    RetentionGap(#[from] ChangeGap),
    /// A primary-key column is required to build a mirror query.
    #[error("changes_since requires primary key columns")]
    MissingPrimaryKey,
    /// Scope column is unsafe to quote.
    #[error("invalid scope column `{0}`")]
    InvalidScopeColumn(String),
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
}

/// Planned mirror-backed changes_since query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorChangesSincePlan {
    /// SQL statement to execute.
    pub statement: SqlStatement,
    /// Parameter index used for scope filtering, when present.
    pub scope_parameter_index: Option<usize>,
}

/// Returns latest-state changes for one table/scope after a mirror sequence cursor.
///
/// # Errors
///
/// Returns [`ChangeFeedError::InvalidLimit`] when `limit_rows <= 0`, or a
/// retention gap when `since_seq` is older than retained mirror/cold metadata.
pub fn changes_since(
    changes: &[MirrorChange],
    table_oid: u32,
    scope_key: Option<&ScopeKey>,
    since_seq: i64,
    limit_rows: Option<i32>,
    oldest_available: Option<SeqId>,
) -> Result<Vec<MirrorChange>, ChangeFeedError> {
    let limit = limit_rows.unwrap_or(DEFAULT_CHANGE_LIMIT);
    if limit <= 0 {
        return Err(ChangeFeedError::InvalidLimit);
    }

    let scoped_changes = changes
        .iter()
        .filter(|change| change.table_oid == table_oid)
        .filter(|change| change.scope_key.as_ref() == scope_key)
        .cloned()
        .collect::<Vec<_>>();

    merge_changes_since(
        &scoped_changes,
        ChangeCursor {
            since_seq,
            limit: limit as usize,
        },
        oldest_available,
    )
    .map_err(Into::into)
}

/// Plans the hot mirror half of `koldstore.changes_since`.
///
/// # Errors
///
/// Returns an error when no primary-key columns are supplied, the scope column
/// is unsafe, or the SQL statement cannot be represented.
pub fn plan_mirror_changes_since(
    _table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key_columns: &[String],
    scope_column: Option<&str>,
) -> Result<MirrorChangesSincePlan, ChangeFeedError> {
    if primary_key_columns.is_empty() {
        return Err(ChangeFeedError::MissingPrimaryKey);
    }

    let mut additional_predicates = Vec::new();
    let mut additional_param_types = Vec::new();
    let scope_parameter_index = scope_column.map(|scope_column| {
        let predicate = scope_predicate_sql("mirror", scope_column, 2)
            .map_err(|_| ChangeFeedError::InvalidScopeColumn(scope_column.to_string()))?;
        additional_predicates.push(predicate);
        additional_param_types.push((2, SqlParamType::Text));
        Ok(2)
    });
    let scope_parameter_index = match scope_parameter_index {
        Some(Ok(index)) => Some(index),
        Some(Err(error)) => return Err(error),
        None => None,
    };

    let mirror = mirror_table
        .as_table_name()
        .map(MirrorRelation::new)
        .map_err(|error| ChangeFeedError::Sql(error.to_string()))?;
    let primary_key: Vec<&str> = primary_key_columns.iter().map(String::as_str).collect();
    let statement = mirror_to_sql(
        plan_select_mirror_rows_after_seq_with_params(
            &mirror,
            &primary_key,
            1,
            3,
            &additional_predicates,
            &additional_param_types,
        )
        .map_err(|error| ChangeFeedError::Sql(error.to_string()))?,
    )
    .map_err(|error| ChangeFeedError::Sql(error.to_string()))?;

    Ok(MirrorChangesSincePlan {
        statement,
        scope_parameter_index,
    })
}
