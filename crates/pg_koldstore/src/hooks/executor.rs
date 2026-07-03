//! DML hook and system-column guard integration.

use koldstore_core::{CommitSeq, RowOperation, ScopeKey, SeqId, TableKind};

use crate::security::scope::{self, ScopeError};
use crate::sql::dml::{delete_decision, DeleteDecision, DmlStamp, ManagedDmlOperation};

/// Manifest cache state written after hot DML dirties a managed scope.
pub const HOT_DML_MANIFEST_SYNC_STATE: &str = "pending_write";

/// Planned effect for one managed hot-DML operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedDmlEffect {
    /// System-column stamp for the hot row or tombstone effect.
    pub stamp: DmlStamp,
    /// Row event operation to append.
    pub row_event_operation: RowOperation,
    /// Manifest sync state to record locally.
    pub manifest_sync_state: &'static str,
    /// Delete route when the operation is DELETE.
    pub delete_decision: Option<DeleteDecision>,
    /// Whether this path preserves the one-hot-row-per-PK invariant.
    pub keeps_one_hot_row_per_pk: bool,
}

/// Returns whether a column is managed system metadata.
#[must_use]
pub fn is_system_column(name: &str) -> bool {
    matches!(name, "_seq" | "_commit_seq" | "_deleted" | "_user_id")
}

/// Returns whether a user write to a system column should be rejected.
#[must_use]
pub fn rejects_system_column_write(name: &str, internal_guard_active: bool) -> bool {
    is_system_column(name) && !internal_guard_active
}

/// DML operations observed by the managed hook shell.
#[must_use]
pub const fn managed_dml_hook_names() -> &'static [&'static str] {
    &["INSERT", "UPDATE", "DELETE", "COPY"]
}

/// Returns whether standard SQL cold-only DELETE can use the exact local metadata path.
#[must_use]
pub const fn simple_pk_delete_supported(simple_pk_predicate: bool, exact_metadata: bool) -> bool {
    simple_pk_predicate && exact_metadata
}

/// Enforces user-scope checks before managed DML touches heap rows or cold metadata.
///
/// # Errors
///
/// Returns a scope error when user-scoped DML is missing an active session scope,
/// has no row scope, or targets a different scope.
pub fn enforce_dml_scope(
    table_kind: TableKind,
    session_user_id: Option<&str>,
    row_scope: Option<&ScopeKey>,
) -> Result<Option<ScopeKey>, ScopeError> {
    let active_scope = scope::active_scope_for_table(table_kind, session_user_id)?;
    if let Some(active_scope) = active_scope.as_ref() {
        scope::enforce_row_scope(active_scope, row_scope)?;
    }
    Ok(active_scope)
}

/// Simple PK equality predicate extracted from a standard SQL DELETE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimplePkPredicate {
    /// PK column name.
    pub column: String,
    /// PK value.
    pub value: serde_json::Value,
}

impl SimplePkPredicate {
    /// Creates a simple PK predicate.
    #[must_use]
    pub fn new(column: impl Into<String>, value: serde_json::Value) -> Self {
        Self {
            column: column.into(),
            value,
        }
    }
}

/// Extracts a standard SQL cold-only DELETE route when exact local metadata is available.
#[must_use]
pub fn extract_simple_pk_delete_predicate(
    predicates: &[SimplePkPredicate],
    primary_key_columns: &[String],
    exact_metadata: bool,
) -> Option<SimplePkPredicate> {
    if !exact_metadata || predicates.len() != 1 || primary_key_columns.len() != 1 {
        return None;
    }
    let predicate = predicates.first()?;
    if predicate.column == primary_key_columns[0] {
        Some(predicate.clone())
    } else {
        None
    }
}

/// Plans a managed INSERT effect.
///
pub fn plan_managed_insert_effect(seq: SeqId, commit_seq: CommitSeq) -> ManagedDmlEffect {
    plan_effect(
        seq,
        commit_seq,
        ManagedDmlOperation::Insert,
        RowOperation::Insert,
        None,
    )
}

/// Plans a managed UPDATE effect.
///
pub fn plan_managed_update_effect(seq: SeqId, commit_seq: CommitSeq) -> ManagedDmlEffect {
    plan_effect(
        seq,
        commit_seq,
        ManagedDmlOperation::Update,
        RowOperation::Update,
        None,
    )
}

/// Plans a managed DELETE effect from local cold PK metadata.
///
pub fn plan_managed_delete_effect(
    seq: SeqId,
    commit_seq: CommitSeq,
    cold_may_contain_pk: bool,
) -> ManagedDmlEffect {
    plan_effect(
        seq,
        commit_seq,
        ManagedDmlOperation::Delete,
        RowOperation::Delete,
        Some(delete_decision(cold_may_contain_pk)),
    )
}

fn plan_effect(
    seq: SeqId,
    commit_seq: CommitSeq,
    operation: ManagedDmlOperation,
    row_event_operation: RowOperation,
    delete_decision: Option<DeleteDecision>,
) -> ManagedDmlEffect {
    let stamp = DmlStamp::new(seq, commit_seq, operation);
    ManagedDmlEffect {
        stamp,
        row_event_operation,
        manifest_sync_state: HOT_DML_MANIFEST_SYNC_STATE,
        delete_decision,
        keeps_one_hot_row_per_pk: operation.keeps_one_hot_row_per_pk(),
    }
}
