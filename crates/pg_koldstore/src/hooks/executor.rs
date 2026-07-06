//! DML hook and clean-schema mirror integration.

use koldstore_core::{CommitSeq, MirrorOperation, ScopeKey, SeqId, TableKind};

use crate::security::scope::{self, ScopeError};
use crate::sql::dml::{delete_decision, DeleteDecision, DmlStamp, ManagedDmlOperation};

/// Manifest cache state written after hot DML dirties a managed scope.
pub const HOT_DML_MANIFEST_SYNC_STATE: &str = "pending_write";

/// Planned effect for one managed hot-DML operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedDmlEffect {
    /// Mirror sequence stamp for the hot row or tombstone effect.
    pub stamp: DmlStamp,
    /// Latest-state mirror operation to record.
    pub mirror_operation: MirrorOperation,
    /// Manifest sync state to record locally.
    pub manifest_sync_state: &'static str,
    /// Delete route when the operation is DELETE.
    pub delete_decision: Option<DeleteDecision>,
    /// Whether this path preserves the one-hot-row-per-PK invariant.
    pub keeps_one_hot_row_per_pk: bool,
}

/// Planned latest-state mirror effect for one user DML row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorCaptureEffect {
    /// Operation value written to the mirror.
    pub operation: MirrorOperation,
    /// SQL expression used to allocate the mirror sequence.
    pub seq_expression: &'static str,
    /// SQL expression used to stamp row age.
    pub changed_at_expression: &'static str,
    /// SQL expression used to capture diagnostic WAL position.
    pub commit_lsn_expression: &'static str,
    /// Whether the effect is coupled to the user transaction.
    pub transactional: bool,
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
        MirrorOperation::Insert,
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
        MirrorOperation::Update,
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
        MirrorOperation::Delete,
        Some(delete_decision(cold_may_contain_pk)),
    )
}

/// Plans the mirror state transition for a managed DML operation.
#[must_use]
pub const fn plan_mirror_capture_effect(operation: ManagedDmlOperation) -> MirrorCaptureEffect {
    let operation = match operation {
        ManagedDmlOperation::Insert | ManagedDmlOperation::Revive => MirrorOperation::Insert,
        ManagedDmlOperation::Update => MirrorOperation::Update,
        ManagedDmlOperation::Delete => MirrorOperation::Delete,
    };

    MirrorCaptureEffect {
        operation,
        seq_expression: "SNOWFLAKE_ID()",
        changed_at_expression: "now()",
        commit_lsn_expression: "pg_current_wal_lsn()",
        transactional: true,
    }
}

fn plan_effect(
    seq: SeqId,
    commit_seq: CommitSeq,
    operation: ManagedDmlOperation,
    mirror_operation: MirrorOperation,
    delete_decision: Option<DeleteDecision>,
) -> ManagedDmlEffect {
    let stamp = DmlStamp::new(seq, commit_seq, operation);
    ManagedDmlEffect {
        stamp,
        mirror_operation,
        manifest_sync_state: HOT_DML_MANIFEST_SYNC_STATE,
        delete_decision,
        keeps_one_hot_row_per_pk: operation.keeps_one_hot_row_per_pk(),
    }
}
