//! DML hook and clean-schema mirror integration.

use koldstore_common::{scope, MirrorOperation, ScopeError, ScopeKey, TableKind};
use koldstore_merge::ManagedDmlOperation;

pub use koldstore_merge::{
    extract_simple_pk_delete_predicate, plan_managed_delete_effect, plan_managed_insert_effect,
    plan_managed_update_effect, simple_pk_delete_supported, ManagedDmlEffect, SimplePkPredicate,
    HOT_DML_MANIFEST_SYNC_STATE,
};

/// Planned latest-state mirror effect for one user DML row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorCaptureEffect {
    /// Operation value written to the mirror.
    pub operation: MirrorOperation,
    /// SQL expression used to allocate the mirror sequence.
    pub seq_expression: &'static str,
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
        seq_expression: koldstore_common::snowflake_default_expression(),
        commit_lsn_expression: "pg_current_wal_lsn()",
        transactional: true,
    }
}
