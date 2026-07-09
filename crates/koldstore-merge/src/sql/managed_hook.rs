//! Managed hot-DML hook effect planning.
//!
//! Owns mirror stamp envelopes and cold-only DELETE routing helpers used by
//! PostgreSQL DML hooks. Mirror SQL expressions stay in `pg_koldstore`.

use koldstore_common::{CommitSeq, MirrorOperation, SeqId};
use koldstore_manifest::SyncState;

use crate::dml::{delete_decision, DeleteDecision, DmlStamp, ManagedDmlOperation};

/// Manifest cache state written after hot DML dirties a managed scope.
pub const HOT_DML_MANIFEST_SYNC_STATE: &str = SyncState::PendingWrite.as_str();

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

/// Returns whether standard SQL cold-only DELETE can use the exact local metadata path.
#[must_use]
pub const fn simple_pk_delete_supported(simple_pk_predicate: bool, exact_metadata: bool) -> bool {
    simple_pk_predicate && exact_metadata
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
#[must_use]
pub fn plan_managed_insert_effect(seq: SeqId, commit_seq: CommitSeq) -> ManagedDmlEffect {
    plan_managed_effect(
        seq,
        commit_seq,
        ManagedDmlOperation::Insert,
        MirrorOperation::Insert,
        None,
    )
}

/// Plans a managed UPDATE effect.
#[must_use]
pub fn plan_managed_update_effect(seq: SeqId, commit_seq: CommitSeq) -> ManagedDmlEffect {
    plan_managed_effect(
        seq,
        commit_seq,
        ManagedDmlOperation::Update,
        MirrorOperation::Update,
        None,
    )
}

/// Plans a managed DELETE effect from local cold PK metadata.
#[must_use]
pub fn plan_managed_delete_effect(
    seq: SeqId,
    commit_seq: CommitSeq,
    cold_may_contain_pk: bool,
) -> ManagedDmlEffect {
    plan_managed_effect(
        seq,
        commit_seq,
        ManagedDmlOperation::Delete,
        MirrorOperation::Delete,
        Some(delete_decision(cold_may_contain_pk)),
    )
}

fn plan_managed_effect(
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
