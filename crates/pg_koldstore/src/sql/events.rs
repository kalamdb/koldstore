//! Row-event SQL helpers.

use koldstore_core::{CommitSeq, LogicalPk, RowEvent, RowOperation, SeqId, StablePkHash};

/// Default changes_since limit.
pub const DEFAULT_CHANGE_LIMIT: i32 = 1000;

/// Builds a row event from DML metadata.
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn append_row_event(
    table_oid: u32,
    scope_key: Option<koldstore_core::ScopeKey>,
    pk: &LogicalPk,
    op: RowOperation,
    seq: SeqId,
    commit_seq: CommitSeq,
    row_image_json: Option<serde_json::Value>,
) -> RowEvent {
    RowEvent {
        table_oid,
        scope_key,
        pk_hash: StablePkHash::compute(pk),
        pk_json: pk.to_canonical_json(),
        op,
        seq,
        commit_seq,
        deleted: matches!(op, RowOperation::Delete),
        row_image_json,
        created_at: chrono::Utc::now(),
    }
}
