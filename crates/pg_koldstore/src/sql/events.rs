//! Row-event SQL helpers.

use koldstore_core::{CommitSeq, LogicalPk, RowEvent, RowOperation, ScopeKey, SeqId, StablePkHash};
use koldstore_merge::{changes_since as merge_changes_since, ChangeCursor, ChangeGap};
use thiserror::Error;

/// Default changes_since limit.
pub const DEFAULT_CHANGE_LIMIT: i32 = 1000;

/// Change-feed helper error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ChangeFeedError {
    /// `limit_rows` must be greater than zero.
    #[error("limit_rows must be positive")]
    InvalidLimit,
    /// Requested cursor is older than retained events.
    #[error(transparent)]
    RetentionGap(#[from] ChangeGap),
}

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

/// Builds the row event emitted when a cold-only delete writes a PK-only tombstone.
#[must_use]
pub fn append_cold_only_tombstone_event(
    table_oid: u32,
    scope_key: Option<koldstore_core::ScopeKey>,
    pk: &LogicalPk,
    seq: SeqId,
    commit_seq: CommitSeq,
) -> RowEvent {
    append_row_event(
        table_oid,
        scope_key,
        pk,
        RowOperation::Delete,
        seq,
        commit_seq,
        None,
    )
}

/// Returns change events for one table/scope after a commit cursor.
///
/// # Errors
///
/// Returns [`ChangeFeedError::InvalidLimit`] when `limit_rows <= 0`, or a
/// retention gap when `since_commit_seq` is older than retained events.
pub fn changes_since(
    events: &[RowEvent],
    table_oid: u32,
    scope_key: Option<&ScopeKey>,
    since_commit_seq: i64,
    limit_rows: Option<i32>,
    oldest_available: Option<CommitSeq>,
) -> Result<Vec<RowEvent>, ChangeFeedError> {
    let limit = limit_rows.unwrap_or(DEFAULT_CHANGE_LIMIT);
    if limit <= 0 {
        return Err(ChangeFeedError::InvalidLimit);
    }

    let scoped_events = events
        .iter()
        .filter(|event| event.table_oid == table_oid)
        .filter(|event| event.scope_key.as_ref() == scope_key)
        .cloned()
        .collect::<Vec<_>>();

    merge_changes_since(
        &scoped_events,
        ChangeCursor {
            since_commit_seq,
            limit: limit as usize,
        },
        oldest_available,
    )
    .map_err(Into::into)
}

/// Retains the latest N row events for one table/scope in commit-sequence order.
#[must_use]
pub fn purge_retained_events(
    events: &[RowEvent],
    table_oid: u32,
    scope_key: Option<&ScopeKey>,
    retain_latest: usize,
) -> Vec<RowEvent> {
    let mut scoped_events = events
        .iter()
        .filter(|event| event.table_oid == table_oid)
        .filter(|event| event.scope_key.as_ref() == scope_key)
        .cloned()
        .collect::<Vec<_>>();
    scoped_events.sort_by_key(|event| event.commit_seq);
    if scoped_events.len() > retain_latest {
        scoped_events.drain(..scoped_events.len() - retain_latest);
    }
    scoped_events
}

/// Returns the oldest retained commit sequence from an already-retained event set.
#[must_use]
pub fn oldest_retained_commit_seq(events: &[RowEvent]) -> Option<CommitSeq> {
    events.iter().map(|event| event.commit_seq).min()
}
