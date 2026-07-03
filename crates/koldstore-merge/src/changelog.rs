//! Row-event cursor helpers.

use koldstore_core::{CommitSeq, RowEvent};
use thiserror::Error;

/// Change cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeCursor {
    pub since_commit_seq: i64,
    pub limit: usize,
}

/// Retention gap error.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("change events before commit sequence {oldest_available} are no longer retained")]
pub struct ChangeGap {
    pub oldest_available: i64,
}

/// Returns events after the cursor in commit-sequence order.
pub fn changes_since(
    events: &[RowEvent],
    cursor: ChangeCursor,
    oldest_available: Option<CommitSeq>,
) -> Result<Vec<RowEvent>, ChangeGap> {
    if let Some(oldest) = oldest_available {
        if cursor.since_commit_seq < oldest.get() - 1 {
            return Err(ChangeGap {
                oldest_available: oldest.get(),
            });
        }
    }
    let mut selected: Vec<RowEvent> = events
        .iter()
        .filter(|event| event.commit_seq.get() > cursor.since_commit_seq)
        .cloned()
        .collect();
    selected.sort_by_key(|event| (event.commit_seq, event.seq));
    selected.truncate(cursor.limit);
    Ok(selected)
}
