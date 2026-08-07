//! Seq-ordered change-feed cursor helpers.
//!
//! Pages advance by exclusive mirror/cold `seq`. Callers may see an older cold
//! version of a PK and a newer hot version on a later page — there is no
//! in-page latest-state collapse across sources.

use koldstore_common::{MirrorChange, SeqId};
use thiserror::Error;

/// Change cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeCursor {
    pub since_seq: i64,
    pub limit: usize,
}

/// Retention gap error.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("change records before sequence {oldest_available} are no longer retained")]
pub struct ChangeGap {
    pub oldest_available: i64,
}

/// Returns changes after the cursor in ascending `seq` order (no per-PK collapse).
pub fn changes_since(
    changes: &[MirrorChange],
    cursor: ChangeCursor,
    oldest_available: Option<SeqId>,
) -> Result<Vec<MirrorChange>, ChangeGap> {
    if let Some(oldest) = oldest_available {
        // `since_seq = 0` means "from the start of retained history".
        // Retention gaps apply only when a real cursor has fallen behind the floor.
        if cursor.since_seq > 0 && cursor.since_seq < oldest.get() - 1 {
            return Err(ChangeGap {
                oldest_available: oldest.get(),
            });
        }
    }

    let mut selected: Vec<MirrorChange> = changes
        .iter()
        .filter(|change| change.seq.get() > cursor.since_seq)
        .cloned()
        .collect();
    selected.sort_by_key(|change| change.seq);
    selected.truncate(cursor.limit);
    Ok(selected)
}

/// Returns the newest `limit` changes in ascending seq order.
///
/// Matches KalamDB `last_rows`: select by descending seq, then deliver
/// oldest→newest. Does not paginate into older history.
pub fn changes_last(changes: &[MirrorChange], limit: usize) -> Vec<MirrorChange> {
    if limit == 0 {
        return Vec::new();
    }
    let mut selected = changes.to_vec();
    selected.sort_by_key(|change| change.seq);
    if selected.len() > limit {
        selected = selected.split_off(selected.len() - limit);
    }
    selected
}
