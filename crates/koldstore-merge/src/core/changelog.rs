//! Latest-state change-feed cursor helpers.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

use koldstore_common::{ChangeSource, MirrorChange, SeqId};
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

/// Returns latest-state changes after the cursor in mirror-sequence order.
pub fn changes_since(
    changes: &[MirrorChange],
    cursor: ChangeCursor,
    oldest_available: Option<SeqId>,
) -> Result<Vec<MirrorChange>, ChangeGap> {
    if let Some(oldest) = oldest_available {
        if cursor.since_seq < oldest.get() - 1 {
            return Err(ChangeGap {
                oldest_available: oldest.get(),
            });
        }
    }

    let mut latest_by_pk = BTreeMap::<String, MirrorChange>::new();
    for change in changes
        .iter()
        .filter(|change| change.seq.get() > cursor.since_seq)
    {
        let key = format!(
            "{}:{}",
            change.scope_key.as_ref().map_or("", |scope| scope.as_str()),
            change.pk_json
        );
        match latest_by_pk.entry(key) {
            Entry::Occupied(mut occupied) => {
                if change_beats(change, occupied.get()) {
                    *occupied.get_mut() = change.clone();
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(change.clone());
            }
        }
    }

    let mut selected = latest_by_pk.into_values().collect::<Vec<_>>();
    selected.sort_by_key(|change| change.seq);
    selected.truncate(cursor.limit);
    Ok(selected)
}

fn change_beats(candidate: &MirrorChange, existing: &MirrorChange) -> bool {
    candidate.seq > existing.seq
        || (candidate.seq == existing.seq
            && candidate.source == ChangeSource::HotMirror
            && existing.source == ChangeSource::ColdRecord)
}
