//! Hot/cold winner resolution.

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use koldstore_common::{ColdRow, CommitSeq, HotRow, LogicalPk, SeqId};

/// Row source for tie-breaking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowSource {
    Hot,
    Cold,
}

/// Resolved winner.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedRow {
    pub pk_json: serde_json::Value,
    pub source: RowSource,
    pub seq: SeqId,
    pub commit_seq: CommitSeq,
    pub row_image: serde_json::Value,
    pub deleted: bool,
}

/// Resolves hot and cold rows (borrowed inputs; clones row images).
#[must_use]
pub fn resolve_rows(hot: &[HotRow], cold: &[ColdRow]) -> Vec<ResolvedRow> {
    resolve_rows_owned(hot.to_vec(), cold.to_vec())
}

/// Resolves hot and cold rows, taking ownership to avoid per-candidate image clones.
///
/// Merge identity uses [`LogicalPk`] directly — canonical JSON is produced only
/// when winners leave the merge for the SQL/API boundary.
#[must_use]
pub fn resolve_rows_owned(hot: Vec<HotRow>, cold: Vec<ColdRow>) -> Vec<ResolvedRow> {
    struct Candidate {
        source: RowSource,
        seq: SeqId,
        commit_seq: CommitSeq,
        deleted: bool,
        row_image: serde_json::Value,
    }

    impl Candidate {
        fn beats(&self, other: &Self) -> bool {
            (self.seq, self.commit_seq) > (other.seq, other.commit_seq)
                || ((self.seq, self.commit_seq) == (other.seq, other.commit_seq)
                    && self.source == RowSource::Hot
                    && other.source == RowSource::Cold)
        }
    }

    let mut winners: HashMap<LogicalPk, Candidate> = HashMap::new();
    for row in cold {
        let candidate = Candidate {
            source: RowSource::Cold,
            seq: row.seq,
            commit_seq: row.commit_seq,
            deleted: row.deleted,
            row_image: row.row_image,
        };
        match winners.entry(row.pk) {
            Entry::Vacant(slot) => {
                slot.insert(candidate);
            }
            Entry::Occupied(mut slot) => {
                if candidate.beats(slot.get()) {
                    slot.insert(candidate);
                }
            }
        }
    }
    for row in hot {
        let candidate = Candidate {
            source: RowSource::Hot,
            seq: row.seq,
            commit_seq: row.commit_seq,
            deleted: row.deleted,
            row_image: row.row_image,
        };
        match winners.entry(row.pk) {
            Entry::Vacant(slot) => {
                slot.insert(candidate);
            }
            Entry::Occupied(mut slot) => {
                if candidate.beats(slot.get()) {
                    slot.insert(candidate);
                }
            }
        }
    }

    winners
        .into_iter()
        .filter(|(_, winner)| !winner.deleted)
        .map(|(pk, winner)| ResolvedRow {
            pk_json: pk.to_canonical_json(),
            source: winner.source,
            seq: winner.seq,
            commit_seq: winner.commit_seq,
            row_image: winner.row_image,
            deleted: winner.deleted,
        })
        .collect()
}
