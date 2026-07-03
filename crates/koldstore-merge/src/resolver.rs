//! Hot/cold winner resolution.

use std::collections::BTreeMap;

use koldstore_core::{ColdRow, CommitSeq, HotRow, LogicalPk, SeqId};

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

/// Resolves hot and cold rows.
#[must_use]
pub fn resolve_rows(hot: &[HotRow], cold: &[ColdRow]) -> Vec<ResolvedRow> {
    #[derive(Clone)]
    struct Candidate {
        source: RowSource,
        pk: LogicalPk,
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

    let mut winners: BTreeMap<String, Candidate> = BTreeMap::new();
    for row in cold {
        let key = row.pk.to_canonical_json().to_string();
        let candidate = Candidate {
            source: RowSource::Cold,
            pk: row.pk.clone(),
            seq: row.seq,
            commit_seq: row.commit_seq,
            deleted: row.deleted,
            row_image: row.row_image.clone(),
        };
        match winners.get(&key) {
            Some(existing) if !candidate.beats(existing) => {}
            _ => {
                winners.insert(key, candidate);
            }
        }
    }
    for row in hot {
        let key = row.pk.to_canonical_json().to_string();
        let candidate = Candidate {
            source: RowSource::Hot,
            pk: row.pk.clone(),
            seq: row.seq,
            commit_seq: row.commit_seq,
            deleted: row.deleted,
            row_image: row.row_image.clone(),
        };
        match winners.get(&key) {
            Some(existing) if !candidate.beats(existing) => {}
            _ => {
                winners.insert(key, candidate);
            }
        }
    }

    winners
        .into_values()
        .filter(|winner| !winner.deleted)
        .map(|winner| ResolvedRow {
            pk_json: winner.pk.to_canonical_json(),
            source: winner.source,
            seq: winner.seq,
            commit_seq: winner.commit_seq,
            row_image: winner.row_image,
            deleted: winner.deleted,
        })
        .collect()
}
