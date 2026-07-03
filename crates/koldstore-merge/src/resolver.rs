//! Hot/cold winner resolution.

use std::collections::BTreeMap;

use koldstore_core::{ColdRow, HotRow};

/// Row source for tie-breaking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowSource {
    Hot,
    Cold,
}

/// Resolved winner.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedRow {
    pub source: RowSource,
    pub row_image: serde_json::Value,
    pub deleted: bool,
}

/// Resolves hot and cold rows.
#[must_use]
pub fn resolve_rows(hot: &[HotRow], cold: &[ColdRow]) -> Vec<ResolvedRow> {
    #[derive(Clone)]
    struct Candidate {
        source: RowSource,
        seq: i64,
        commit_seq: i64,
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
            seq: row.seq.get(),
            commit_seq: row.commit_seq.get(),
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
            seq: row.seq.get(),
            commit_seq: row.commit_seq.get(),
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
            source: winner.source,
            row_image: winner.row_image,
            deleted: winner.deleted,
        })
        .collect()
}
