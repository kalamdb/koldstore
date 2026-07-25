//! Hot/cold winner resolution.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use koldstore_common::{ColdRow, CommitSeq, HotRow, LogicalPk, LogicalPkValues, SeqId};

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

struct Candidate {
    pk_json: Option<serde_json::Value>,
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

    fn into_resolved(mut self, pk_json: Option<serde_json::Value>) -> ResolvedRow {
        ResolvedRow {
            pk_json: pk_json
                .or_else(|| self.pk_json.take())
                .expect("resolved candidates require canonical PK JSON"),
            source: self.source,
            seq: self.seq,
            commit_seq: self.commit_seq,
            row_image: self.row_image,
            deleted: self.deleted,
        }
    }
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
pub(crate) fn resolve_rows_owned(hot: Vec<HotRow>, cold: Vec<ColdRow>) -> Vec<ResolvedRow> {
    let mut winners: HashMap<LogicalPk, Candidate> = HashMap::new();
    for row in cold {
        let candidate = Candidate {
            pk_json: None,
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
            pk_json: None,
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
        .map(|(pk, winner)| winner.into_resolved(Some(pk.to_canonical_json())))
        .collect()
}

/// Stateful exact winner resolver for hot-first, newest-to-oldest row batches.
///
/// Persisted state contains only value-only PK identities. Row payloads are
/// retained only while resolving the current batch and are moved into returned
/// winners, allowing the executor to drop each payload after tuple emission.
#[derive(Debug, Clone, Default)]
pub struct NewestFirstWinnerResolver {
    masked: HashSet<LogicalPkValues>,
    seen: HashSet<LogicalPkValues>,
    /// Optional fail-closed cap on distinct identities retained by this scan.
    max_seen_keys: Option<usize>,
}

/// Raised when a scan would retain more exact PK identities than allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeenKeyLimitExceeded {
    /// Configured maximum distinct identities for this scan.
    pub limit: usize,
    /// Distinct identities already retained when the limit was hit.
    pub seen: usize,
}

impl std::fmt::Display for SeenKeyLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "merge seen-key limit exceeded (limit={}, seen={})",
            self.limit, self.seen
        )
    }
}

impl std::error::Error for SeenKeyLimitExceeded {}

impl NewestFirstWinnerResolver {
    /// Creates a resolver pre-masked by mirror tombstone keys.
    #[must_use]
    pub fn new(masked_pks: impl IntoIterator<Item = LogicalPk>) -> Self {
        let masked = masked_pks
            .into_iter()
            .map(LogicalPk::into_values)
            .collect::<HashSet<_>>();
        Self {
            seen: masked.clone(),
            masked,
            max_seen_keys: None,
        }
    }

    /// Caps distinct PK identities retained by this resolver (`None` / `0` = unlimited).
    #[must_use]
    pub fn with_max_seen_keys(mut self, max_seen_keys: Option<usize>) -> Self {
        self.max_seen_keys = max_seen_keys.filter(|limit| *limit > 0);
        self
    }

    /// Resolves a hot batch before any cold batches are supplied.
    pub fn resolve_hot_batch(
        &mut self,
        rows: Vec<HotRow>,
    ) -> Result<Vec<ResolvedRow>, SeenKeyLimitExceeded> {
        let mut winners = HashMap::with_capacity(rows.len());
        for row in rows {
            insert_candidate(
                &mut winners,
                row.pk,
                Candidate {
                    pk_json: None,
                    source: RowSource::Hot,
                    seq: row.seq,
                    commit_seq: row.commit_seq,
                    deleted: row.deleted,
                    row_image: row.row_image,
                },
            );
        }
        self.take_unseen(winners)
    }

    /// Resolves one cold batch older than every previously supplied batch.
    pub fn resolve_cold_batch(
        &mut self,
        rows: Vec<ColdRow>,
    ) -> Result<Vec<ResolvedRow>, SeenKeyLimitExceeded> {
        let mut winners = HashMap::with_capacity(rows.len());
        for row in rows {
            insert_candidate(
                &mut winners,
                row.pk,
                Candidate {
                    pk_json: None,
                    source: RowSource::Cold,
                    seq: row.seq,
                    commit_seq: row.commit_seq,
                    deleted: row.deleted,
                    row_image: row.row_image,
                },
            );
        }
        self.take_unseen(winners)
    }

    /// Masks keys only for batches supplied after this call.
    ///
    /// This is used for mirror tombstones: a tombstone suppresses stale cold
    /// versions, but must not suppress a live hot winner already resolved.
    pub fn mask_older_pks(
        &mut self,
        pks: impl IntoIterator<Item = LogicalPk>,
    ) -> Result<(), SeenKeyLimitExceeded> {
        for pk in pks {
            self.insert_seen(pk.into_values())?;
        }
        Ok(())
    }

    /// Returns the number of exact PK identities retained by this scan.
    #[must_use]
    pub fn seen_key_count(&self) -> usize {
        self.seen.len()
    }

    /// Makes the current exact identity set the baseline restored by [`Self::reset`].
    pub fn checkpoint(&mut self) {
        self.masked.clone_from(&self.seen);
    }

    /// Restores the initial mirror mask for executor rescan.
    pub fn reset(&mut self) {
        self.seen.clone_from(&self.masked);
    }

    fn take_unseen(
        &mut self,
        winners: HashMap<LogicalPkValues, Candidate>,
    ) -> Result<Vec<ResolvedRow>, SeenKeyLimitExceeded> {
        let mut resolved = Vec::with_capacity(winners.len());
        for (identity, winner) in winners {
            if winner.deleted {
                self.insert_seen(identity)?;
                continue;
            }
            if !self.insert_seen(identity)? {
                continue;
            }
            resolved.push(winner.into_resolved(None));
        }
        Ok(resolved)
    }

    /// Inserts `identity` into `seen`.
    ///
    /// Returns `Ok(true)` when the identity is newly retained, `Ok(false)` when it
    /// was already present, and `Err` when retaining it would exceed the cap.
    fn insert_seen(&mut self, identity: LogicalPkValues) -> Result<bool, SeenKeyLimitExceeded> {
        if self.seen.contains(&identity) {
            return Ok(false);
        }
        if let Some(limit) = self.max_seen_keys {
            if self.seen.len() >= limit {
                return Err(SeenKeyLimitExceeded {
                    limit,
                    seen: self.seen.len(),
                });
            }
        }
        self.seen.insert(identity);
        Ok(true)
    }
}

fn insert_candidate(
    winners: &mut HashMap<LogicalPkValues, Candidate>,
    pk: LogicalPk,
    mut candidate: Candidate,
) {
    candidate.pk_json = Some(pk.to_canonical_json());
    match winners.entry(pk.into_values()) {
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
