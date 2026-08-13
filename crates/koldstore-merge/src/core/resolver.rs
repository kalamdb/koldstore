//! Hot/cold winner resolution.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use koldstore_common::{ColdRow, HotRow, LogicalPk, LogicalPkValues, RowImage, SeqId};

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
    pub row_image: RowImage,
    pub deleted: bool,
}

struct Candidate {
    /// Retained only by streaming batches, then encoded for emitted winners.
    pk: Option<LogicalPk>,
    source: RowSource,
    seq: SeqId,
    deleted: bool,
    row_image: RowImage,
}

impl Candidate {
    fn beats(&self, other: &Self) -> bool {
        candidate_beats(self.seq, self.source, other.seq, other.source)
    }

    fn into_resolved(mut self, pk_json: Option<serde_json::Value>) -> ResolvedRow {
        ResolvedRow {
            pk_json: pk_json
                .or_else(|| self.pk.take().map(|pk| pk.to_canonical_json()))
                .expect("resolved candidates require canonical PK JSON"),
            source: self.source,
            seq: self.seq,
            row_image: self.row_image,
            deleted: self.deleted,
        }
    }
}

struct BorrowedCandidate<'a> {
    source: RowSource,
    seq: SeqId,
    deleted: bool,
    row_image: &'a RowImage,
}

impl BorrowedCandidate<'_> {
    fn beats(&self, other: &Self) -> bool {
        candidate_beats(self.seq, self.source, other.seq, other.source)
    }

    fn to_resolved(&self, pk: &LogicalPk) -> ResolvedRow {
        ResolvedRow {
            pk_json: pk.to_canonical_json(),
            source: self.source,
            seq: self.seq,
            row_image: self.row_image.clone(),
            deleted: self.deleted,
        }
    }
}

fn candidate_beats(
    candidate_seq: SeqId,
    candidate_source: RowSource,
    current_seq: SeqId,
    current_source: RowSource,
) -> bool {
    candidate_seq > current_seq
        || (candidate_seq == current_seq
            && candidate_source == RowSource::Hot
            && current_source == RowSource::Cold)
}

/// Resolves borrowed hot and cold rows, cloning payloads only for final winners.
#[must_use]
pub fn resolve_rows(hot: &[HotRow], cold: &[ColdRow]) -> Vec<ResolvedRow> {
    let mut winners: HashMap<&LogicalPk, BorrowedCandidate<'_>> =
        HashMap::with_capacity(hot.len().max(cold.len()));
    for row in cold {
        insert_borrowed_candidate(
            &mut winners,
            &row.pk,
            BorrowedCandidate {
                source: RowSource::Cold,
                seq: row.seq,
                deleted: row.deleted,
                row_image: &row.row_image,
            },
        );
    }
    for row in hot {
        insert_borrowed_candidate(
            &mut winners,
            &row.pk,
            BorrowedCandidate {
                source: RowSource::Hot,
                seq: row.seq,
                deleted: row.deleted,
                row_image: &row.row_image,
            },
        );
    }

    winners
        .into_iter()
        .filter(|(_, winner)| !winner.deleted)
        .map(|(pk, winner)| winner.to_resolved(pk))
        .collect()
}

/// Resolves hot and cold rows, taking ownership to avoid per-candidate image clones.
///
/// Merge identity uses [`LogicalPk`] directly — canonical JSON is produced only
/// when winners leave the merge for the SQL/API boundary.
#[must_use]
pub fn resolve_rows_owned(hot: Vec<HotRow>, cold: Vec<ColdRow>) -> Vec<ResolvedRow> {
    let mut winners: HashMap<LogicalPk, Candidate> =
        HashMap::with_capacity(hot.len().max(cold.len()));
    for row in cold {
        insert_owned_candidate(
            &mut winners,
            row.pk,
            Candidate {
                pk: None,
                source: RowSource::Cold,
                seq: row.seq,
                deleted: row.deleted,
                row_image: row.row_image,
            },
        );
    }
    for row in hot {
        insert_owned_candidate(
            &mut winners,
            row.pk,
            Candidate {
                pk: None,
                source: RowSource::Hot,
                seq: row.seq,
                deleted: row.deleted,
                row_image: row.row_image,
            },
        );
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
    ///
    /// Winners are returned in input encounter order so ordered progressive
    /// paths can stream Index Scan / pathkey order without an external Sort.
    pub fn resolve_hot_batch(
        &mut self,
        rows: Vec<HotRow>,
    ) -> Result<Vec<ResolvedRow>, SeenKeyLimitExceeded> {
        self.resolve_batch(rows, RowSource::Hot, |row| {
            (row.pk, row.seq, row.deleted, row.row_image)
        })
    }

    /// Resolves one cold batch older than every previously supplied batch.
    ///
    /// Winners preserve batch encounter order (segment decode order).
    pub fn resolve_cold_batch(
        &mut self,
        rows: Vec<ColdRow>,
    ) -> Result<Vec<ResolvedRow>, SeenKeyLimitExceeded> {
        self.resolve_batch(rows, RowSource::Cold, |row| {
            (row.pk, row.seq, row.deleted, row.row_image)
        })
    }

    fn resolve_batch<R>(
        &mut self,
        rows: Vec<R>,
        source: RowSource,
        into_parts: impl Fn(R) -> (LogicalPk, SeqId, bool, RowImage),
    ) -> Result<Vec<ResolvedRow>, SeenKeyLimitExceeded> {
        let mut winners = HashMap::with_capacity(rows.len());
        let mut order = Vec::with_capacity(rows.len());
        for row in rows {
            let (pk, seq, deleted, row_image) = into_parts(row);
            let identity = pk.clone().into_values();
            let is_new = insert_candidate(
                &mut winners,
                identity.clone(),
                Candidate {
                    pk: Some(pk),
                    source,
                    seq,
                    deleted,
                    row_image,
                },
            );
            if is_new {
                order.push(identity);
            }
        }
        self.take_unseen_ordered(winners, order)
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

    fn take_unseen_ordered(
        &mut self,
        mut winners: HashMap<LogicalPkValues, Candidate>,
        order: Vec<LogicalPkValues>,
    ) -> Result<Vec<ResolvedRow>, SeenKeyLimitExceeded> {
        let mut resolved = Vec::with_capacity(order.len());
        for identity in order {
            let Some(winner) = winners.remove(&identity) else {
                continue;
            };
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

fn insert_borrowed_candidate<'a>(
    winners: &mut HashMap<&'a LogicalPk, BorrowedCandidate<'a>>,
    pk: &'a LogicalPk,
    candidate: BorrowedCandidate<'a>,
) {
    match winners.entry(pk) {
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

fn insert_owned_candidate(
    winners: &mut HashMap<LogicalPk, Candidate>,
    pk: LogicalPk,
    candidate: Candidate,
) {
    match winners.entry(pk) {
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

fn insert_candidate(
    winners: &mut HashMap<LogicalPkValues, Candidate>,
    identity: LogicalPkValues,
    candidate: Candidate,
) -> bool {
    match winners.entry(identity) {
        Entry::Vacant(slot) => {
            slot.insert(candidate);
            true
        }
        Entry::Occupied(mut slot) => {
            if candidate.beats(slot.get()) {
                slot.insert(candidate);
            }
            false
        }
    }
}
