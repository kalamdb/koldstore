//! Flush job state transitions.

use std::collections::BTreeMap;

use koldstore_core::{CommitSeq, Result, ScopeKey, SeqId, StablePkHash};
use koldstore_parquet::{ColumnStats, FooterSummary, RowGroupStats, SegmentFooterMetadata};

/// Manifest sync states used by flush jobs.
pub const FLUSH_STATES: &[&str] = &["pending_write", "syncing", "in_sync", "stale", "error"];

/// Local manifest cache sync state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestSyncState {
    /// Hot DML has dirtied the local scope.
    PendingWrite,
    /// Flush is publishing cold artifacts.
    Syncing,
    /// Local cache matches the object-store manifest.
    InSync,
    /// Local cache must be refreshed before planning cold reads.
    Stale,
    /// Last flush attempt failed.
    Error,
}

impl ManifestSyncState {
    /// Returns the SQL/catalog representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PendingWrite => "pending_write",
            Self::Syncing => "syncing",
            Self::InSync => "in_sync",
            Self::Stale => "stale",
            Self::Error => "error",
        }
    }

    /// Starts a flush for a pending, stale, or errored scope.
    #[must_use]
    pub const fn start_flush(self) -> Self {
        match self {
            Self::PendingWrite | Self::Stale | Self::Error => Self::Syncing,
            Self::Syncing | Self::InSync => self,
        }
    }

    /// Completes a successful flush.
    #[must_use]
    pub const fn finish_success(self, remaining_hot_rows: bool) -> Self {
        if remaining_hot_rows {
            Self::PendingWrite
        } else {
            Self::InSync
        }
    }

    /// Completes a failed flush.
    #[must_use]
    pub const fn finish_error(self) -> Self {
        Self::Error
    }
}

/// Flush metadata written after manifest commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdMetadataUpdate {
    /// Minimum `_seq`.
    pub min_seq: SeqId,
    /// Maximum `_seq`.
    pub max_seq: SeqId,
    /// Minimum `_commit_seq`.
    pub min_commit_seq: CommitSeq,
    /// Maximum `_commit_seq`.
    pub max_commit_seq: CommitSeq,
    /// Segment row count.
    pub row_count: u64,
    /// Segment byte size.
    pub byte_size: u64,
}

/// Hot row candidate read by a bounded flush scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HotRowCandidate {
    /// Stable logical PK hash.
    pub pk_hash: StablePkHash,
    /// Row/effect sequence.
    pub seq: SeqId,
    /// Commit-order cursor.
    pub commit_seq: CommitSeq,
    /// Whether this candidate is a hot tombstone.
    pub deleted: bool,
}

impl HotRowCandidate {
    /// Creates a live hot-row candidate.
    #[must_use]
    pub const fn live(pk_hash: StablePkHash, seq: SeqId, commit_seq: CommitSeq) -> Self {
        Self {
            pk_hash,
            seq,
            commit_seq,
            deleted: false,
        }
    }

    /// Creates a hot tombstone candidate.
    #[must_use]
    pub const fn tombstone(pk_hash: StablePkHash, seq: SeqId, commit_seq: CommitSeq) -> Self {
        Self {
            pk_hash,
            seq,
            commit_seq,
            deleted: true,
        }
    }
}

/// Input captured from a bounded hot-row flush scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushBatchInput {
    /// Maximum rows to scan in one batch.
    pub batch_size: usize,
    /// Candidate hot rows.
    pub rows: Vec<HotRowCandidate>,
}

impl FlushBatchInput {
    /// Resolves latest hot rows by PK and records batch continuation state.
    #[must_use]
    pub fn plan(self) -> FlushBatchPlan {
        let scanned_rows = self.rows.len();
        let mut latest_by_pk = BTreeMap::<StablePkHash, HotRowCandidate>::new();
        for row in self.rows {
            latest_by_pk
                .entry(row.pk_hash.clone())
                .and_modify(|existing| {
                    if (row.seq, row.commit_seq) > (existing.seq, existing.commit_seq) {
                        *existing = row.clone();
                    }
                })
                .or_insert(row);
        }
        let rows = latest_by_pk.into_values().collect::<Vec<_>>();
        let live_rows = rows.iter().filter(|row| !row.deleted).count();
        let tombstones_retained = rows.len() - live_rows;

        FlushBatchPlan {
            rows,
            live_rows,
            tombstones_retained,
            should_continue: should_continue_batch(scanned_rows, self.batch_size),
        }
    }
}

/// Planned flush batch after latest-version/tombstone resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushBatchPlan {
    /// Latest candidate per logical PK.
    pub rows: Vec<HotRowCandidate>,
    /// Live rows eligible for Parquet.
    pub live_rows: usize,
    /// Tombstones retained hot to mask older cold rows.
    pub tombstones_retained: usize,
    /// Whether another bounded batch should be scanned.
    pub should_continue: bool,
}

impl FlushBatchPlan {
    /// Builds a footer summary for live rows in this planned batch.
    #[must_use]
    pub fn footer_summary(&self) -> FooterSummary {
        let live_rows = self.rows.iter().filter(|row| !row.deleted);
        let min_seq = live_rows.clone().map(|row| row.seq.get()).min();
        let max_seq = live_rows.clone().map(|row| row.seq.get()).max();
        let min_commit_seq = live_rows.clone().map(|row| row.commit_seq.get()).min();
        let max_commit_seq = live_rows.map(|row| row.commit_seq.get()).max();

        FooterSummary {
            row_groups: vec![RowGroupStats {
                row_group: 0,
                min_seq,
                max_seq,
                min_commit_seq,
                max_commit_seq,
            }],
        }
    }
}

/// Planned `koldstore.cold_segments` catalog insertion after manifest commit.
#[derive(Debug, Clone, PartialEq)]
pub struct ColdSegmentCatalogInsert {
    /// Managed table oid.
    pub table_oid: u32,
    /// Optional user-scope key.
    pub scope_key: Option<ScopeKey>,
    /// Final Parquet object path.
    pub object_path: String,
    /// Minimum `_seq`.
    pub min_seq: SeqId,
    /// Maximum `_seq`.
    pub max_seq: SeqId,
    /// Minimum `_commit_seq`.
    pub min_commit_seq: CommitSeq,
    /// Maximum `_commit_seq`.
    pub max_commit_seq: CommitSeq,
    /// Segment row count.
    pub row_count: u64,
    /// Segment byte size.
    pub byte_size: u64,
    /// Segment schema version.
    pub schema_version: u32,
    /// Segment column stats.
    pub column_stats: BTreeMap<String, ColumnStats>,
    /// Active only after manifest commit.
    pub status: &'static str,
    /// Manifest identity that published this segment.
    pub manifest_etag: String,
}

/// Planned `koldstore.cold_pk_hints` catalog update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdPkHintUpdate {
    /// Managed table oid.
    pub table_oid: u32,
    /// Optional user-scope key.
    pub scope_key: Option<ScopeKey>,
    /// Stable logical PK hash.
    pub pk_hash: StablePkHash,
    /// Hint kind: `exact`, `bloom`, or `range`.
    pub hint_kind: String,
    /// Latest known cold `_seq`.
    pub latest_seq: SeqId,
    /// Latest known cold `_commit_seq`.
    pub latest_commit_seq: CommitSeq,
}

/// Failure plan for a flush attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushFailurePlan {
    /// Next local manifest cache state.
    pub next_manifest_state: ManifestSyncState,
    /// Whether hot heap data remains authoritative.
    pub hot_data_authoritative: bool,
    /// Job state recorded for operators.
    pub job_state: &'static str,
    /// Last error message.
    pub last_error: Option<String>,
}

impl FlushFailurePlan {
    /// Builds failure metadata for object-store outage.
    #[must_use]
    pub fn object_store_outage(error: impl Into<String>) -> Self {
        Self {
            next_manifest_state: ManifestSyncState::Error,
            hot_data_authoritative: true,
            job_state: "error",
            last_error: Some(error.into()),
        }
    }
}

/// Plans `koldstore.cold_segments` insertion from published footer metadata.
pub fn plan_cold_segment_insert(
    table_oid: u32,
    scope_key: Option<ScopeKey>,
    object_path: impl Into<String>,
    metadata: SegmentFooterMetadata,
    manifest_etag: impl Into<String>,
) -> Result<ColdSegmentCatalogInsert> {
    Ok(ColdSegmentCatalogInsert {
        table_oid,
        scope_key,
        object_path: object_path.into(),
        min_seq: SeqId::new(metadata.min_seq)?,
        max_seq: SeqId::new(metadata.max_seq)?,
        min_commit_seq: CommitSeq::new(metadata.min_commit_seq)?,
        max_commit_seq: CommitSeq::new(metadata.max_commit_seq)?,
        row_count: metadata.row_count,
        byte_size: metadata.byte_size,
        schema_version: metadata.schema_version,
        column_stats: metadata.column_stats,
        status: "active",
        manifest_etag: manifest_etag.into(),
    })
}

/// Plans local PK hint updates for live rows written to cold storage.
#[must_use]
pub fn plan_cold_pk_hint_updates(
    table_oid: u32,
    scope_key: Option<ScopeKey>,
    batch: &FlushBatchPlan,
    hint_kind: &str,
) -> Vec<ColdPkHintUpdate> {
    batch
        .rows
        .iter()
        .filter(|row| !row.deleted)
        .map(|row| ColdPkHintUpdate {
            table_oid,
            scope_key: scope_key.clone(),
            pk_hash: row.pk_hash.clone(),
            hint_kind: hint_kind.to_string(),
            latest_seq: row.seq,
            latest_commit_seq: row.commit_seq,
        })
        .collect()
}

/// Returns the next manifest sync state after a successful flush.
#[must_use]
pub const fn successful_flush_state(remaining_hot_rows: bool) -> &'static str {
    ManifestSyncState::Syncing
        .finish_success(remaining_hot_rows)
        .as_str()
}

/// Returns whether a bounded flush batch should continue.
#[must_use]
pub const fn should_continue_batch(scanned_rows: usize, batch_size: usize) -> bool {
    scanned_rows >= batch_size
}
