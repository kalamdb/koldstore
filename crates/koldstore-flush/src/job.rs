//! Flush job state transitions.

use std::{
    cmp::Ordering,
    collections::{btree_map::Entry, BTreeMap},
    num::NonZeroUsize,
};

use koldstore_catalog::HintKind;
use koldstore_common::{
    compare_json_values, ColumnId, CommitSeq, KoldstoreError, MirrorOperation, Result, ScopeKey,
    SeqId, StablePkHash,
};
use koldstore_jobs::{LeaseEpoch, LeaseSeconds};
use koldstore_manifest::SyncState;
use koldstore_parquet::{ColumnStats, FooterSummary, RowGroupStats, SegmentFooterMetadata};
use koldstore_schema::MirrorInitializationState;
use uuid::Uuid;

/// Manifest sync state alias used by flush orchestration.
pub use koldstore_manifest::SyncState as ManifestSyncState;

/// Returns whether a managed table can be selected for flush.
#[must_use]
pub const fn allows_flush_after_initialization(state: MirrorInitializationState) -> bool {
    matches!(state, MirrorInitializationState::Complete)
}

/// Positive lease duration for a claimed flush job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushLeaseSeconds(LeaseSeconds);

impl FlushLeaseSeconds {
    /// Creates a positive lease duration.
    ///
    /// # Errors
    ///
    /// Returns an error when the duration is zero.
    pub fn new(value: u32) -> Result<Self> {
        let Some(value) = LeaseSeconds::new(value) else {
            return Err(KoldstoreError::InvalidSequence {
                field: "lease_seconds",
                value: 0,
            });
        };
        Ok(Self(value))
    }

    /// Returns the raw seconds value.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Monotonic lease epoch used to fence stale workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobLeaseEpoch(LeaseEpoch);

impl JobLeaseEpoch {
    /// Creates a non-negative lease epoch.
    ///
    /// # Errors
    ///
    /// Returns an error when the epoch is negative.
    pub const fn new(value: i64) -> Result<Self> {
        if let Some(value) = LeaseEpoch::new(value) {
            Ok(Self(value))
        } else {
            Err(KoldstoreError::InvalidSequence {
                field: "lease_epoch",
                value,
            })
        }
    }

    /// Returns the raw epoch value.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0.get()
    }
}

/// Claimed flush job lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushJobLease {
    /// Job id.
    pub job_id: Uuid,
    /// Worker/backend that owns the current lease.
    pub lease_owner: Uuid,
    /// Lease epoch observed by the worker.
    pub lease_epoch: JobLeaseEpoch,
    /// Lease duration.
    pub lease_seconds: FlushLeaseSeconds,
}

impl FlushJobLease {
    /// Creates a claimed lease.
    #[must_use]
    pub const fn new(
        job_id: Uuid,
        lease_owner: Uuid,
        lease_epoch: JobLeaseEpoch,
        lease_seconds: FlushLeaseSeconds,
    ) -> Self {
        Self {
            job_id,
            lease_owner,
            lease_epoch,
            lease_seconds,
        }
    }

    /// Returns whether a progress update belongs to this live lease.
    #[must_use]
    pub const fn matches(self, lease_owner: Uuid, lease_epoch: JobLeaseEpoch) -> bool {
        self.lease_owner.as_u128() == lease_owner.as_u128()
            && self.lease_epoch.get() == lease_epoch.get()
    }
}

/// Bounded flush execution settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushExecutionConfig {
    max_rows_per_batch: NonZeroUsize,
    max_bytes_per_batch: u64,
    max_batches_per_run: NonZeroUsize,
    lease_seconds: FlushLeaseSeconds,
}

impl FlushExecutionConfig {
    /// Creates validated flush execution settings.
    ///
    /// # Errors
    ///
    /// Returns an error when any bounded setting is zero.
    pub fn new(
        max_rows_per_batch: usize,
        max_bytes_per_batch: u64,
        max_batches_per_run: usize,
        lease_seconds: u32,
    ) -> Result<Self> {
        let Some(max_rows_per_batch) = NonZeroUsize::new(max_rows_per_batch) else {
            return Err(KoldstoreError::InvalidSequence {
                field: "max_rows_per_batch",
                value: 0,
            });
        };
        if max_bytes_per_batch == 0 {
            return Err(KoldstoreError::InvalidSequence {
                field: "max_bytes_per_batch",
                value: 0,
            });
        }
        let Some(max_batches_per_run) = NonZeroUsize::new(max_batches_per_run) else {
            return Err(KoldstoreError::InvalidSequence {
                field: "max_batches_per_run",
                value: 0,
            });
        };

        Ok(Self {
            max_rows_per_batch,
            max_bytes_per_batch,
            max_batches_per_run,
            lease_seconds: FlushLeaseSeconds::new(lease_seconds)?,
        })
    }

    /// Maximum candidate rows buffered for a batch.
    #[must_use]
    pub const fn max_rows_per_batch(self) -> usize {
        self.max_rows_per_batch.get()
    }

    /// Maximum estimated row bytes buffered for a batch.
    #[must_use]
    pub const fn max_bytes_per_batch(self) -> u64 {
        self.max_bytes_per_batch
    }

    /// Maximum batches a worker should process before releasing the job.
    #[must_use]
    pub const fn max_batches_per_run(self) -> usize {
        self.max_batches_per_run.get()
    }

    /// Lease duration.
    #[must_use]
    pub const fn lease_seconds(self) -> FlushLeaseSeconds {
        self.lease_seconds
    }
}

/// Result of trying to append a row to a bounded flush batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushBatchPush {
    /// Row was accepted into the current batch.
    Accepted,
    /// Batch is full and the caller should flush/finish it first.
    Full,
}

/// Bounded hot-row batch builder.
#[derive(Debug, Clone)]
pub struct FlushBatchBuilder {
    config: FlushExecutionConfig,
    rows: Vec<HotRowCandidate>,
    estimated_bytes: u64,
}

impl FlushBatchBuilder {
    /// Creates a batch builder with bounded preallocation.
    #[must_use]
    pub fn new(config: FlushExecutionConfig) -> Self {
        Self {
            config,
            rows: Vec::with_capacity(config.max_rows_per_batch().min(1024)),
            estimated_bytes: 0,
        }
    }

    /// Attempts to append one row without exceeding configured bounds.
    pub fn push(&mut self, row: HotRowCandidate, estimated_row_bytes: u64) -> FlushBatchPush {
        if self.rows.len() >= self.config.max_rows_per_batch() {
            return FlushBatchPush::Full;
        }

        let would_exceed_bytes = self.estimated_bytes.saturating_add(estimated_row_bytes)
            > self.config.max_bytes_per_batch();
        if would_exceed_bytes && !self.rows.is_empty() {
            return FlushBatchPush::Full;
        }

        self.rows.push(row);
        self.estimated_bytes = self.estimated_bytes.saturating_add(estimated_row_bytes);
        FlushBatchPush::Accepted
    }

    /// Finishes the builder into a flush-batch input.
    #[must_use]
    pub fn finish(self) -> FlushBatchInput {
        FlushBatchInput {
            batch_size: self.config.max_rows_per_batch(),
            rows: self.rows,
        }
    }
}

/// Sequence upper bound captured when a flush job claims a scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushWatermark {
    seq_upper_bound: SeqId,
}

/// One selected mirror row captured for a clean-schema flush job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorFlushSelectedRow {
    /// JSON primary-key identity.
    pub pk_json: serde_json::Value,
    /// Mirror sequence selected for this attempt.
    pub seq: SeqId,
    /// Latest mirror operation selected for this attempt.
    pub operation: MirrorOperation,
}

/// Stable selected mirror set persisted or carried by a flush job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorFlushSelectionSet {
    /// Selected rows in stable sequence order.
    pub rows: Vec<MirrorFlushSelectedRow>,
    /// Highest selected sequence.
    pub seq_cutoff: Option<SeqId>,
}

impl MirrorFlushSelectionSet {
    /// Creates a stable selected set sorted by mirror sequence.
    #[must_use]
    pub fn new(mut rows: Vec<MirrorFlushSelectedRow>) -> Self {
        rows.sort_by_key(|row| row.seq);
        let seq_cutoff = rows.iter().map(|row| row.seq).max();
        Self { rows, seq_cutoff }
    }

    /// Serializes the selected set for job payload or cleanup SQL binding.
    #[must_use]
    pub fn to_payload_json(&self) -> serde_json::Value {
        serde_json::Value::Array(
            self.rows
                .iter()
                .map(|row| {
                    let mut object = match &row.pk_json {
                        serde_json::Value::Object(object) => object.clone(),
                        _ => serde_json::Map::new(),
                    };
                    object.insert("seq".to_string(), serde_json::json!(row.seq.get()));
                    object.insert("op".to_string(), serde_json::json!(row.operation.code()));
                    serde_json::Value::Object(object)
                })
                .collect(),
        )
    }
}

impl FlushWatermark {
    /// Creates a flush watermark from a committed sequence upper bound.
    #[must_use]
    pub const fn new(seq_upper_bound: SeqId) -> Self {
        Self { seq_upper_bound }
    }

    /// Returns the sequence upper bound.
    #[must_use]
    pub const fn seq_upper_bound(self) -> SeqId {
        self.seq_upper_bound
    }

    /// Returns whether a hot candidate belongs to this flush attempt.
    #[must_use]
    pub fn includes(self, row: &HotRowCandidate) -> bool {
        row.seq <= self.seq_upper_bound
    }
}

/// Durable phase for a flush job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushJobPhase {
    /// Job has been claimed and leased.
    Claimed,
    /// Hot rows are being scanned under the flush watermark.
    ScanHotRows,
    /// Parquet data is being written to a temp object.
    WriteParquetTemp,
    /// Temp object is being copied/validated as final.
    PublishFinalObject,
    /// PostgreSQL segment catalog rows are committed before manifest visibility.
    CommitCatalog,
    /// Manifest object and manifest catalog row become the visibility boundary.
    PublishManifest,
    /// Hot heap rows are being conditionally cleaned up.
    CleanupHotRows,
    /// Job finished.
    Finished,
}

impl FlushJobPhase {
    /// Returns the catalog representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::ScanHotRows => "scan_hot_rows",
            Self::WriteParquetTemp => "write_parquet_temp",
            Self::PublishFinalObject => "publish_final_object",
            Self::CommitCatalog => "commit_catalog",
            Self::PublishManifest => "publish_manifest",
            Self::CleanupHotRows => "cleanup_hot_rows",
            Self::Finished => "finished",
        }
    }
}

/// Returns the durable flush phase order.
#[must_use]
pub const fn flush_execution_phases() -> &'static [FlushJobPhase] {
    &[
        FlushJobPhase::Claimed,
        FlushJobPhase::ScanHotRows,
        FlushJobPhase::WriteParquetTemp,
        FlushJobPhase::PublishFinalObject,
        FlushJobPhase::CommitCatalog,
        FlushJobPhase::PublishManifest,
        FlushJobPhase::CleanupHotRows,
        FlushJobPhase::Finished,
    ]
}

/// Returns whether hot cleanup may run in the given phase.
#[must_use]
pub const fn can_cleanup_hot_rows(phase: FlushJobPhase) -> bool {
    matches!(
        phase,
        FlushJobPhase::CleanupHotRows | FlushJobPhase::Finished
    )
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
    /// Optional app/system column values used to compute cold stats.
    pub column_values: BTreeMap<String, serde_json::Value>,
}

impl HotRowCandidate {
    /// Creates a live hot-row candidate.
    #[must_use]
    pub fn live(pk_hash: StablePkHash, seq: SeqId, commit_seq: CommitSeq) -> Self {
        Self {
            pk_hash,
            seq,
            commit_seq,
            deleted: false,
            column_values: BTreeMap::new(),
        }
    }

    /// Creates a hot tombstone candidate.
    #[must_use]
    pub fn tombstone(pk_hash: StablePkHash, seq: SeqId, commit_seq: CommitSeq) -> Self {
        Self {
            pk_hash,
            seq,
            commit_seq,
            deleted: true,
            column_values: BTreeMap::new(),
        }
    }

    /// Attaches column values captured for the flushed row image.
    #[must_use]
    pub fn with_column_values<I, K>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = (K, serde_json::Value)>,
        K: Into<String>,
    {
        self.column_values = values
            .into_iter()
            .map(|(column, value)| (column.into(), value))
            .collect();
        self
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
            match latest_by_pk.entry(row.pk_hash.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(row);
                }
                Entry::Occupied(mut entry) => {
                    if (row.seq, row.commit_seq) > (entry.get().seq, entry.get().commit_seq) {
                        entry.insert(row);
                    }
                }
            }
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
        let mut min_seq = None::<i64>;
        let mut max_seq = None::<i64>;
        let mut min_commit_seq = None::<i64>;
        let mut max_commit_seq = None::<i64>;

        for row in self.rows.iter().filter(|row| !row.deleted) {
            let seq = row.seq.get();
            let commit_seq = row.commit_seq.get();
            min_seq = Some(min_seq.map_or(seq, |current| current.min(seq)));
            max_seq = Some(max_seq.map_or(seq, |current| current.max(seq)));
            min_commit_seq =
                Some(min_commit_seq.map_or(commit_seq, |current| current.min(commit_seq)));
            max_commit_seq =
                Some(max_commit_seq.map_or(commit_seq, |current| current.max(commit_seq)));
        }

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

    /// Computes min/max stats for configured columns from live flushed rows.
    #[must_use]
    pub fn column_stats<I, S>(&self, columns: I) -> BTreeMap<ColumnId, ColumnStats>
    where
        I: IntoIterator<Item = (ColumnId, S)>,
        S: AsRef<str>,
    {
        let mut accumulators = BTreeMap::<ColumnId, (String, ColumnStatsAccumulator)>::new();
        for (column_id, column) in columns {
            let name = column.as_ref().trim();
            if !name.is_empty() {
                accumulators
                    .entry(column_id)
                    .or_insert_with(|| (name.to_string(), ColumnStatsAccumulator::default()));
            }
        }

        for row in self.rows.iter().filter(|row| !row.deleted) {
            for (column, accumulator) in accumulators.values_mut() {
                if let Some(value) = row.column_values.get(column.as_str()) {
                    accumulator.push(value);
                }
            }
        }

        accumulators
            .into_iter()
            .filter_map(|(column_id, (_, accumulator))| {
                accumulator.finish().map(|stats| (column_id, stats))
            })
            .collect()
    }
}

#[derive(Debug, Default)]
struct ColumnStatsAccumulator {
    min: Option<serde_json::Value>,
    max: Option<serde_json::Value>,
    invalid: bool,
}

impl ColumnStatsAccumulator {
    fn push(&mut self, value: &serde_json::Value) {
        if value.is_null() || self.invalid {
            return;
        }
        match (&self.min, &self.max) {
            (None, None) => {
                self.min = Some(value.clone());
                self.max = Some(value.clone());
            }
            (Some(min), Some(max)) => {
                let Some(min_ordering) = compare_json_values(value, min) else {
                    self.invalidate();
                    return;
                };
                let Some(max_ordering) = compare_json_values(value, max) else {
                    self.invalidate();
                    return;
                };
                if min_ordering == Ordering::Less {
                    self.min = Some(value.clone());
                }
                if max_ordering == Ordering::Greater {
                    self.max = Some(value.clone());
                }
            }
            _ => self.invalidate(),
        }
    }

    fn finish(self) -> Option<ColumnStats> {
        if self.invalid {
            return None;
        }
        Some(ColumnStats {
            min: self.min?,
            max: self.max?,
        })
    }

    fn invalidate(&mut self) {
        self.invalid = true;
        self.min = None;
        self.max = None;
    }
}

/// Planned `koldstore.segments` catalog insertion after manifest commit.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentCatalogInsert {
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
    pub column_stats: BTreeMap<ColumnId, ColumnStats>,
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
    /// Hint kind: exact, bloom, or range.
    pub hint_kind: HintKind,
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
            next_manifest_state: SyncState::Error,
            hot_data_authoritative: true,
            job_state: "error",
            last_error: Some(error.into()),
        }
    }
}

/// Plans `koldstore.segments` insertion from published footer metadata.
pub fn plan_segment_insert(
    table_oid: u32,
    scope_key: Option<ScopeKey>,
    object_path: impl Into<String>,
    metadata: SegmentFooterMetadata,
    manifest_etag: impl Into<String>,
) -> Result<SegmentCatalogInsert> {
    Ok(SegmentCatalogInsert {
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
        status: "staged",
        manifest_etag: manifest_etag.into(),
    })
}

/// Plans local PK hint updates for live rows written to cold storage.
#[must_use]
pub fn plan_cold_pk_hint_updates(
    table_oid: u32,
    scope_key: Option<ScopeKey>,
    batch: &FlushBatchPlan,
    hint_kind: HintKind,
) -> Vec<ColdPkHintUpdate> {
    batch
        .rows
        .iter()
        .filter(|row| !row.deleted)
        .map(|row| ColdPkHintUpdate {
            table_oid,
            scope_key: scope_key.clone(),
            pk_hash: row.pk_hash.clone(),
            hint_kind,
            latest_seq: row.seq,
            latest_commit_seq: row.commit_seq,
        })
        .collect()
}

/// Returns whether a flushed live row may be removed from hot storage.
#[must_use]
pub fn conditional_cleanup_allowed(
    flushed_candidate: &HotRowCandidate,
    current_seq: SeqId,
    current_commit_seq: CommitSeq,
    watermark: FlushWatermark,
) -> bool {
    !flushed_candidate.deleted
        && watermark.includes(flushed_candidate)
        && flushed_candidate.seq == current_seq
        && flushed_candidate.commit_seq == current_commit_seq
}

/// Returns whether a bounded flush batch should continue.
#[must_use]
pub const fn should_continue_batch(scanned_rows: usize, batch_size: usize) -> bool {
    batch_size > 0 && scanned_rows >= batch_size
}
