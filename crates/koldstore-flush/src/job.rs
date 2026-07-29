//! Flush job state transitions and legacy batch-planning helpers.
//!
//! **Not on the live `flush_table` path.** Production encode uses
//! [`crate::encode::stream_flush_chunks`] plus
//! [`crate::segment_catalog::plan_flush_segments_batch_insert`]. Types here
//! (`FlushBatchBuilder`, `HotRowCandidate`, …) remain for unit tests and benches
//! that exercise bounded-batch semantics.

use std::{
    collections::{btree_map::Entry, BTreeMap},
    num::NonZeroUsize,
};

use koldstore_common::{CommitSeq, KoldstoreError, MirrorOperation, Result, SeqId, StablePkHash};

/// Bounded flush execution settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushExecutionConfig {
    max_rows_per_batch: NonZeroUsize,
    max_bytes_per_batch: u64,
    max_batches_per_run: NonZeroUsize,
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
    pub fn live(pk_hash: StablePkHash, seq: SeqId, commit_seq: CommitSeq) -> Self {
        Self {
            pk_hash,
            seq,
            commit_seq,
            deleted: false,
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
