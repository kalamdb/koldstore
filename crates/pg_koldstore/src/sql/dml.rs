//! Public DML SQL function boundaries.

use koldstore_core::{CommitSeq, Result, SeqId};

/// Result of a DML helper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DmlResult {
    /// Affected logical rows.
    pub affected_rows: i64,
    /// Whether a tombstone was written.
    pub tombstone_written: bool,
    /// Whether cold storage was read.
    pub cold_lookup_performed: bool,
}

/// Managed DML operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedDmlOperation {
    /// Hot insert.
    Insert,
    /// Hot update.
    Update,
    /// Hot delete.
    Delete,
    /// Tombstone revive.
    Revive,
}

impl ManagedDmlOperation {
    /// Returns whether the operation preserves the one-hot-row-per-PK invariant.
    #[must_use]
    pub const fn keeps_one_hot_row_per_pk(self) -> bool {
        matches!(
            self,
            Self::Insert | Self::Update | Self::Delete | Self::Revive
        )
    }
}

/// Stamp assigned to a managed DML effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DmlStamp {
    /// Row/effect sequence.
    pub seq: SeqId,
    /// Commit-order cursor.
    pub commit_seq: CommitSeq,
    /// Operation.
    pub operation: ManagedDmlOperation,
    /// Delete marker.
    pub deleted: bool,
}

impl DmlStamp {
    /// Creates a DML stamp.
    ///
    /// # Errors
    ///
    /// Returns an error when sequence values are invalid.
    pub fn new(seq: i64, commit_seq: i64, operation: ManagedDmlOperation) -> Result<Self> {
        Ok(Self {
            seq: SeqId::new(seq)?,
            commit_seq: CommitSeq::new(commit_seq)?,
            operation,
            deleted: matches!(operation, ManagedDmlOperation::Delete),
        })
    }
}

/// Delete route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteDecision {
    /// Remove the hot row physically.
    PhysicalDelete,
    /// Keep or insert a tombstone to mask cold rows.
    Tombstone,
}

/// Decides how to route a delete from local cold metadata.
#[must_use]
pub const fn delete_decision(cold_may_contain_pk: bool) -> DeleteDecision {
    if cold_may_contain_pk {
        DeleteDecision::Tombstone
    } else {
        DeleteDecision::PhysicalDelete
    }
}

/// SQL fragment for reviving one hot tombstone row.
#[must_use]
pub fn revive_tombstone_sql(table_name: &str) -> String {
    format!("UPDATE {table_name} SET _deleted = false WHERE _deleted = true")
}
