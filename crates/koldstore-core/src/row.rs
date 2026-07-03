//! Hot, cold, tombstone, and row-event models.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{CommitSeq, LogicalPk, ScopeKey, SeqId, StablePkHash};

/// Change operation recorded in `koldstore.row_events`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowOperation {
    /// A new logical row was inserted.
    Insert,
    /// An existing hot row was updated.
    Update,
    /// A row was deleted or tombstoned.
    Delete,
    /// A hot tombstone was revived into a live row.
    Revive,
}

/// Current hot overlay row metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HotRow {
    /// Logical primary key.
    pub pk: LogicalPk,
    /// Optional user scope.
    pub scope_key: Option<ScopeKey>,
    /// Row/effect sequence.
    pub seq: SeqId,
    /// Commit-order cursor.
    pub commit_seq: CommitSeq,
    /// Whether this row is a tombstone.
    pub deleted: bool,
    /// Application column payload.
    pub row_image: Value,
}

impl HotRow {
    /// Converts this hot row to a tombstone while preserving PK and scope.
    #[must_use]
    pub fn into_tombstone(self) -> Tombstone {
        let pk_hash = StablePkHash::compute(&self.pk);
        Tombstone {
            pk: self.pk,
            scope_key: self.scope_key,
            seq: self.seq,
            commit_seq: self.commit_seq,
            pk_hash,
        }
    }
}

/// Immutable cold row metadata and payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColdRow {
    /// Logical primary key.
    pub pk: LogicalPk,
    /// Optional user scope.
    pub scope_key: Option<ScopeKey>,
    /// Row/effect sequence.
    pub seq: SeqId,
    /// Commit-order cursor.
    pub commit_seq: CommitSeq,
    /// Whether this cold row is a retained tombstone.
    pub deleted: bool,
    /// Segment schema version.
    pub schema_version: u32,
    /// Application column payload.
    pub row_image: Value,
}

/// Hot tombstone that masks older cold rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tombstone {
    /// Logical primary key.
    pub pk: LogicalPk,
    /// Optional user scope.
    pub scope_key: Option<ScopeKey>,
    /// Tombstone sequence.
    pub seq: SeqId,
    /// Tombstone commit cursor.
    pub commit_seq: CommitSeq,
    /// Stable PK hash.
    pub pk_hash: StablePkHash,
}

/// Committed row event for change-feed consumers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowEvent {
    /// Relation oid represented as a stable integer in pure crates.
    pub table_oid: u32,
    /// Optional user scope.
    pub scope_key: Option<ScopeKey>,
    /// Stable PK hash.
    pub pk_hash: StablePkHash,
    /// JSON primary-key object.
    pub pk_json: Value,
    /// Event operation.
    pub op: RowOperation,
    /// Row/effect sequence.
    pub seq: SeqId,
    /// Commit-order cursor.
    pub commit_seq: CommitSeq,
    /// Delete/tombstone marker.
    pub deleted: bool,
    /// Optional event payload.
    pub row_image_json: Option<Value>,
    /// Event timestamp.
    pub created_at: DateTime<Utc>,
}
