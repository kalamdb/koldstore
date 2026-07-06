//! Hot, cold, tombstone, mirror, and latest-state change models.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{CommitSeq, KoldstoreError, LogicalPk, Result, ScopeKey, SeqId, StablePkHash};

/// Operation recorded in a latest-state change-log mirror.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MirrorOperation {
    /// Latest state is an insert or reinsert.
    Insert,
    /// Latest state is an update to an existing live row.
    Update,
    /// Latest state is a delete tombstone.
    Delete,
}

impl MirrorOperation {
    /// All mirror operations in stable insert/update/delete order.
    pub const ALL: [Self; 3] = [Self::Insert, Self::Update, Self::Delete];

    /// Returns the SQL `smallint` operation code.
    #[must_use]
    pub const fn code(self) -> i16 {
        match self {
            Self::Insert => 1,
            Self::Update => 2,
            Self::Delete => 3,
        }
    }

    /// Returns the suffix used by change-log mirror capture trigger names.
    #[must_use]
    pub const fn capture_trigger_suffix(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }

    /// Returns the PostgreSQL `CREATE TRIGGER` event keyword for this operation.
    #[must_use]
    pub const fn sql_trigger_event(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }

    /// Returns the trigger-row reference used by mirror capture upserts.
    #[must_use]
    pub const fn capture_row_ref(self) -> &'static str {
        match self {
            Self::Insert | Self::Update => "NEW",
            Self::Delete => "OLD",
        }
    }

    /// Builds the per-table change-log mirror capture trigger name.
    #[must_use]
    pub fn capture_trigger_name(self, mirror_table_name: &str) -> String {
        format!(
            "{}_{}_capture",
            mirror_table_name,
            self.capture_trigger_suffix()
        )
    }

    /// Parses a SQL `smallint` operation code.
    ///
    /// # Errors
    ///
    /// Returns an error when `code` is not one of 1, 2, or 3.
    pub const fn from_code(code: i16) -> Result<Self> {
        match code {
            1 => Ok(Self::Insert),
            2 => Ok(Self::Update),
            3 => Ok(Self::Delete),
            value => Err(KoldstoreError::InvalidOperationCode(value)),
        }
    }

    /// Returns true for delete/tombstone operations.
    #[must_use]
    pub const fn is_delete(self) -> bool {
        matches!(self, Self::Delete)
    }
}

/// Latest mirror state for one primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MirrorState {
    operation: Option<MirrorOperation>,
}

impl MirrorState {
    /// Returns a state with no mirror row yet.
    #[must_use]
    pub const fn missing() -> Self {
        Self { operation: None }
    }

    /// Applies a committed operation and returns the new latest state.
    #[must_use]
    pub const fn apply(self, operation: MirrorOperation) -> Self {
        let _ = self;
        Self {
            operation: Some(operation),
        }
    }

    /// Returns the latest operation, if any.
    #[must_use]
    pub const fn operation(self) -> Option<MirrorOperation> {
        self.operation
    }

    /// Returns true when the latest state is a delete tombstone.
    #[must_use]
    pub const fn is_tombstone(self) -> bool {
        matches!(self.operation, Some(MirrorOperation::Delete))
    }
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

/// Source that produced a latest-state change-feed row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ChangeSource {
    /// Current unflushed state from the table-specific mirror.
    HotMirror,
    /// Flushed state reconstructed from cold record metadata.
    ColdRecord,
}

/// Latest-state change-feed row from mirror/cold metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MirrorChange {
    /// Relation oid represented as a stable integer in pure crates.
    pub table_oid: u32,
    /// Optional user scope.
    pub scope_key: Option<ScopeKey>,
    /// JSON primary-key object.
    pub pk_json: Value,
    /// Latest mirror/cold operation.
    pub operation: MirrorOperation,
    /// Row/effect sequence.
    pub seq: SeqId,
    /// Change timestamp.
    pub changed_at: DateTime<Utc>,
    /// Delete/tombstone marker.
    pub deleted: bool,
    /// Optional live row payload.
    pub row_image_json: Option<Value>,
    /// Source of this latest-state change row.
    pub source: ChangeSource,
}
