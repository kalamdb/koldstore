//! CustomScan plan serialization.

use koldstore_core::{Predicate, Result, ScopeKey, SeqId};
use serde::{Deserialize, Serialize};

/// Attribute numbers for managed system columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemColumnAttnums {
    /// `_seq` attribute number.
    pub seq: i16,
    /// `_commit_seq` attribute number.
    pub commit_seq: i16,
    /// `_deleted` attribute number.
    pub deleted: i16,
    /// Optional scope attribute number.
    pub scope: Option<i16>,
}

/// Cold segment hint serialized into the CustomScan plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentHint {
    /// Segment catalog identity.
    pub segment_id: String,
    /// Optional user scope for the cold segment.
    pub scope_key: Option<ScopeKey>,
    /// Final object-store path.
    pub object_path: String,
    /// Selected row groups after safe pruning.
    pub selected_row_groups: Vec<usize>,
    /// Segment minimum `_seq`.
    pub min_seq: SeqId,
    /// Segment maximum `_seq`.
    pub max_seq: SeqId,
}

/// Serialized custom-plan identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MergeScanPlan {
    /// Managed table oid.
    pub table_oid: u32,
    /// PostgreSQL scan relation id.
    pub scanrelid: u32,
    /// Logical primary-key columns.
    pub primary_key_columns: Vec<String>,
    /// Managed system-column attnums.
    pub system_column_attnums: SystemColumnAttnums,
    /// Optional user scope key captured at planning time.
    pub scope_key: Option<ScopeKey>,
    /// Predicates proven safe for pre-merge pruning.
    pub safe_quals: Vec<Predicate>,
    /// Residual predicates evaluated after winner resolution.
    pub residual_quals: Vec<Predicate>,
    /// Security/RLS predicates evaluated after winner resolution or fail-closed.
    pub security_quals: Vec<Predicate>,
    /// Required output/qual columns.
    pub projection: Vec<String>,
    /// Visible cold segment hints.
    pub segment_hints: Vec<SegmentHint>,
}

impl MergeScanPlan {
    /// Creates a merge scan plan.
    #[must_use]
    pub fn new(table_oid: u32, primary_key_columns: Vec<String>) -> Self {
        Self {
            table_oid,
            scanrelid: 0,
            primary_key_columns,
            system_column_attnums: SystemColumnAttnums {
                seq: 0,
                commit_seq: 0,
                deleted: 0,
                scope: None,
            },
            scope_key: None,
            safe_quals: Vec::new(),
            residual_quals: Vec::new(),
            security_quals: Vec::new(),
            projection: Vec::new(),
            segment_hints: Vec::new(),
        }
    }

    /// Serializes the plan payload for PostgreSQL `custom_private`.
    ///
    /// # Errors
    ///
    /// Returns a JSON error if the payload cannot be serialized.
    pub fn serialize(&self) -> Result<String> {
        serde_json::to_string(self).map_err(Into::into)
    }

    /// Deserializes a plan payload from PostgreSQL `custom_private`.
    ///
    /// # Errors
    ///
    /// Returns a JSON error if the payload is malformed.
    pub fn deserialize(value: &str) -> Result<Self> {
        serde_json::from_str(value).map_err(Into::into)
    }

    /// Expressions that PostgreSQL must evaluate after winner resolution.
    #[must_use]
    pub fn custom_exprs(&self) -> Vec<Predicate> {
        self.residual_quals
            .iter()
            .chain(self.security_quals.iter())
            .cloned()
            .collect()
    }

    /// Projection columns serialized into `custom_private`.
    #[must_use]
    pub fn custom_private_projection(&self) -> &[String] {
        &self.projection
    }
}
