//! Typed JSON row shapes exchanged with mirror SQL.

use serde::Deserialize;

/// Aggregate sequence stats returned by mirror stats probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct MirrorSeqStats {
    /// Number of rows in the mirror.
    pub row_count: i64,
    /// Minimum mirror `seq`.
    pub min_seq: i64,
    /// Maximum mirror `seq`.
    pub max_seq: i64,
    /// Minimum commit sequence covered by the mirror.
    pub min_commit_seq: i64,
    /// Maximum commit sequence covered by the mirror.
    pub max_commit_seq: i64,
}

/// One mirror row selected for flush or policy evaluation.
#[derive(Debug, Clone, Deserialize)]
pub struct MirrorSelectionRow {
    /// Mirror sequence.
    pub seq: i64,
    /// Mirror operation code.
    pub op: i64,
    /// Remaining column values keyed by column name.
    #[serde(flatten)]
    pub fields: serde_json::Map<String, serde_json::Value>,
}
