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
