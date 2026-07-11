//! Cold PK hint models (legacy / unused on the flush write path).
//!
//! Exact per-PK catalog rows were removed from flush: they recreated heap-scale
//! bloat in PostgreSQL. Segment prune uses `koldstore.segment_stats` and
//! Parquet stats/bloom instead. This module remains for typed `HintKind` /
//! planning helpers until cold-only DML APIs are redesigned around may-contain
//! segment stats.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Hint kind stored in `koldstore.cold_pk_hints.hint_kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HintKind {
    /// Exact PK presence in a cold segment.
    Exact,
    /// Probabilistic may-contain hint.
    Bloom,
    /// Range may-contain hint.
    Range,
}

impl HintKind {
    /// Catalog / SQL spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Bloom => "bloom",
            Self::Range => "range",
        }
    }

    /// Parses a catalog spelling.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "exact" => Some(Self::Exact),
            "bloom" => Some(Self::Bloom),
            "range" => Some(Self::Range),
            _ => None,
        }
    }
}

/// Cold PK hint row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdPkHint {
    /// Managed table OID.
    pub table_oid: u32,
    /// Optional user-scope key.
    pub scope_key: Option<String>,
    /// Stable logical PK hash.
    pub pk_hash: String,
    /// Segment that owns the hint.
    pub segment_id: Uuid,
    /// Hint kind.
    pub hint_kind: HintKind,
    /// Latest known cold `_seq`.
    pub latest_seq: i64,
    /// Latest known cold `_commit_seq`.
    pub latest_commit_seq: i64,
}

impl ColdPkHint {
    /// Looks up local PK metadata, preferring exact hints over may-contain hints.
    ///
    /// PERFORMANCE: returns borrowed references so callers avoid cloning hint
    /// rows on the hot DML / tombstone path.
    #[must_use]
    pub fn lookup<'a>(pk_hash: &str, hints: &'a [Self]) -> PkLookup<'a> {
        let mut may_contain = Vec::new();
        for hint in hints.iter().filter(|hint| hint.pk_hash == pk_hash) {
            match hint.hint_kind {
                HintKind::Exact => return PkLookup::Exact(hint),
                HintKind::Bloom | HintKind::Range => may_contain.push(hint),
            }
        }

        if may_contain.is_empty() {
            PkLookup::Absent
        } else {
            PkLookup::MayContain(may_contain)
        }
    }
}

/// Local PK lookup result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PkLookup<'a> {
    /// No matching hint.
    Absent,
    /// Exact segment presence.
    Exact(&'a ColdPkHint),
    /// Probabilistic may-contain hints.
    MayContain(Vec<&'a ColdPkHint>),
}

impl PkLookup<'_> {
    /// Returns true when this lookup preserves exact SQL rowcount semantics.
    #[must_use]
    pub const fn can_preserve_exact_rowcount(&self) -> bool {
        matches!(self, Self::Exact(_))
    }

    /// Returns true when this lookup can produce an idempotent tombstone.
    #[must_use]
    pub const fn can_write_idempotent_tombstone(&self, allow_may_contain: bool) -> bool {
        match self {
            Self::Exact(_) => true,
            Self::MayContain(_) => allow_may_contain,
            Self::Absent => false,
        }
    }
}
