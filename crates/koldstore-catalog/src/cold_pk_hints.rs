//! Cold PK hint models.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Hint kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HintKind {
    Exact,
    Bloom,
    Range,
}

/// Cold PK hint row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdPkHint {
    pub table_oid: u32,
    pub scope_key: Option<String>,
    pub pk_hash: String,
    pub segment_id: Uuid,
    pub hint_kind: HintKind,
    pub latest_seq: i64,
    pub latest_commit_seq: i64,
}

impl ColdPkHint {
    /// Looks up local PK metadata, preferring exact hints over may-contain hints.
    #[must_use]
    pub fn lookup(pk_hash: &str, hints: &[Self]) -> PkLookup {
        let mut may_contain = Vec::new();
        for hint in hints.iter().filter(|hint| hint.pk_hash == pk_hash) {
            match hint.hint_kind {
                HintKind::Exact => return PkLookup::Exact(hint.clone()),
                HintKind::Bloom | HintKind::Range => may_contain.push(hint.clone()),
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
pub enum PkLookup {
    Absent,
    Exact(ColdPkHint),
    MayContain(Vec<ColdPkHint>),
}
