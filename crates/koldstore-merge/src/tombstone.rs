//! Tombstone retention decisions.

/// Tombstone action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TombstoneDecision {
    PhysicalDelete,
    KeepTombstone,
}

/// Decides whether a tombstone is required.
#[must_use]
pub fn tombstone_required(cold_may_contain_pk: bool) -> TombstoneDecision {
    if cold_may_contain_pk {
        TombstoneDecision::KeepTombstone
    } else {
        TombstoneDecision::PhysicalDelete
    }
}
