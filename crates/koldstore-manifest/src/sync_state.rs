//! Manifest cache sync states.

use serde::{Deserialize, Serialize};

/// Local manifest cache state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncState {
    /// Local and object-store manifest agree.
    InSync,
    /// Hot data changed and needs a flush.
    PendingWrite,
    /// Flush is currently writing/publishing.
    Syncing,
    /// Local cache must be refreshed.
    Stale,
    /// Last sync attempt failed.
    Error,
}

impl SyncState {
    /// Returns whether the manifest cache can transition to `next`.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::InSync, Self::PendingWrite)
                | (Self::PendingWrite, Self::Syncing)
                | (Self::PendingWrite, Self::Error)
                | (Self::Syncing, Self::InSync)
                | (Self::Syncing, Self::PendingWrite)
                | (Self::Syncing, Self::Error)
                | (Self::Stale, Self::InSync)
                | (Self::Stale, Self::Error)
                | (Self::Error, Self::PendingWrite)
                | (Self::Error, Self::Syncing)
        )
    }
}
