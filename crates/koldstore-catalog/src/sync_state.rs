//! Catalog `manifest.sync_state` FSM.
//!
//! Lives with catalog bookkeeping (not the object-store `manifest.json` model)
//! so merge/flush can depend on the enum without pulling `koldstore-storage`.

use serde::{Deserialize, Serialize};

/// Local manifest cache sync state stored in `koldstore.manifest.sync_state`.
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
    /// All catalog-visible sync states.
    pub const ALL: [Self; 5] = [
        Self::PendingWrite,
        Self::Syncing,
        Self::InSync,
        Self::Stale,
        Self::Error,
    ];

    /// Returns the SQL/catalog representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InSync => "in_sync",
            Self::PendingWrite => "pending_write",
            Self::Syncing => "syncing",
            Self::Stale => "stale",
            Self::Error => "error",
        }
    }

    /// Starts a flush for a pending, stale, or errored scope.
    #[must_use]
    pub const fn start_flush(self) -> Self {
        match self {
            Self::PendingWrite | Self::Stale | Self::Error => Self::Syncing,
            Self::Syncing | Self::InSync => self,
        }
    }

    /// Completes a successful flush.
    #[must_use]
    pub const fn finish_success(self, remaining_hot_rows: bool) -> Self {
        if remaining_hot_rows {
            Self::PendingWrite
        } else {
            Self::InSync
        }
    }

    /// Completes a failed flush.
    #[must_use]
    pub const fn finish_error(self) -> Self {
        Self::Error
    }

    /// Returns whether the catalog sync state can transition to `next`.
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

    /// Dirties an in-sync catalog row after hot DML.
    ///
    /// Mirrors `koldstore.internal_bump_row_counts`: only `in_sync` becomes
    /// `pending_write`; other states are left unchanged.
    #[must_use]
    pub const fn after_hot_dml(self) -> Self {
        match self {
            Self::InSync => Self::PendingWrite,
            other => other,
        }
    }
}
