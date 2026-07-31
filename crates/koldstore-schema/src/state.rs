//! Mirror initialization state stored in `koldstore.schemas`.
//!
//! The state belongs with schema metadata because it describes whether the
//! managed-table schema has a complete clean-schema mirror backing it.

use serde::{Deserialize, Serialize};

/// Mirror initialization lifecycle state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MirrorInitializationState {
    /// Metadata exists but no complete mirror state is available.
    #[default]
    NotStarted,
    /// Snapshot backfill is copying existing heap rows into the mirror.
    Backfilling,
    /// Backfill finished; WAL catch-up is applying committed changes above the floor.
    CatchingUp,
    /// Every pre-existing row has a mirror state unless superseded by newer DML.
    Complete,
    /// Initialization failed and needs retry or rollback.
    Failed,
}

impl MirrorInitializationState {
    /// Catalog / SQL spelling for this state.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Backfilling => "backfilling",
            Self::CatchingUp => "catching_up",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }

    /// Parses a catalog spelling into a typed state.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "not_started" => Some(Self::NotStarted),
            "backfilling" => Some(Self::Backfilling),
            "catching_up" => Some(Self::CatchingUp),
            "complete" => Some(Self::Complete),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}
