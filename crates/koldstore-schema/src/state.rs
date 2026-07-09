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
    /// DML capture is active and existing rows may still be scanning.
    Capturing,
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
            Self::Capturing => "capturing",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }

    /// Parses a catalog spelling into a typed state.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "not_started" => Some(Self::NotStarted),
            "capturing" => Some(Self::Capturing),
            "complete" => Some(Self::Complete),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}
