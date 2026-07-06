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
