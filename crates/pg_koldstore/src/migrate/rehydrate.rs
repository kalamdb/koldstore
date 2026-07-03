//! Demigration rehydrate helpers.

/// Default demigration rehydrates cold rows.
pub const DEFAULT_REHYDRATE: bool = true;

/// Demigration execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DemigrationMode {
    /// Rebuild the heap from the logical merged table before disabling hooks.
    Rehydrate,
    /// Disable management and leave cold artifacts as an archive.
    ArchiveDetach,
}

/// Options accepted by `koldstore.demigrate_table`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DemigrateOptions {
    /// Whether to rehydrate current logical rows into the heap.
    pub rehydrate: bool,
    /// Whether to mark cold artifacts deleted after a successful rehydrate.
    pub drop_cold: bool,
    /// Whether to remove pg-koldstore system columns from the heap.
    pub drop_system_columns: bool,
}

impl Default for DemigrateOptions {
    fn default() -> Self {
        Self {
            rehydrate: DEFAULT_REHYDRATE,
            drop_cold: false,
            drop_system_columns: false,
        }
    }
}

impl DemigrateOptions {
    /// Returns the planned demigration mode.
    #[must_use]
    pub fn mode(self) -> DemigrationMode {
        if self.rehydrate {
            DemigrationMode::Rehydrate
        } else {
            DemigrationMode::ArchiveDetach
        }
    }

    /// `drop_cold` is only safe after the rehydrate phase completed.
    #[must_use]
    pub fn requires_successful_rehydrate(self) -> bool {
        self.drop_cold
    }
}
