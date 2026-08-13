//! Mirror tombstone overlay for masking older cold winners.

use std::collections::HashSet;

use koldstore_common::{ColdRow, LogicalPk};

/// Unflushed mirror tombstones that mask older cold Parquet rows.
#[derive(Debug, Default, Clone)]
pub struct MirrorOverlay {
    masked_pks: HashSet<LogicalPk>,
}

impl MirrorOverlay {
    /// Creates an overlay from exact logical primary keys.
    #[must_use]
    pub fn new(masked_pks: impl IntoIterator<Item = LogicalPk>) -> Self {
        Self {
            masked_pks: masked_pks.into_iter().collect(),
        }
    }

    /// Removes masked cold rows in place and returns the number removed.
    pub fn retain_unmasked(&self, cold_rows: &mut Vec<ColdRow>) -> usize {
        let before = cold_rows.len();
        cold_rows.retain(|row| !self.masked_pks.contains(&row.pk));
        before.saturating_sub(cold_rows.len())
    }

    /// Adds one exact tombstone key, returning whether it was new.
    pub fn insert(&mut self, pk: LogicalPk) -> bool {
        self.masked_pks.insert(pk)
    }

    /// Returns whether `pk` is masked by an unflushed tombstone.
    #[must_use]
    pub fn contains(&self, pk: &LogicalPk) -> bool {
        self.masked_pks.contains(pk)
    }

    /// Iterates exact masked keys without cloning them.
    pub fn iter(&self) -> impl Iterator<Item = &LogicalPk> {
        self.masked_pks.iter()
    }

    /// Returns the number of distinct tombstone keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.masked_pks.len()
    }

    /// Returns whether the overlay has no tombstone keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.masked_pks.is_empty()
    }

    /// Consumes the overlay into exact masked keys.
    pub fn into_masked_pks(self) -> impl Iterator<Item = LogicalPk> {
        self.masked_pks.into_iter()
    }
}
