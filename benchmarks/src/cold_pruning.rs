//! Cold pruning benchmark scenario.

/// Benchmark scenario name.
pub const NAME: &str = "cold_pruning";

/// Cold pruning verdict input.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ColdPruningResult {
    /// Total row groups in fixture.
    pub total_row_groups: usize,
    /// Selected row groups after pruning.
    pub selected_row_groups: usize,
}

impl ColdPruningResult {
    /// Returns skipped row-group ratio.
    #[must_use]
    pub fn skipped_ratio(self) -> f64 {
        if self.total_row_groups == 0 {
            0.0
        } else {
            let skipped = self
                .total_row_groups
                .saturating_sub(self.selected_row_groups);
            skipped as f64 / self.total_row_groups as f64
        }
    }
}
