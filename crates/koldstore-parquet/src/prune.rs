//! Row-group pruning helpers.

/// Pruning result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneDecision {
    pub selected_row_groups: Vec<usize>,
    pub skipped_row_groups: usize,
}

/// Row-group pruner placeholder.
#[derive(Debug, Default, Clone)]
pub struct RowGroupPruner;

impl RowGroupPruner {
    /// Prunes row groups whose `_seq` range cannot overlap the requested range.
    #[must_use]
    pub fn prune_seq_range(
        &self,
        footer: &crate::FooterSummary,
        min: i64,
        max: i64,
    ) -> PruneDecision {
        let selected_row_groups: Vec<usize> = footer
            .row_groups
            .iter()
            .filter(|row_group| match (row_group.min_seq, row_group.max_seq) {
                (Some(group_min), Some(group_max)) => group_max >= min && group_min <= max,
                _ => true,
            })
            .map(|row_group| row_group.row_group)
            .collect();
        PruneDecision {
            skipped_row_groups: footer
                .row_groups
                .len()
                .saturating_sub(selected_row_groups.len()),
            selected_row_groups,
        }
    }
}
