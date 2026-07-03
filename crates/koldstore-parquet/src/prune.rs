//! Row-group pruning helpers.

use std::collections::BTreeMap;

use koldstore_core::{CommitSeq, SeqId};

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
        min: SeqId,
        max: SeqId,
    ) -> PruneDecision {
        let selected_row_groups: Vec<usize> = footer
            .row_groups
            .iter()
            .filter(|row_group| match (row_group.min_seq, row_group.max_seq) {
                (Some(group_min), Some(group_max)) => {
                    group_max >= min.get() && group_min <= max.get()
                }
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

    /// Prunes row groups whose `_commit_seq` range cannot overlap the requested range.
    #[must_use]
    pub fn prune_commit_seq_range(
        &self,
        footer: &crate::FooterSummary,
        min: CommitSeq,
        max: CommitSeq,
    ) -> PruneDecision {
        let selected_row_groups: Vec<usize> = footer
            .row_groups
            .iter()
            .filter(
                |row_group| match (row_group.min_commit_seq, row_group.max_commit_seq) {
                    (Some(group_min), Some(group_max)) => {
                        group_max >= min.get() && group_min <= max.get()
                    }
                    _ => true,
                },
            )
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

    /// Prunes row groups using PK bloom/may-contain metadata.
    ///
    /// Row groups with no bloom metadata are selected because they cannot be
    /// proven irrelevant.
    #[must_use]
    pub fn prune_pk_values<I, S>(
        &self,
        footer: &crate::FooterSummary,
        bloom_values: &BTreeMap<usize, Vec<String>>,
        requested_values: I,
    ) -> PruneDecision
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let requested = requested_values
            .into_iter()
            .map(|value| value.as_ref().to_string())
            .collect::<Vec<_>>();
        let selected_row_groups = footer
            .row_groups
            .iter()
            .filter(|row_group| {
                bloom_values.get(&row_group.row_group).is_none_or(|values| {
                    values
                        .iter()
                        .any(|candidate| requested.iter().any(|requested| requested == candidate))
                })
            })
            .map(|row_group| row_group.row_group)
            .collect::<Vec<_>>();

        PruneDecision {
            skipped_row_groups: footer
                .row_groups
                .len()
                .saturating_sub(selected_row_groups.len()),
            selected_row_groups,
        }
    }
}
