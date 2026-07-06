//! Row-group pruning helpers.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use koldstore_common::{CommitSeq, SeqId};

use crate::ColumnStats;

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

    /// Returns true when segment min/max stats may overlap the requested range.
    ///
    /// Missing, null, or incomparable stats return true so callers scan
    /// conservatively instead of risking false negatives.
    #[must_use]
    pub fn segment_column_may_overlap(
        &self,
        column_stats: &BTreeMap<String, ColumnStats>,
        column: &str,
        min: &serde_json::Value,
        max: &serde_json::Value,
    ) -> bool {
        let Some(stats) = column_stats.get(column) else {
            return true;
        };
        if min.is_null() || max.is_null() || stats.min.is_null() || stats.max.is_null() {
            return true;
        }
        let Some(max_vs_min) = compare_json_values(&stats.max, min) else {
            return true;
        };
        let Some(min_vs_max) = compare_json_values(&stats.min, max) else {
            return true;
        };

        max_vs_min != Ordering::Less && min_vs_max != Ordering::Greater
    }
}

fn compare_json_values(left: &serde_json::Value, right: &serde_json::Value) -> Option<Ordering> {
    match (left, right) {
        (serde_json::Value::Number(left), serde_json::Value::Number(right)) => {
            left.as_f64()?.partial_cmp(&right.as_f64()?)
        }
        (serde_json::Value::String(left), serde_json::Value::String(right)) => {
            Some(left.cmp(right))
        }
        (serde_json::Value::Bool(left), serde_json::Value::Bool(right)) => Some(left.cmp(right)),
        _ => None,
    }
}
