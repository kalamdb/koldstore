//! Bound shapes and preferred access hints for `cold_segment_index` lookups.
//!
//! PostgreSQL-free: the extension picks a statement from [`SegmentIndexLookupShape`]
//! and may surface [`preferred_segment_index_access`] in EXPLAIN. SQL never forces
//! an index (no HINT / BitmapAnd); the planner may still choose seq_scan.

/// Bound shape used for `koldstore.cold_segment_index` candidate SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentIndexLookupShape {
    /// Both lower and upper bounds present (`closed` statement).
    BoundedRange,
    /// Lower bound only (`max_value >= lower`).
    LowerBound,
    /// Upper bound only (`min_value <= upper`).
    UpperBound,
    /// No encodeable bounds; list all active segments.
    AllActive,
}

impl SegmentIndexLookupShape {
    /// Stable EXPLAIN / test label for this shape.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BoundedRange => "bounded_range",
            Self::LowerBound => "lower_bound",
            Self::UpperBound => "upper_bound",
            Self::AllActive => "all_active",
        }
    }
}

/// Preferred `cold_segment_index` access path for a bound shape (not forced).
#[must_use]
pub const fn preferred_segment_index_access(shape: SegmentIndexLookupShape) -> &'static str {
    match shape {
        SegmentIndexLookupShape::BoundedRange => "bitmap_and_or_single",
        SegmentIndexLookupShape::LowerBound => "max_idx",
        SegmentIndexLookupShape::UpperBound => "min_idx",
        SegmentIndexLookupShape::AllActive => "seq_scan",
    }
}

/// Selects row-group IDs from aligned packed Sort Key V1 arrays.
///
/// Known bounds use bytewise comparisons because Sort Key V1 preserves
/// PostgreSQL ordering. Proven all-null groups cannot satisfy ordinary range
/// predicates and are removed. Missing statistics stay conservative.
///
/// # Errors
///
/// Returns an error when array cardinalities or null counts are malformed.
pub fn select_packed_row_groups(
    row_group_count: usize,
    row_group_row_counts: &[i64],
    row_group_min_values: &[Option<Vec<u8>>],
    row_group_max_values: &[Option<Vec<u8>>],
    row_group_null_counts: &[Option<i64>],
    query_lower: Option<&[u8]>,
    query_upper: Option<&[u8]>,
) -> Result<Vec<usize>, String> {
    if row_group_count == 0
        || row_group_row_counts.len() != row_group_count
        || row_group_min_values.len() != row_group_count
        || row_group_max_values.len() != row_group_count
        || row_group_null_counts.len() != row_group_count
    {
        return Err("packed row-group metadata cardinality mismatch".to_string());
    }
    if query_lower.is_none() && query_upper.is_none() {
        return Ok((0..row_group_count).collect());
    }

    let mut selected = Vec::with_capacity(row_group_count);
    for row_group_id in 0..row_group_count {
        let row_count = row_group_row_counts[row_group_id];
        let null_count = row_group_null_counts[row_group_id];
        if row_count <= 0 || null_count.is_some_and(|count| count < 0 || count > row_count) {
            return Err(format!(
                "packed row-group {row_group_id} has invalid row/null count"
            ));
        }
        let all_null = null_count == Some(row_count);
        match (
            &row_group_min_values[row_group_id],
            &row_group_max_values[row_group_id],
        ) {
            (Some(min), Some(max)) => {
                if all_null {
                    return Err(format!(
                        "packed row-group {row_group_id} has bounds for an all-null column"
                    ));
                }
                if min > max {
                    return Err(format!(
                        "packed row-group {row_group_id} has reversed min/max bounds"
                    ));
                }
                let overlaps_lower = query_lower.is_none_or(|lower| max.as_slice() >= lower);
                let overlaps_upper = query_upper.is_none_or(|upper| min.as_slice() <= upper);
                if overlaps_lower && overlaps_upper {
                    selected.push(row_group_id);
                }
            }
            (None, None) if all_null => {}
            // Wholly missing bounds mean unknown statistics.
            (None, None) => selected.push(row_group_id),
            _ => {
                return Err(format!(
                    "packed row-group {row_group_id} has unpaired min/max bounds"
                ))
            }
        }
    }
    Ok(selected)
}

/// Selects row groups that may contain a SeqId strictly after `last_seq`.
///
/// # Errors
///
/// Returns an error when the packed array is empty or contains an invalid
/// non-positive SeqId.
pub fn select_row_groups_after_seq(
    row_group_max_seqs: &[i64],
    last_seq: i64,
) -> Result<Vec<usize>, String> {
    if row_group_max_seqs.is_empty() {
        return Err("packed row-group SeqId metadata is empty".to_string());
    }
    if row_group_max_seqs.iter().any(|seq| *seq <= 0) {
        return Err("packed row-group maximum SeqId must be positive".to_string());
    }
    Ok(row_group_max_seqs
        .iter()
        .enumerate()
        .filter_map(|(row_group_id, max_seq)| (*max_seq > last_seq).then_some(row_group_id))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::{select_packed_row_groups, select_row_groups_after_seq};

    #[test]
    fn packed_bounds_prune_by_aligned_position_and_keep_unknown_groups() {
        let selected = select_packed_row_groups(
            4,
            &[2, 2, 2, 2],
            &[Some(vec![1]), Some(vec![10]), None, None],
            &[Some(vec![5]), Some(vec![20]), None, None],
            &[Some(0), Some(0), Some(2), None],
            Some(&[6]),
            Some(&[12]),
        )
        .unwrap();

        // Group 0 is below the query, group 1 overlaps, group 2 is proven
        // all-null, and group 3 has unknown stats and stays conservative.
        assert_eq!(selected, vec![1, 3]);
    }

    #[test]
    fn malformed_packed_bound_lengths_are_rejected() {
        let error = select_packed_row_groups(
            2,
            &[1, 1],
            &[Some(vec![1])],
            &[Some(vec![2]), Some(vec![3])],
            &[Some(0), Some(0)],
            Some(&[1]),
            Some(&[2]),
        )
        .unwrap_err();

        assert!(error.contains("cardinality"));
    }

    #[test]
    fn malformed_packed_bound_pairs_are_rejected() {
        let error = select_packed_row_groups(
            1,
            &[1],
            &[Some(vec![1])],
            &[None],
            &[Some(0)],
            Some(&[1]),
            Some(&[2]),
        )
        .unwrap_err();

        assert!(error.contains("unpaired"));
    }

    #[test]
    fn seq_cursor_prunes_row_groups_entirely_before_the_cursor() {
        assert_eq!(
            select_row_groups_after_seq(&[10, 20, 30], 20).unwrap(),
            vec![2]
        );
    }
}
