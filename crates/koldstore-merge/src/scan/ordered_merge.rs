//! Progressive ordered merge helpers (PG-free).
//!
//! Frontier batch checks and row-group competitiveness over encoded keys.

use super::ordered_frontier::{compare_hot_to_cold_bound, FrontierDecision, OrderDirection};

/// True when every hot leading key strictly outranks `cold_bound` for `direction`.
#[must_use]
pub fn hot_keys_dominate_bound(
    direction: OrderDirection,
    hot_keys: &[Option<Vec<u8>>],
    cold_bound: Option<&[u8]>,
) -> bool {
    if hot_keys.is_empty() {
        return cold_bound.is_none();
    }
    hot_keys.iter().all(|key| {
        compare_hot_to_cold_bound(direction, key.as_deref(), cold_bound)
            == FrontierDecision::HotStrictlyWins
    })
}

/// Selects row-group indexes that may still win/tie against `hot_key`.
///
/// ASC: keep groups whose min bound is `<=` hot (or unknown).
/// DESC: keep groups whose max bound is `>=` hot (or unknown).
/// Missing `hot_key` keeps every group (conservative).
#[must_use]
pub fn select_competitive_row_groups(
    direction: OrderDirection,
    hot_key: Option<&[u8]>,
    row_group_mins: &[Option<Vec<u8>>],
    row_group_maxs: &[Option<Vec<u8>>],
) -> Vec<usize> {
    // Catalog arrays should have identical cardinality, but incomplete metadata
    // must fail open. Iterating the larger side preserves an unmatched row group
    // and treats its missing directional bound as unknown/competitive.
    let n = row_group_mins.len().max(row_group_maxs.len());
    let Some(hot) = hot_key else {
        return (0..n).collect();
    };
    let mut selected = Vec::new();
    for idx in 0..n {
        let competes = match direction {
            OrderDirection::Asc => match row_group_mins.get(idx).and_then(Option::as_deref) {
                None => true,
                Some(min_bound) => min_bound <= hot,
            },
            OrderDirection::Desc => match row_group_maxs.get(idx).and_then(Option::as_deref) {
                None => true,
                Some(max_bound) => max_bound >= hot,
            },
        };
        if competes {
            selected.push(idx);
        }
    }
    selected
}

/// Intersects catalog-planned row groups with an ordered-frontier selection.
///
/// The planned order (and any duplicate entries) is preserved. Sorting the
/// owned competitive set once avoids the quadratic repeated-membership scan in
/// the PostgreSQL adapter without allocating a second lookup collection.
#[must_use]
pub fn intersect_row_group_selections(
    planned: Vec<usize>,
    mut competitive: Vec<usize>,
) -> Vec<usize> {
    competitive.sort_unstable();
    competitive.dedup();
    planned
        .into_iter()
        .filter(|row_group| competitive.binary_search(row_group).is_ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desc_page_dominates_when_all_above_cold_max() {
        let keys = [Some(b"z".to_vec()), Some(b"y".to_vec())];
        assert!(hot_keys_dominate_bound(
            OrderDirection::Desc,
            &keys,
            Some(b"m")
        ));
    }

    #[test]
    fn desc_page_does_not_dominate_on_tie() {
        let keys = [Some(b"m".to_vec())];
        assert!(!hot_keys_dominate_bound(
            OrderDirection::Desc,
            &keys,
            Some(b"m")
        ));
    }

    #[test]
    fn desc_skips_row_groups_below_hot_key() {
        let mins = [
            Some(b"a".to_vec()),
            Some(b"m".to_vec()),
            Some(b"x".to_vec()),
        ];
        let maxs = [
            Some(b"c".to_vec()),
            Some(b"p".to_vec()),
            Some(b"z".to_vec()),
        ];
        assert_eq!(
            select_competitive_row_groups(OrderDirection::Desc, Some(b"q"), &mins, &maxs),
            vec![2]
        );
    }

    #[test]
    fn asc_skips_row_groups_above_hot_key() {
        let mins = [
            Some(b"a".to_vec()),
            Some(b"m".to_vec()),
            Some(b"x".to_vec()),
        ];
        let maxs = [
            Some(b"c".to_vec()),
            Some(b"p".to_vec()),
            Some(b"z".to_vec()),
        ];
        assert_eq!(
            select_competitive_row_groups(OrderDirection::Asc, Some(b"d"), &mins, &maxs),
            vec![0]
        );
    }

    #[test]
    fn mismatched_bound_arrays_keep_unmatched_row_groups_conservatively() {
        let mins = [Some(b"a".to_vec())];
        let maxs = [Some(b"c".to_vec()), Some(b"z".to_vec())];

        assert_eq!(
            select_competitive_row_groups(OrderDirection::Asc, Some(b"d"), &mins, &maxs),
            vec![0, 1]
        );
        assert_eq!(
            select_competitive_row_groups(OrderDirection::Desc, Some(b"d"), &mins, &maxs),
            vec![1]
        );
    }

    #[test]
    fn row_group_intersection_preserves_planned_order() {
        assert_eq!(
            intersect_row_group_selections(vec![5, 2, 5, 9], vec![9, 3, 5]),
            vec![5, 5, 9]
        );
    }
}
