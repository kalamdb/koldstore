//! Progressive ordered merge helpers (PG-free).
//!
//! Frontier batch checks over already-encoded composite keys.

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
}
