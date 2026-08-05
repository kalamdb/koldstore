//! Ordered actual-vs-bound frontier comparisons (PG-free).
//!
//! Decides whether the next hot candidate can be emitted without opening cold
//! Parquet, or whether cold bounds may still win/tie and must be expanded.
//! Keys are opaque Sort Key V1 / composite `bytea` values (byte-ordered).

/// Scan direction for a single ordered progressive identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    /// Ascending: cold's best unopened key is its minimum bound.
    Asc,
    /// Descending: cold's best unopened key is its maximum bound.
    Desc,
}

/// Outcome of comparing one hot actual key to the cold frontier bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrontierDecision {
    /// Hot strictly outranks every remaining cold bound; emit hot, skip Parquet.
    HotStrictlyWins,
    /// Cold may produce the next (or tied) key; expand frontier before emitting.
    ColdMayWinOrTie,
}

/// Compares a hot actual composite key to the cold frontier's best bound.
///
/// `cold_best_bound` is the competitive unopened cold key for this direction:
/// segment/row-group **min** for [`OrderDirection::Asc`], **max** for
/// [`OrderDirection::Desc`]. Missing cold bound → [`FrontierDecision::HotStrictlyWins`]
/// (empty frontier). Missing hot key is not valid and treated as needing cold
/// expansion (conservative).
#[must_use]
pub fn compare_hot_to_cold_bound(
    direction: OrderDirection,
    hot_key: Option<&[u8]>,
    cold_best_bound: Option<&[u8]>,
) -> FrontierDecision {
    let Some(hot) = hot_key else {
        return FrontierDecision::ColdMayWinOrTie;
    };
    let Some(cold) = cold_best_bound else {
        return FrontierDecision::HotStrictlyWins;
    };
    match direction {
        // ASC: emit hot only when it is strictly before every remaining cold key.
        OrderDirection::Asc => {
            if hot < cold {
                FrontierDecision::HotStrictlyWins
            } else {
                FrontierDecision::ColdMayWinOrTie
            }
        }
        // DESC: emit hot only when it is strictly after every remaining cold key.
        OrderDirection::Desc => {
            if hot > cold {
                FrontierDecision::HotStrictlyWins
            } else {
                FrontierDecision::ColdMayWinOrTie
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asc_hot_strictly_before_cold_min() {
        assert_eq!(
            compare_hot_to_cold_bound(OrderDirection::Asc, Some(b"a"), Some(b"c")),
            FrontierDecision::HotStrictlyWins
        );
    }

    #[test]
    fn asc_tie_and_hot_after_need_cold() {
        assert_eq!(
            compare_hot_to_cold_bound(OrderDirection::Asc, Some(b"c"), Some(b"c")),
            FrontierDecision::ColdMayWinOrTie
        );
        assert_eq!(
            compare_hot_to_cold_bound(OrderDirection::Asc, Some(b"d"), Some(b"c")),
            FrontierDecision::ColdMayWinOrTie
        );
    }

    #[test]
    fn desc_hot_strictly_after_cold_max() {
        assert_eq!(
            compare_hot_to_cold_bound(OrderDirection::Desc, Some(b"z"), Some(b"m")),
            FrontierDecision::HotStrictlyWins
        );
    }

    #[test]
    fn desc_tie_and_hot_before_need_cold() {
        assert_eq!(
            compare_hot_to_cold_bound(OrderDirection::Desc, Some(b"m"), Some(b"m")),
            FrontierDecision::ColdMayWinOrTie
        );
        assert_eq!(
            compare_hot_to_cold_bound(OrderDirection::Desc, Some(b"a"), Some(b"m")),
            FrontierDecision::ColdMayWinOrTie
        );
    }

    #[test]
    fn empty_cold_frontier_hot_wins() {
        assert_eq!(
            compare_hot_to_cold_bound(OrderDirection::Desc, Some(b"x"), None),
            FrontierDecision::HotStrictlyWins
        );
    }

    #[test]
    fn missing_hot_is_conservative() {
        assert_eq!(
            compare_hot_to_cold_bound(OrderDirection::Asc, None, Some(b"a")),
            FrontierDecision::ColdMayWinOrTie
        );
    }
}
