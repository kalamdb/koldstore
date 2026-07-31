//! Shared JSON comparison helpers.

use std::cmp::Ordering;

/// Compares JSON values when both sides share a comparable scalar type.
#[must_use]
pub fn compare_json_values(
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> Option<Ordering> {
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

/// Returns whether catalog/footer `[stats_min, stats_max]` may overlap a probe range.
///
/// Either probe bound may be omitted (open-ended). Missing, null, or incomparable
/// values return `true` so callers scan conservatively instead of false-negatives.
#[must_use]
pub fn column_stats_range_may_overlap(
    stats_min: &serde_json::Value,
    stats_max: &serde_json::Value,
    probe_min: Option<&serde_json::Value>,
    probe_max: Option<&serde_json::Value>,
) -> bool {
    if stats_min.is_null() || stats_max.is_null() {
        return true;
    }

    if let Some(min) = probe_min {
        if min.is_null() {
            return true;
        }
        match compare_json_values(stats_max, min) {
            Some(Ordering::Less) => return false,
            Some(_) => {}
            None => return true,
        }
    }

    if let Some(max) = probe_max {
        if max.is_null() {
            return true;
        }
        match compare_json_values(stats_min, max) {
            Some(Ordering::Greater) => return false,
            Some(_) => {}
            None => return true,
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::{column_stats_range_may_overlap, compare_json_values};
    use serde_json::json;
    use std::cmp::Ordering;

    #[test]
    fn compare_json_values_orders_numbers_and_strings() {
        assert_eq!(
            compare_json_values(&json!(1), &json!(2)),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_json_values(&json!("a"), &json!("b")),
            Some(Ordering::Less)
        );
        assert_eq!(compare_json_values(&json!(1), &json!("1")), None);
    }

    #[test]
    fn column_stats_range_may_overlap_open_and_closed() {
        assert!(column_stats_range_may_overlap(
            &json!(1),
            &json!(10),
            Some(&json!(5)),
            Some(&json!(15))
        ));
        assert!(!column_stats_range_may_overlap(
            &json!(1),
            &json!(10),
            Some(&json!(11)),
            None
        ));
    }
}
