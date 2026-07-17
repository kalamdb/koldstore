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
