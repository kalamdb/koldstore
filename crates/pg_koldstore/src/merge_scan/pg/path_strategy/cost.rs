//! Plan-time costing helpers for KoldMergeScan path strategies.
//!
//! Costs are estimates layered on the native hot child. PostgreSQL may
//! interpolate startup vs total cost when a parent `LIMIT` is present.

/// Catalog lookup overhead added to CustomPath startup and total cost.
pub(crate) const CATALOG_LOOKUP_COST: f64 = 10.0;
/// Mirror overlay / winner-resolution overhead on total cost.
pub(crate) const MERGE_OVERLAY_COST: f64 = 5.0;
/// Per active cold segment estimate used at plan time (local catalog only).
pub(crate) const COLD_SEGMENT_COST: f64 = 25.0;

/// Startup bias for a general-merge (or other) path: catalog frontier lookup.
#[must_use]
pub(crate) const fn catalog_startup_bias() -> f64 {
    CATALOG_LOOKUP_COST
}

/// Total cost for a general merge wrapping `hot_total` with `segment_count`
/// published cold segments.
#[must_use]
pub(crate) fn general_merge_total_cost(hot_total: f64, segment_count: usize) -> f64 {
    hot_total
        + CATALOG_LOOKUP_COST
        + (segment_count as f64) * COLD_SEGMENT_COST
        + MERGE_OVERLAY_COST
}
