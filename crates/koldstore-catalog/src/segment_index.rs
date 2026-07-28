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
