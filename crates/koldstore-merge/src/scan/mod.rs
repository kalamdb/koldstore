//! CustomScan plan, path, and execution models.
//!
//! Owns PG-free merge-scan planning, path replacement, and hot/cold winner
//! resolution helpers. PostgreSQL CustomScan FFI stays in `pg_koldstore`.

pub mod exec;
pub mod path;
pub mod plan;

/// Hot heap rows use a sentinel sequence during winner resolution so any live
/// hot row beats every cold candidate for the same primary key.
pub const HOT_SEQ_SENTINEL: i64 = i64::MAX;

pub use exec::{
    begin_merge_scan, begin_merge_scan_with_plan, execute_merge_scan,
    execute_merge_scan_with_filters, ColdAvailability, FilterPlan, MergeScanError, MergeScanResult,
    ScanResourceCounters, ScanState,
};
pub use path::{
    build_path_replacement, clear_partial_heap_paths, custom_scan_explain_label,
    PathReplacementDecision, PlannerPath, PlannerPathKind, CUSTOM_PATH_NAME,
};
pub use plan::{
    group_segments_newest_first, retain_pre_merge_cold_prune_predicates,
    validate_prune_predicates_indexed, ColdPruneColumnPolicy, MergeMetadataAttnums, MergeScanPlan,
    MirrorOverlayStrategy, SegmentHint, SegmentPrunePredicate, SegmentStatsHint,
};
