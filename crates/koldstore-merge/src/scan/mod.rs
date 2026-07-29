//! CustomScan plan, path, and execution models.
//!
//! Owns PG-free merge-scan planning, path replacement, and hot/cold winner
//! resolution helpers. PostgreSQL CustomScan FFI stays in `pg_koldstore`.

pub mod exec;
pub mod materialize;
pub mod path;
pub mod plan;

pub use exec::{
    begin_merge_scan, begin_merge_scan_with_plan, evaluate_after_winner_resolution,
    execute_merge_scan, execute_merge_scan_with_filters, ColdAvailability, FilterPlan,
    MergeScanError, MergeScanResult, ScanResourceCounters, ScanState,
};
pub use materialize::HOT_SEQ_SENTINEL;
pub use path::{
    build_path_replacement, clear_partial_heap_paths, custom_scan_explain_label,
    replace_heap_final_path, PathReplacementDecision, PlannerPath, PlannerPathKind,
    CUSTOM_PATH_NAME,
};
pub use plan::{
    group_segments_newest_first, retain_pre_merge_cold_prune_predicates,
    validate_prune_predicates_indexed, ColdPruneColumnPolicy, MergeMetadataAttnums, MergeScanPlan,
    MirrorOverlayStrategy, SegmentHint, SegmentPrunePredicate, SegmentStatsHint,
};
