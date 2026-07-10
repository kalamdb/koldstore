//! Hot/cold merge resolver, change-feed helpers, and RLS planning.

#[path = "core/changelog.rs"]
pub mod changelog;
#[path = "sql/dml.rs"]
pub mod dml;
#[path = "sql/events.rs"]
pub mod events;
#[path = "sql/managed_hook.rs"]
pub mod managed_hook;
#[path = "planning/quals.rs"]
pub mod quals;
#[path = "core/resolver.rs"]
pub mod resolver;
#[path = "planning/rls.rs"]
pub mod rls;
#[path = "scan/mod.rs"]
pub mod scan;
#[path = "core/tombstone.rs"]
pub mod tombstone;

pub use changelog::{changes_since, ChangeCursor, ChangeGap};
pub use dml::{
    allocate_seq_for_tests, delete_decision, delete_decision_with_flush_fence, plan_delete_row,
    plan_hydrate_pk, plan_standard_sql_cold_only_update, plan_update_row, stamp_dml_effect,
    ColdUpdateOutcome, DeleteDecision, DeleteInputState, DeleteRowRequest, DmlResult, DmlStamp,
    HydratePkRequest, ManagedDmlOperation, UpdateRowRequest, COLD_DML_FUNCTIONS,
};
pub use events::{
    plan_mirror_changes_since, ChangeFeedError, MirrorChangesSincePlan, DEFAULT_CHANGE_LIMIT,
};
pub use managed_hook::{
    extract_simple_pk_delete_predicate, plan_managed_delete_effect, plan_managed_insert_effect,
    plan_managed_update_effect, simple_pk_delete_supported, ManagedDmlEffect, SimplePkPredicate,
    HOT_DML_MANIFEST_SYNC_STATE,
};
pub use quals::{build_pruning_plan, classify_predicates, ClassifiedPredicates, PruningPlan};
pub use resolver::{resolve_rows, ResolvedRow, RowSource};
pub use rls::{
    enforce_or_fail_closed, plan_security_quals, unsupported_rls_error, SecurityQualPlan,
};
pub use scan::{
    begin_merge_scan, begin_merge_scan_with_plan, build_path_replacement,
    custom_scan_explain_label, evaluate_after_winner_resolution, execute_merge_scan,
    execute_merge_scan_with_filters, prune_segment_stats, prune_segment_stats_hints,
    replace_heap_final_path, ColdAvailability, FilterPlan, MergeMetadataAttnums, MergeScanError,
    MergeScanPlan, MergeScanResult, PathReplacementDecision, PlannerPath, PlannerPathKind,
    ScanResourceCounters, ScanState, SegmentHint, SegmentPrunePredicate, SegmentStatsHint,
    CUSTOM_PATH_NAME, HOT_SEQ_SENTINEL,
};
pub use tombstone::{tombstone_required, TombstoneDecision};
