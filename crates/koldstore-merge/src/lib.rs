//! Hot/cold merge resolver, change-feed helpers, and RLS planning.

pub mod changelog;
pub mod dml;
pub mod events;
pub mod quals;
pub mod resolver;
pub mod rls;
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
pub use quals::{build_pruning_plan, classify_predicates, ClassifiedPredicates, PruningPlan};
pub use resolver::{resolve_rows, ResolvedRow, RowSource};
pub use rls::{
    enforce_or_fail_closed, plan_security_quals, unsupported_rls_error, SecurityQualPlan,
};
pub use tombstone::{tombstone_required, TombstoneDecision};
