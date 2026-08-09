//! Hot-to-cold flush workflow planning.
//!
//! Owns flush eligibility, job state transitions, manifest sync planning, segment
//! cleanup, and recovery classification. Must not depend on `pgrx`. PostgreSQL job
//! enqueue and SPI execution stay in `pg_koldstore`.
//!
//! The lower storage stack is exposed explicitly so the supervision tree reads
//! naturally as `supervisor::flush::{manifest, storage, parquet}`.

/// Manifest publication and generation contracts beneath flush.
pub use koldstore_manifest as manifest;
/// Parquet encoding/decoding beneath flush.
pub use koldstore_parquet as parquet;
/// Object-storage backends beneath flush.
pub use koldstore_storage as storage;

pub mod cleanup;
pub mod encode;
pub mod failpoints;
pub mod jobs_sql;
pub mod ops;
pub mod policy;
pub mod recovery;
pub mod retention;
pub mod scheduler;
pub mod segment_catalog;
pub mod segment_write;
pub mod stats;
pub mod table_counters;
pub mod table_flush;
pub mod table_jobs;
pub mod write;

pub use failpoints::{FailpointAction, FlushFailpoint, FAILPOINT_NAMES};

pub use cleanup::{
    cleanup_allowed, plan_seq_range_cleanup, retain_tombstone, CleanSchemaCleanupPlan,
};
pub use encode::{
    stream_flush_chunks, MirrorFlushPageCursor, StreamEncodeInput, StreamEncodeOutcome,
};
pub use ops::{
    classify_command, flush_table_request, plan_count_pending_flush_jobs,
    plan_enqueue_or_lookup_flush_job, plan_koldstore_exec, plan_mirror_flush_selection_batch,
    plan_mirror_flush_selection_batch_with_order_key, plan_next_pending_flush_due_epoch_ms,
    plan_select_pending_flush_candidates, plan_select_pending_flush_candidates_after,
    sql_param_cast, table_status_plan, FlushJobEnqueuePlan, FlushRequest, KoldstoreExecPlan,
    MirrorFlushSelectionPlan, OpsCommand, OpsError,
};
pub use policy::{policy_flush_row_count, selected_rows_meet_file_minimum};
pub use retention::{plan_purge_old_jobs, JobRetentionError, DEFAULT_PURGE_BATCH_LIMIT};
pub use scheduler::{
    evaluate_older_than_scan, plan_older_than_eligible_mirror_rows,
    plan_select_auto_flush_candidate_tables, scheduler_should_flush, scheduler_should_flush_parsed,
    AutoFlushPlanError, OlderThanEvaluation, AUTO_FLUSH_TABLE_PREDICATE,
};
pub use segment_catalog::{
    plan_activate_flush_segments, plan_flush_segments_batch_insert, SegmentCatalogError,
};
pub use segment_write::{
    flush_segment_relative_path, write_flush_segment_with_client, WrittenFlushSegment,
};
pub use stats::{
    apply_force_flush_pass_cap, resolve_force_flush_selection, resolve_policy_flush_selection,
    should_continue_flush_catchup, should_start_catchup_pass, validate_flush_row_selection,
    FlushStats, ResolvedFlushSelection, FORCE_FLUSH_PASS_ROW_CAP, FORCE_TOMBSTONE_ONLY_CAP,
};
pub use table_counters::{
    flush_mirror_fetch_limit, plan_apply_flush_row_count_deltas, plan_bump_table_row_counts,
    plan_refresh_table_row_counters, TableRowCounters, FLUSH_MIRROR_FETCH_BATCH_SIZE,
};
pub use table_flush::{max_rows_per_file_from_policy, TableFlushBatchOutcome};
pub use table_jobs::{
    flush_phase, plan_cancel_jobs_for_drop, plan_clear_table_cancel_request,
    plan_flush_cancel_requested, plan_flush_job_is_completed, plan_insert_flush_job,
    plan_list_jobs, plan_list_running_flush_table_oids, plan_lookup_active_flush_job,
    plan_mark_flush_job_cancelled, plan_mark_flush_job_completed,
    plan_mark_flush_job_completed_after_cancel, plan_mark_flush_job_failed,
    plan_mark_flush_job_running, plan_reclaim_running_flush_jobs, plan_request_cancel_job,
    plan_request_cancel_table_jobs, plan_update_flush_job_progress, TableFlushJobError,
};
pub use write::FlushWriteChunk;
