//! Hot-to-cold flush workflow planning.
//!
//! Owns flush eligibility, job state transitions, manifest sync planning, segment
//! cleanup, and recovery classification. Must not depend on `pgrx`. PostgreSQL job
//! enqueue and SPI execution stay in `pg_koldstore`.

pub mod cleanup;
pub mod encode;
pub mod job;
pub mod ops;
pub mod pending_catalog;
pub mod policy;
pub mod pre_flush;
pub mod recovery;
pub mod scope_counters;
pub mod segment_catalog;
pub mod segment_write;
pub mod stats;
pub mod table_counters;
pub mod table_flush;
pub mod table_jobs;
pub mod worker;
pub mod write;

pub use cleanup::{
    plan_clean_schema_cleanup, plan_seq_range_cleanup, plan_typed_clean_schema_cleanup,
    CleanSchemaCleanupPlan, CleanupCatalogColumn,
};
pub use encode::{stream_flush_chunks, StreamEncodeInput, StreamEncodeOutcome};
pub use koldstore_jobs::{JobId, JobStatus, JobType, LeaseEpoch, StaleLeaseAction};
pub use ops::*;
pub use pending_catalog::{
    materialize_pending_upserts, plan_delete_pending, plan_delete_pending_for_scopes,
    plan_list_pending, plan_upsert_pending, PendingCatalogError, PendingUpsert,
};
pub use policy::policy_flush_row_count;
pub use pre_flush::{
    consume_pending_plans, flush_pending_threshold, pending_is_flushable, plan_pending_segments,
    PendingSegmentPlan, PreFlushInput,
};
pub use scope_counters::ScopeCounters;
pub use segment_catalog::{
    indexed_column_stats_json, plan_flush_segments_batch_insert, plan_manifest_row_upsert,
    SegmentCatalogError,
};
pub use segment_write::{
    write_flush_segment_file, write_flush_segment_with_client, WrittenFlushSegment,
};
pub use stats::{
    resolve_force_flush_selection, resolve_policy_flush_selection, validate_flush_row_selection,
    FlushStats, ResolvedFlushSelection, FORCE_TOMBSTONE_ONLY_CAP,
};
pub use table_counters::{
    plan_apply_flush_row_count_deltas, plan_bump_table_row_counts, plan_read_table_row_counters,
    plan_refresh_table_row_counters, TableRowCounters, FLUSH_MIRROR_FETCH_BATCH_SIZE,
};
pub use table_flush::{
    manifest_paths, max_rows_per_file_from_policy, TableFlushBatchOutcome,
    TableFlushPreparedContext,
};
// Re-export manifest assembly/I/O so existing flush callers keep a stable path.
pub use koldstore_manifest::{
    build_manifest_segment_from_catalog_row, load_manifest_from_path, manifest_from_catalog_rows,
    write_manifest_to_path, CatalogManifestSegmentRow,
};
pub use table_jobs::{
    plan_insert_inline_flush_job, plan_lookup_active_inline_flush_job,
    plan_mark_inline_flush_job_completed, plan_mark_inline_flush_job_failed,
    plan_mark_inline_flush_job_running, TableFlushJobError,
};
pub use write::FlushWriteChunk;
