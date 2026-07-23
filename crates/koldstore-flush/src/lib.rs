//! Hot-to-cold flush workflow planning.
//!
//! Owns flush eligibility, job state transitions, manifest sync planning, segment
//! cleanup, and recovery classification. Must not depend on `pgrx`. PostgreSQL job
//! enqueue and SPI execution stay in `pg_koldstore`.

pub mod cleanup;
pub mod encode;
pub mod job;
pub mod ops;
pub mod policy;
pub mod recovery;
pub mod scheduler;
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
pub use ops::*;
pub use policy::policy_flush_row_count;
pub use scheduler::{scheduler_should_flush, scheduler_should_flush_parsed};
pub use segment_catalog::{
    indexed_column_stats_json, plan_activate_flush_segments, plan_flush_segments_batch_insert,
    SegmentCatalogError,
};
pub use segment_write::{
    flush_segment_object_path, write_flush_segment_file, write_flush_segment_with_client,
    WrittenFlushSegment,
};
pub use stats::{
    apply_force_flush_wave_cap, resolve_force_flush_selection, resolve_policy_flush_selection,
    validate_flush_row_selection, FlushStats, ResolvedFlushSelection, FORCE_FLUSH_WAVE_ROW_CAP,
    FORCE_TOMBSTONE_ONLY_CAP,
};
pub use table_counters::{
    flush_mirror_fetch_limit, plan_apply_flush_row_count_deltas, plan_bump_table_row_counts,
    plan_read_table_row_counters, plan_refresh_table_row_counters, TableRowCounters,
    FLUSH_MIRROR_FETCH_BATCH_SIZE,
};
pub use table_flush::{
    max_rows_per_file_from_policy, relative_manifest_path, TableFlushBatchOutcome,
    TableFlushPreparedContext,
};
// Re-export manifest assembly/I/O so existing flush callers keep a stable path.
pub use koldstore_catalog::CatalogManifestSegmentRow;
pub use koldstore_manifest::{
    build_manifest_segment_from_catalog_row, load_manifest_from_path, manifest_from_catalog_rows,
    write_manifest_to_path,
};
pub use table_jobs::{
    plan_insert_inline_flush_job, plan_lookup_active_inline_flush_job,
    plan_mark_inline_flush_job_completed, plan_mark_inline_flush_job_failed,
    plan_mark_inline_flush_job_running, plan_update_inline_flush_job_progress, TableFlushJobError,
};
pub use write::FlushWriteChunk;
