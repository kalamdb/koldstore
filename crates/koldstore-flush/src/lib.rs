//! Hot-to-cold flush workflow planning.
//!
//! Owns flush eligibility, job state transitions, manifest sync planning, segment
//! cleanup, and recovery classification. Must not depend on `pgrx`. PostgreSQL job
//! enqueue and SPI execution stay in `pg_koldstore`.

pub mod cleanup;
pub mod job;
pub mod ops;
pub mod policy;
pub mod recovery;
pub mod segment_catalog;
pub mod stats;
pub mod table_flush;
pub mod table_jobs;
pub mod worker;
pub mod write;

pub use koldstore_jobs::{JobId, JobStatus, JobType, LeaseEpoch, StaleLeaseAction};
pub use ops::*;
pub use segment_catalog::{
    manifest_from_catalog_rows, plan_flush_cold_segment_insert, plan_flush_pk_hint_insert,
    plan_manifest_row_upsert, CatalogManifestSegmentRow, SegmentCatalogError,
};
pub use stats::{
    decode_mirror_policy_rows, flush_stats_for_rows, resolve_flush_stats,
    validate_flush_row_selection, FlushStats,
};
pub use table_flush::{
    manifest_paths, max_rows_per_file_from_policy, TableFlushBatchOutcome,
    TableFlushPreparedContext,
};
pub use table_jobs::{
    plan_insert_inline_flush_job, plan_lookup_active_inline_flush_job,
    plan_mark_inline_flush_job_completed, plan_mark_inline_flush_job_failed,
    plan_mark_inline_flush_job_running, TableFlushJobError,
};
pub use write::{
    chunk_flush_write_input, plan_flush_cleanup_row, plan_flush_cold_record,
    plan_flush_write_input, FlushWriteChunk, FlushWriteInput,
};
