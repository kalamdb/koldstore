//! Storage contracts for clean-schema change-log mirror tables.
//!
//! Organized by capture mode while sharing one `__cl` storage contract:
//! - [`shared`] — naming, DDL, metadata columns, primitive read/write SQL
//! - [`strict`] — statement-trigger capture planners
//! - [`async`] — `pgoutput` decoder, apply-row helpers, batch flush policy
//!
//! Keep separate from `koldstore-catalog`: catalog resolves *which* mirror a
//! managed table uses; this crate builds SQL *against* that mirror. Must stay a
//! `koldstore-common`-only leaf. SPI/WAL/worker wiring stays in `pg_koldstore`.

pub mod r#async;
pub mod shared;
pub mod strict;

// Stable top-level paths (existing callers).
pub use r#async::{
    decode_message, must_flush_before_push, pg_value_json, pk_identity, primary_key_json,
    BatchFlushReason, PgOutputColumn, PgOutputDecodeError, PgOutputMessage, PgOutputRelation,
    PgOutputTuple, PgOutputValue, APPLY_BATCH_ROWS,
};
pub use shared::{
    mirror_delete_using_selected_sql, mirror_relation_for_source, mirror_selected_join_predicate,
    mirror_to_sql, plan_async_mirror_batch_insert, plan_async_mirror_batch_update,
    plan_delete_selected_mirror_rows, plan_drop_mirror_table, plan_mirror_oldest_rows_max_seq,
    plan_mirror_oldest_rows_stats, plan_mirror_op_stats, plan_mirror_schema, plan_mirror_stats,
    plan_select_mirror_rows_after_seq, plan_select_mirror_rows_after_seq_with_params,
    plan_upsert_mirror_row, quoted_pk_columns, selected_record_columns, MirrorAccess, MirrorColumn,
    MirrorError, MirrorRelation, MirrorResult, MirrorSchemaPlan, MirrorSelectionRow,
    MirrorSeqStats, MirrorStatement, SqlParamType, CHANGE_LOG_MIRROR_SUFFIX, KOLDSTORE_SCHEMA,
};
pub use strict::{
    async_worker_kick_trigger_name, async_worker_kick_trigger_names, plan_drop_mirror_dml_triggers,
    plan_mirror_capture, plan_mirror_capture_teardown, MirrorCaptureError, MirrorCapturePlan,
    MirrorCaptureResult,
};

// Module aliases so `koldstore_mirror::pgoutput` / `::batch` keep working.
pub use r#async::{apply_row, batch, pgoutput};
pub use shared::{columns, error, read, relation, row_json, schema, statement, write};
