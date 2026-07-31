//! Storage contracts for clean-schema change-log mirror tables.
//!
//! - [`shared`] — naming, DDL, metadata columns, primitive read/write SQL
//! - [`guard`] — PK / segment-order mutation guard planners
//! - [`async`] — `pgoutput` decoder, apply-row helpers, batch flush policy
//!
//! Authoritative mirror `seq` values are allocated only by the serialized WAL
//! applier. Keep separate from `koldstore-catalog`: catalog resolves *which*
//! mirror a managed table uses; this crate builds SQL *against* that mirror.
//! Must stay a `koldstore-common`-only leaf. SPI/WAL/worker wiring stays in
//! `pg_koldstore`.

pub mod r#async;
pub mod guard;
pub mod shared;

// Stable top-level paths (existing callers).
pub use r#async::{
    decode_message, must_flush_before_push, pg_value_json, pg_value_text, pk_identity,
    primary_key_json, BatchFlushReason, PgOutputColumn, PgOutputDecodeError, PgOutputMessage,
    PgOutputRelation, PgOutputTuple, PgOutputValue, APPLY_BATCH_ROWS,
};
pub use guard::{
    plan_mirror_pk_guard, plan_mirror_source_teardown, MirrorGuardError, MirrorGuardResult,
    MirrorPkGuardPlan,
};
pub use shared::{
    mirror_relation_for_source, mirror_to_sql, plan_async_mirror_batch_delete_existing,
    plan_async_mirror_batch_update, plan_async_mirror_batch_upsert, plan_drop_mirror_table,
    plan_mirror_force_flush_stats, plan_mirror_oldest_rows_max_seq, plan_mirror_op_stats,
    plan_mirror_pk_column_renames, plan_mirror_schema, plan_mirror_schema_with_order_key,
    plan_mirror_stats, plan_select_mirror_rows_after_seq,
    plan_select_mirror_rows_after_seq_with_params, plan_upsert_mirror_row, quoted_pk_columns,
    MirrorAccess, MirrorColumn, MirrorError, MirrorRelation, MirrorResult, MirrorSchemaPlan,
    MirrorSeqStats, MirrorStatement, SqlParamType, CHANGE_LOG_MIRROR_SUFFIX, KOLDSTORE_SCHEMA,
};

// Module aliases so `koldstore_mirror::pgoutput` / `::batch` keep working.
pub use r#async::{apply_row, batch, pgoutput};
pub use shared::{columns, error, read, relation, row_json, schema, statement, write};

/// Compatibility alias used by demigrate teardown.
pub use plan_mirror_source_teardown as plan_mirror_capture_teardown;
