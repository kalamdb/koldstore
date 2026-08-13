//! Clean-schema change-log mirror contracts.
//!
//! - [`shared`] — naming, DDL, metadata columns, primitive read/write SQL
//! - [`guard`] — PK / segment-order mutation guard planners
//! - [`async`] — `pgoutput` decoder, apply-row helpers, batch flush policy
//!
//! Authoritative mirror `seq` values are allocated only by the serialized WAL
//! applier. Keep separate from `koldstore-catalog`: catalog resolves *which*
//! mirror a managed table uses; this module builds SQL *against* that mirror.

pub mod r#async;
pub mod guard;
pub mod shared;

pub use guard::{
    pk_guard_trigger_name, plan_mirror_pk_guard, plan_mirror_source_teardown, MirrorGuardError,
    MirrorGuardResult, MirrorPkGuardPlan,
};
pub use r#async::{
    decode_message, must_flush_before_push, order_column_text, parse_pk_bool, parse_pk_ints,
    pg_value_text, pk_column_indexes, pk_identity, pk_type_oids, primary_key_cells,
    take_pk_cells_and_order_text, BatchFlushReason, PgOutputColumn, PgOutputDecodeError,
    PgOutputMessage, PgOutputRelation, PgOutputTuple, PgOutputValue, PkBindColumn, PkCell,
    PkIdentity, APPLY_BATCH_ROWS, BOOLOID, INT2OID, INT4OID, INT8OID,
};
pub use shared::{
    mirror_relation_for_source, mirror_seq_index_name, mirror_tombstone_index_name,
    plan_async_mirror_batch_delete_existing, plan_async_mirror_batch_update,
    plan_async_mirror_batch_upsert, plan_drop_mirror_table, plan_mirror_force_flush_stats,
    plan_mirror_oldest_rows_max_seq, plan_mirror_op_stats, plan_mirror_pk_column_renames,
    plan_mirror_relation_rename, plan_mirror_schema, plan_mirror_schema_with_order_key,
    plan_mirror_stats, plan_select_mirror_last_rows, plan_select_mirror_last_rows_with_params,
    plan_select_mirror_rows_after_seq, plan_select_mirror_rows_after_seq_with_params,
    plan_upsert_mirror_row, published_column_list, quoted_pk_columns, MirrorColumn, MirrorError,
    MirrorRelation, MirrorResult, MirrorSchemaPlan, MirrorSeqStats, SqlAccess, SqlParamType,
    SqlStatement, CHANGE_LOG_MIRROR_SUFFIX, KOLDSTORE_SCHEMA,
};

pub use r#async::{apply_row, batch, pgoutput};
pub use shared::{columns, error, read, relation, row_json, schema, statement, write};
