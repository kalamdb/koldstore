//! Shared `__cl` mirror storage contract (strict and async).
//!
//! Naming, metadata columns, DDL, and primitive read/write SQL that both
//! capture modes share. Mode-specific planners live under `strict` / `async`.

pub mod columns;
pub mod error;
pub mod read;
pub mod relation;
pub mod row_json;
pub mod schema;
pub mod statement;
pub mod write;

pub use columns::MirrorColumn;
pub use error::{MirrorError, MirrorResult};
pub use read::{
    plan_mirror_force_flush_stats, plan_mirror_oldest_rows_max_seq, plan_mirror_op_stats,
    plan_mirror_stats, plan_select_mirror_rows_after_seq,
    plan_select_mirror_rows_after_seq_with_params,
};
pub use relation::{
    mirror_relation_for_source, MirrorRelation, CHANGE_LOG_MIRROR_SUFFIX, KOLDSTORE_SCHEMA,
};
pub use row_json::MirrorSeqStats;
pub use schema::{
    plan_drop_mirror_table, plan_mirror_pk_column_renames, plan_mirror_schema,
    plan_mirror_schema_with_order_key, MirrorSchemaPlan,
};
pub use statement::{mirror_to_sql, MirrorAccess, MirrorStatement, SqlParamType};
pub use write::{
    plan_async_mirror_batch_delete_existing, plan_async_mirror_batch_update,
    plan_async_mirror_batch_upsert, plan_upsert_mirror_row, quoted_pk_columns,
};
