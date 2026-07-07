//! Storage contracts for clean-schema change-log mirror tables.
//!
//! This crate owns the low-level mirror table API: relation naming, metadata
//! columns, table DDL, and primitive read/write SQL fragments. PostgreSQL
//! execution and higher-level workflows stay in `pg_koldstore`.

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
    plan_mirror_stats, plan_select_mirror_rows_after_seq,
    plan_select_mirror_rows_after_seq_with_params,
};
pub use relation::{
    mirror_relation_for_source, MirrorRelation, CHANGE_LOG_MIRROR_SUFFIX, KOLDSTORE_SCHEMA,
};
pub use row_json::{MirrorPolicyRowJson, MirrorSelectionRow, MirrorSeqStats};
pub use schema::{plan_drop_mirror_table, plan_mirror_schema, MirrorSchemaPlan};
pub use statement::{mirror_to_sql, MirrorAccess, MirrorStatement, SqlParamType};
pub use write::{
    mirror_delete_using_selected_sql, mirror_selected_join_predicate,
    plan_delete_selected_mirror_rows, plan_upsert_mirror_row, quoted_pk_columns,
    selected_record_columns,
};
