//! Shared pg-koldstore domain types and helpers with no PostgreSQL or object-store dependency.
//!
//! Layout:
//! - [`domain`] — row/PK/seq/scope/filter/table models and snowflake ids
//! - [`sql`] — statement metadata, quoting, session literals, LSN helpers
//! - [`config`] — manage/flush options and GUC privilege policy
//! - [`error`] — shared error type (crate root)
//!
//! Top-level module names (`pk`, `session`, `scope`, …) are re-exported for
//! stable import paths. New shared types default to the matching folder.
//! Must not depend on any other `koldstore-*` crate.

pub mod config;
pub mod domain;
pub mod error;
pub mod sql;

// Stable top-level paths used across the workspace.
pub use config::privileges;
pub use domain::{filter, pk, row, scope, seq, snowflake, table_kind, table_name};
pub use sql::{ident, json, lsn, pg_type_name, session, strings};

pub use config::{
    flush_enabled_from_options, hot_row_limit_from_options, validate_max_rows_per_file,
    FlushPolicy, ManageTableOptions, MigrationStatus, MirrorCaptureMode, ParquetCompression,
    DEFAULT_MIN_MAX_ROWS_PER_FILE,
};
pub use error::{Diagnostic, KoldstoreError, Result};
pub use filter::{ColumnClass, Predicate, PredicateClass, PredicateValue};
pub use ident::{escape_sql_literal, is_safe_identifier, quote_ident, quote_qualified_ident};
pub use json::compare_json_values;
pub use lsn::format_pg_lsn;
pub use pg_type_name::canonical_postgres_type_name;
pub use pk::{
    LogicalPk, PgCollation, PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PkValue,
    PrimaryKeyColumnShape, PrimaryKeyShape, StablePkHash,
};
pub use privileges::{can_set_guc, RoleClass, INTERNAL_GUCS};
pub use row::{
    ChangeSource, ColdRow, HotRow, MirrorChange, MirrorOperation, MirrorState, Tombstone,
};
pub use scope::{
    active_scope_for_table, enforce_row_scope, normalize_scope, require_user_scope, scope_matches,
    scope_predicate_sql, ScopeError, ScopeSqlError,
};
pub use seq::{CommitSeq, ScopeKey, SeqId};
pub use session::{
    normalize_user_id, primary_key_default_clause, snowflake_default_expression,
    snowflake_id_call_expression, SessionSqlError, SessionSqlResult, SNOWFLAKE_ID_FUNCTION,
};
pub use snowflake::{next_id, worker_id, SnowflakeError, KOLDSTORE_EPOCH_MILLIS};
pub use sql::{map_sql_error, SqlAccess, SqlError, SqlParamType, SqlResult, SqlStatement};
pub use strings::dedupe_nonblank;
pub use table_kind::TableKind;
pub use table_name::{QualifiedTableName, TableName};
