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
pub use domain::{
    column, filter, object_keys, pk, row, scope, segment_paths, seq, snowflake,
    storage_id, table_kind, table_name,
};
pub use sql::{ident, json, lsn, pg_type_name, session, strings};

pub use column::{ColumnId, ColumnRef};
pub use config::{
    flush_enabled_from_options, hot_row_limit_from_options, validate_max_rows_per_file,
    FlushPolicy, ManageTableOptions, MigrationStatus, MoveAfter, ParquetCompression,
    DEFAULT_MAX_ROWS_PER_FLUSH, DEFAULT_MIN_MAX_ROWS_PER_FILE,
};
pub use error::{Diagnostic, KoldstoreError, Result};
pub use filter::{ColumnClass, Predicate, PredicateClass, PredicateValue};
pub use ident::{escape_sql_literal, is_safe_identifier, quote_ident, quote_qualified_ident};
pub use json::{column_stats_range_may_overlap, compare_json_values};
pub use lsn::{format_pg_lsn, parse_pg_lsn, AppliedWalBoundary, WalFenceLsn};
pub use object_keys::{join_object_key, manifest_object_key, normalize_table_prefix};
pub use pg_type_name::canonical_postgres_type_name;
pub use pk::{
    LogicalPk, LogicalPkValues, PgCollation, PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal,
    PkValue, PrimaryKeyColumnShape, PrimaryKeyShape, StablePkHash,
};
pub use privileges::{can_set_guc, RoleClass, INTERNAL_GUCS};
pub use row::{
    ChangeSource, ColdRow, HotRow, MirrorChange, MirrorOperation, MirrorState, Tombstone,
};
pub use scope::{
    active_scope_for_table, enforce_row_scope, normalize_scope, require_user_scope, scope_matches,
    scope_predicate_sql, ScopeError, ScopeSqlError,
};
pub use segment_paths::{
    segment_folder_number, segment_path_token, segment_relative_object_path, SEGMENTS_PER_FOLDER,
    SEGMENT_PATH_TOKEN_LEN,
};
pub use seq::{ScopeKey, SeqId};
pub use session::{
    normalize_user_id, primary_key_default_clause, snowflake_default_expression,
    snowflake_id_call_expression, SessionSqlError, SessionSqlResult, SNOWFLAKE_ID_FUNCTION,
};
pub use snowflake::{
    minimum_id_at_unix_millis, next_id, next_id_after, worker_id, SnowflakeError,
    KOLDSTORE_EPOCH_MILLIS,
};
pub use sql::{map_sql_error, SqlAccess, SqlError, SqlParamType, SqlResult, SqlStatement};
pub use storage_id::StorageId;
pub use strings::dedupe_nonblank;
pub use table_kind::TableKind;
pub use table_name::{QualifiedTableName, TableName};
