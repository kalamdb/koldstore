//! Shared pg-koldstore domain types and helpers with no PostgreSQL or object-store dependency.
//!
//! New shared identifiers, row models, sequence types, and pure validation helpers belong here.
//! Must not depend on any other `koldstore-*` crate.

pub mod error;
pub mod filter;
pub mod ident;
pub mod pk;
pub mod row;
pub mod scope;
pub mod seq;
pub mod session;
pub mod snowflake;
pub mod sql;
pub mod table_kind;
pub mod table_name;

pub use error::{Diagnostic, KoldstoreError, Result};
pub use filter::{ColumnClass, Predicate, PredicateClass, PredicateValue};
pub use ident::{is_safe_identifier, quote_ident};
pub use pk::{
    LogicalPk, PgCollation, PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PkValue,
    PrimaryKeyColumnShape, PrimaryKeyShape, StablePkHash,
};
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
pub use table_kind::TableKind;
pub use table_name::{QualifiedTableName, TableName};
