//! Shared pg-koldstore types with no PostgreSQL or object-store dependency.

pub mod error;
pub mod filter;
pub mod ident;
pub mod pk;
pub mod row;
pub mod seq;
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
pub use seq::{CommitSeq, ScopeKey, SeqId};
pub use table_kind::TableKind;
pub use table_name::TableName;
