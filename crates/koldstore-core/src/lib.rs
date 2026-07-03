//! Shared pg-koldstore types with no PostgreSQL or object-store dependency.

pub mod error;
pub mod filter;
pub mod pk;
pub mod row;
pub mod seq;
pub mod table_kind;
pub mod table_name;

pub use error::{Diagnostic, KoldstoreError, Result};
pub use filter::{ColumnClass, Predicate, PredicateClass, PredicateValue};
pub use pk::{LogicalPk, PkColumn, PkValue, StablePkHash};
pub use row::{ColdRow, HotRow, RowEvent, RowOperation, Tombstone};
pub use seq::{CommitSeq, ScopeKey, SeqId};
pub use table_kind::TableKind;
pub use table_name::TableName;
