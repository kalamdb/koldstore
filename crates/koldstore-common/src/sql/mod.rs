//! SQL text and planning helpers with no PostgreSQL runtime dependency.
//!
//! Statement metadata, identifier quoting, session SQL literals, LSN formatting,
//! and related pure helpers live here. Domain row/PK models live in
//! [`crate::domain`].

mod statement;

pub mod ident;
pub mod json;
pub mod lsn;
pub mod pg_type_name;
pub mod session;
pub mod strings;

pub use statement::{map_sql_error, SqlAccess, SqlError, SqlParamType, SqlResult, SqlStatement};
