//! Domain models shared across KoldStore crates.
//!
//! Owns identifiers, row/mirror shapes, typed cells (`cell`), sequences, scopes,
//! filters, and snowflake ids. SQL text helpers live in [`crate::sql`]; manage
//! options live in [`crate::config`].

pub mod cell;
pub mod column;
pub mod filter;
pub mod object_keys;
pub mod pk;
pub mod row;
pub mod scope;
pub mod segment_paths;
pub mod seq;
pub mod snowflake;
pub mod storage_id;
pub mod table_kind;
pub mod table_name;
pub mod table_oid;
pub mod time;
