//! Domain models shared across KoldStore crates.
//!
//! Owns identifiers, row/mirror shapes, sequences, scopes, filters, and
//! snowflake ids. SQL text helpers live in [`crate::sql`]; manage options live
//! in [`crate::config`].

pub mod commit_sequence;
pub mod filter;
pub mod pk;
pub mod row;
pub mod scope;
pub mod seq;
pub mod snowflake;
pub mod table_kind;
pub mod table_name;
