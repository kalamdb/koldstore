//! Migrated-table schema registry (`koldstore.schemas`).
//!
//! Owns column sets, schema versions, type-matrix validation, and initialization
//! state for managed tables. Must not depend on `pgrx`. SQL execution stays in
//! `pg_koldstore`.

pub mod schema_registry;
pub mod type_matrix;

pub use schema_registry::{SchemaColumn, SchemaRegistryEntry};
pub use type_matrix::TypeMatrix;
