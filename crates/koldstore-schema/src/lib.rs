//! Migrated-table schema registry (`koldstore.schemas`).
//!
//! Owns column sets, schema versions, type-matrix validation, and initialization
//! state for managed tables. Must not depend on `pgrx`. SQL execution stays in
//! `pg_koldstore`.

pub mod pg_type;
pub mod schema_registry;
pub mod state;
pub mod type_matrix;

pub use pg_type::{PgIntegerArrayOid, PgType, SchemaError};
pub use schema_registry::{SchemaColumn, SchemaRegistryEntry};
pub use state::MirrorInitializationState;
pub use type_matrix::{normalize_type_name, TypeMatrix};
