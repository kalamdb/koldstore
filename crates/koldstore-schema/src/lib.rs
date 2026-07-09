//! Migrated-table schema registry (`koldstore.schemas`).
//!
//! Owns column sets, schema versions, type-matrix validation, and initialization
//! state for managed tables. Keep separate from `koldstore-catalog`: this crate
//! must stay free of cold-segment / manifest bookkeeping so migrate and parquet
//! can depend on it without pulling catalog SQL. Must not depend on `pgrx`.
//! SQL execution stays in `pg_koldstore`.

pub mod evolution;
pub mod pg_type;
pub mod schema_registry;
pub mod state;
pub mod type_matrix;

pub use evolution::{
    plan_schema_evolution, CatalogColumnShape, SchemaEvolutionAction, SchemaEvolutionError,
    SchemaEvolutionInput,
};
pub use pg_type::{PgIntegerArrayOid, PgType, SchemaError};
pub use schema_registry::{SchemaColumn, SchemaRegistryEntry};
pub use state::MirrorInitializationState;
pub use type_matrix::{normalize_type_name, TypeMatrix};
