//! PostgreSQL type support and schema-evolution policy.
//!
//! Catalog-owned schema versions live in `koldstore-catalog`. This leaf crate
//! stays free of catalog and `pgrx` dependencies.

pub mod evolution;
pub mod pg_type;
pub mod state;
pub mod type_matrix;

pub use evolution::{
    plan_schema_evolution, ActiveColumnShape, CatalogColumnShape, SchemaEvolutionAction,
    SchemaEvolutionError, SchemaEvolutionInput,
};
pub use pg_type::{PgIntegerArrayOid, PgType, SchemaError};
pub use state::MirrorInitializationState;
pub use type_matrix::{normalize_type_name, TypeMatrix};
