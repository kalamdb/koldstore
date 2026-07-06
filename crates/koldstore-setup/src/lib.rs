//! Extension install DDL plans for internal `koldstore` objects.
//!
//! Represents bootstrap tables, indexes, sequences, composite types, and grants as
//! ordered typed plans. Must not depend on `pgrx`. Bootstrap wiring and execution
//! stay in `pg_koldstore`.

pub mod bootstrap;
pub mod catalog_tables;
pub mod indexes;

pub use bootstrap::{BootstrapObjectKind, BootstrapObjectPlan, BootstrapPlan};
pub use catalog_tables::{missing_catalog_tables, CatalogTableSpec, REQUIRED_CATALOG_TABLES};
pub use indexes::{missing_catalog_indexes, CatalogIndexSpec, REQUIRED_CATALOG_INDEXES};
