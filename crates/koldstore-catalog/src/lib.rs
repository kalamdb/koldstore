//! Serializable catalog models and validation logic.

pub mod cold_pk_hints;
pub mod cold_segments;
pub mod row_events;
pub mod schema_registry;
pub mod table_meta;
pub mod type_matrix;

pub use cold_pk_hints::{ColdPkHint, HintKind, PkLookup};
pub use cold_segments::{ColdSegment, SegmentVisibility};
pub use row_events::CatalogRowEvent;
pub use schema_registry::{SchemaColumn, SchemaRegistryEntry};
pub use table_meta::{FkPolicyDecision, FlushPolicy, ManagedTableMeta, MirrorInitializationState};
pub use type_matrix::{PgTypeClass, TypeMatrix, TypeSupport};
