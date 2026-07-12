//! Serializable catalog models for cold-data bookkeeping and versioned schema access.
//!
//! Owns cold segments, PK hints, managed-table snapshots, PG-free catalog **read**
//! SQL, decoding, cache shapes, and (in progress) versioned schema accessors.
//! Keep separate from:
//! - `koldstore-schema`: type matrix / evolution leaf (this crate depends on it one-way)
//! - `koldstore-mirror`: `__cl` DML/DDL SQL (catalog only stores/looks up
//!   `mirror_relation`; mirror builds upserts/stats against it)
//!
//! Cold segment/manifest **writes** live in `koldstore-flush`. Schema registry
//! **writes** are moving onto catalog version APIs (feature `003-column-id-lifecycle`).

pub mod cache;
pub mod cold_pk_hints;
pub mod decode;
pub mod queries;
pub mod schema_registry;
pub mod schema_versions;
pub mod segments;
pub mod table_meta;

pub use cache::{
    decode_managed_table_snapshot, decode_managed_table_snapshot_str, ManagedTableSnapshot,
    ManagedTableSnapshotCache,
};
pub use cold_pk_hints::{ColdPkHint, HintKind, PkLookup};
pub use decode::column_stats_min_max_map;
pub use koldstore_common::FlushPolicy;
pub use schema_registry::{SchemaColumn, SchemaRegistryEntry, SchemaVersion};
pub use schema_versions::{active_schema, allocate_column_id, decode_schema_version, schema_at};
pub use segments::{Segment, SegmentLifecycleError, SegmentVisibility};
pub use table_meta::{FkPolicyDecision, ManagedTableMeta};
