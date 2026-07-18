//! Serializable catalog models for cold-data bookkeeping.
//!
//! Owns cold segments, managed-table snapshots, sync-state FSM, PG-free catalog
//! **read** SQL, decoding, and cache shapes. Keep separate from:
//! - `koldstore-schema`: table shape/registry (this crate depends on it one-way)
//! - `koldstore-mirror`: `__cl` DML/DDL SQL (catalog only stores/looks up
//!   `mirror_relation`; mirror builds upserts/stats against it)
//! - `koldstore-manifest`: derived object-store `manifest.json` (assembly/I/O)
//!
//! Schema registry **writes** live in `koldstore-migrate`; cold segment/manifest
//! **writes** live in `koldstore-flush`.

pub mod cache;
pub mod cold_segments;
pub mod decode;
pub mod manifest_row;
pub mod queries;
pub mod sync_state;

pub use cache::{
    decode_managed_table_snapshot, decode_managed_table_snapshot_str, BoundedOidCache,
    ManagedTableSnapshot, ManagedTableSnapshotCache, OptionalLookupCache,
    DEFAULT_OPTIONAL_LOOKUP_CACHE_LIMIT,
};
pub use cold_segments::SegmentVisibility;
pub use decode::{column_stats_min_max_map, column_stats_min_max_map_into};
pub use koldstore_common::FlushPolicy;
pub use manifest_row::CatalogManifestSegmentRow;
pub use sync_state::SyncState;
