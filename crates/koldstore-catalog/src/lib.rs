//! Serializable catalog models for cold-data bookkeeping.
//!
//! Owns cold segments, PK hints, managed-table snapshots, PG-free catalog **read**
//! SQL, decoding, and cache shapes. Keep separate from:
//! - `koldstore-schema`: table shape/registry (this crate depends on it one-way)
//! - `koldstore-mirror`: `__cl` DML/DDL SQL (catalog only stores/looks up
//!   `mirror_relation`; mirror builds upserts/stats against it)
//!
//! Schema registry **writes** live in `koldstore-migrate`; cold segment/manifest
//! **writes** live in `koldstore-flush`.

pub mod cache;
pub mod cold_pk_hints;
pub mod cold_segments;
pub mod decode;
pub mod queries;
pub mod table_meta;

pub use cache::{
    decode_managed_table_snapshot, decode_managed_table_snapshot_str, ManagedTableSnapshot,
    ManagedTableSnapshotCache, OptionalLookupCache,
};
pub use cold_pk_hints::{ColdPkHint, HintKind, PkLookup};
pub use cold_segments::{ColdSegment, SegmentVisibility};
pub use decode::column_stats_min_max_map;
pub use koldstore_common::FlushPolicy;
pub use table_meta::{FkPolicyDecision, ManagedTableMeta};
