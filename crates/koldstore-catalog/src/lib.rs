//! Serializable catalog models for cold-data bookkeeping.
//!
//! Owns cold segments, PK hints, managed table metadata, PG-free catalog SQL
//! builders, decoding, and cache shapes. Migrated-table schema registry models
//! live in `koldstore-schema`.

pub mod cache;
pub mod cold_pk_hints;
pub mod cold_segments;
pub mod decode;
pub mod queries;
pub mod table_meta;

pub use cache::{decode_managed_table_snapshot, ManagedTableSnapshot, ManagedTableSnapshotCache};
pub use cold_pk_hints::{ColdPkHint, HintKind, PkLookup};
pub use cold_segments::{ColdSegment, SegmentVisibility};
pub use table_meta::{FkPolicyDecision, FlushPolicy, ManagedTableMeta};
