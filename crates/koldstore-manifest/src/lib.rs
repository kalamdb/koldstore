//! Kalamdb-compatible manifest model, assembly, I/O, and publish helpers.
//!
//! Owns the on-disk manifest document, catalog→manifest assembly, local JSON
//! load/write, sync-state transitions, and publish planning. Must not depend on
//! `pgrx`. Flush orchestration stays in `koldstore-flush`; SPI stays in
//! `pg_koldstore`.

pub mod assembly;
#[path = "lifecycle/create.rs"]
pub mod create;
#[path = "lifecycle/drop.rs"]
pub mod drop;
pub mod io;
pub mod model;
pub mod paths;
#[path = "state/sync_state.rs"]
pub mod sync_state;
#[path = "lifecycle/update.rs"]
pub mod update;

pub use assembly::{
    build_manifest_segment_from_catalog_row, manifest_column_stats, manifest_from_catalog_rows,
    manifest_relative_segment_path, CatalogManifestSegmentRow, ManifestAssemblyError,
};
pub use io::{
    load_manifest_from_path, manifest_from_json_bytes, manifest_from_json_str,
    manifest_to_json_bytes, try_load_manifest_with_client, write_manifest_to_path,
    write_manifest_with_client,
};
pub use model::{
    FilesState, Manifest, ManifestBatchAppend, ManifestBloomFilter, ManifestColumnStats,
    ManifestSegment, PkFilter, PublishState, SegmentStatus,
};
pub use paths::{manifest_paths, relative_manifest_path, table_object_prefix};
pub use sync_state::SyncState;
