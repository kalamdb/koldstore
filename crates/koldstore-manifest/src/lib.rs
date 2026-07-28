//! Kalamdb-compatible folder-sharded manifest model, assembly, I/O, and publish.
//!
//! Owns the on-disk export (`manifest.json` root + `{folder}/manifest-shard.json`),
//! catalog→manifest assembly, and path helpers. Catalog sync-state FSM lives in
//! `koldstore-catalog`. Must not depend on `pgrx`. Flush orchestration stays in
//! `koldstore-flush`; SPI stays in `pg_koldstore`.
//!
//! PostgreSQL remains query SoT; object manifests are derived export only.

pub mod assembly;
pub mod io;
pub mod lifecycle;
pub mod model;
pub mod paths;
mod shards;

pub use assembly::{
    build_manifest_segment_from_catalog_row, manifest_from_catalog_rows,
    manifest_relative_segment_path, ManifestAssemblyError,
};
pub use io::{
    manifest_from_json_bytes, manifest_to_json_bytes, try_load_manifest_from_path,
    try_load_manifest_with_client, write_manifest_to_path, write_manifest_with_client,
};
pub use koldstore_catalog::{CatalogManifestSegmentRow, SyncState};
pub use model::{
    FilesState, Manifest, ManifestBatchAppend, ManifestBloomFilter, ManifestColumnStats,
    ManifestSegment, ManifestShard, ManifestShardRef, PkFilter, PublishState, SegmentStatus,
    MANIFEST_SHARD_VERSION, MANIFEST_VERSION,
};
pub use paths::{
    manifest_paths, relative_manifest_path, relative_manifest_shard_object_path,
    relative_manifest_shard_path_for_folder, segment_folder_number, segment_object_path,
    segment_path_token, segment_relative_object_path, table_object_prefix,
    MANIFEST_SHARD_FILE_NAME, SEGMENTS_PER_FOLDER, SEGMENT_PATH_TOKEN_LEN,
};
