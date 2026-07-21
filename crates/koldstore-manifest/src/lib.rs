//! Kalamdb-compatible manifest model, assembly, I/O, and publish helpers.
//!
//! Owns the on-disk manifest document, catalog→manifest assembly, local JSON
//! load/write, and path helpers. Catalog sync-state FSM lives in
//! `koldstore-catalog`. Must not depend on `pgrx`. Flush orchestration stays in
//! `koldstore-flush`; SPI stays in `pg_koldstore`.

pub mod assembly;
pub mod io;
pub mod lifecycle;
pub mod model;
pub mod paths;

pub use assembly::{
    build_manifest_segment_from_catalog_row, manifest_from_catalog_rows,
    manifest_relative_segment_path, ManifestAssemblyError,
};
pub use io::{
    load_manifest_from_path, manifest_from_json_bytes, manifest_from_json_str,
    manifest_to_json_bytes, try_load_manifest_with_client, write_manifest_to_path,
    write_manifest_with_client,
};
pub use koldstore_catalog::{CatalogManifestSegmentRow, SyncState};
pub use model::{
    FilesState, Manifest, ManifestBatchAppend, ManifestBloomFilter, ManifestColumnStats,
    ManifestSegment, PkFilter, PublishState, SegmentStatus,
};
pub use paths::{
    manifest_paths, relative_manifest_path, segment_folder_number, segment_object_path,
    segment_path_token, segment_relative_object_path, table_object_prefix, SEGMENTS_PER_FOLDER,
    SEGMENT_PATH_TOKEN_LEN,
};
