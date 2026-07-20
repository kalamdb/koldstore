//! Relative and absolute manifest path helpers for a managed table.
//!
//! Cold segments use one table-wide layout (no per-scope object prefixes):
//! `{namespace}/{table}/{folder:03}/segment-{NNNN}-{token}.parquet`.
//! Manifest segment paths store the table-relative form
//! `{folder:03}/segment-{NNNN}-{token}.parquet`.
//!
//! `token` is a short hex derived from the catalog segment UUID — enough to
//! disambiguate orphan retries without embedding the full UUID in the key.

use std::path::PathBuf;

/// Max segments per numeric folder before rolling to the next (`001/`, `002/`, …).
pub const SEGMENTS_PER_FOLDER: u32 = 100;

/// Hex characters from the segment id used in object names (32 bits).
pub const SEGMENT_PATH_TOKEN_LEN: usize = 8;

/// Object-store table prefix `{namespace}/{table_name}`.
#[must_use]
pub fn table_object_prefix(namespace: &str, table_name: &str) -> String {
    format!("{namespace}/{table_name}")
}

/// Folder number for a batch (`1` → `001/`, `101` → `002/`, …).
///
/// Uses 1-based batch numbering with [`SEGMENTS_PER_FOLDER`] segments per folder.
/// `batch_number <= 0` maps to folder `1` (test / edge paths).
#[must_use]
pub fn segment_folder_number(batch_number: i32) -> u32 {
    let n = u32::try_from(batch_number.max(1)).unwrap_or(1);
    (n - 1) / SEGMENTS_PER_FOLDER + 1
}

/// Short path token from a segment UUID (dashes ignored, first 8 hex chars).
///
/// Catalog identity stays a full UUID; object keys only need collision resistance
/// across retries at the same `batch_number`.
#[must_use]
pub fn segment_path_token(segment_id: impl std::fmt::Display) -> String {
    segment_id
        .to_string()
        .chars()
        .filter(|ch| *ch != '-')
        .take(SEGMENT_PATH_TOKEN_LEN)
        .collect()
}

/// Table-relative segment path stored in the manifest (`001/segment-0001-{token}.parquet`).
///
/// `path_token` is typically [`segment_path_token`] of the catalog segment id.
#[must_use]
pub fn segment_relative_object_path(batch_number: i32, path_token: impl AsRef<str>) -> String {
    let folder = segment_folder_number(batch_number);
    let batch = u32::try_from(batch_number.max(0)).unwrap_or(0);
    let token = path_token.as_ref();
    format!("{folder:03}/segment-{batch:04}-{token}.parquet")
}

/// Full object key under the table prefix.
#[must_use]
pub fn segment_object_path(
    prefix: &str,
    batch_number: i32,
    path_token: impl AsRef<str>,
) -> String {
    let prefix = prefix.trim_matches('/');
    format!(
        "{prefix}/{}",
        segment_relative_object_path(batch_number, path_token)
    )
}

/// Relative manifest path under the table prefix (`…/manifest.json`).
#[must_use]
pub fn relative_manifest_path(namespace: &str, table_name: &str) -> String {
    format!(
        "{}/manifest.json",
        table_object_prefix(namespace, table_name)
    )
}

/// Relative and absolute manifest paths for a managed table.
#[must_use]
pub fn manifest_paths(namespace: &str, table_name: &str, base_path: &str) -> (String, PathBuf) {
    let manifest_path = relative_manifest_path(namespace, table_name);
    let absolute_manifest_path = PathBuf::from(base_path).join(&manifest_path);
    (manifest_path, absolute_manifest_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folder_rolls_every_hundred_segments() {
        assert_eq!(segment_folder_number(1), 1);
        assert_eq!(segment_folder_number(100), 1);
        assert_eq!(segment_folder_number(101), 2);
        assert_eq!(segment_folder_number(200), 2);
        assert_eq!(segment_folder_number(201), 3);
        assert_eq!(segment_folder_number(0), 1);
    }

    #[test]
    fn path_token_is_short_hex_from_uuid() {
        assert_eq!(
            segment_path_token("a0dbcb97-3976-44fa-9638-48be5e85a778"),
            "a0dbcb97"
        );
        assert_eq!(
            segment_path_token("11111111-1111-1111-1111-111111111111"),
            "11111111"
        );
    }

    #[test]
    fn segment_paths_are_padded_and_table_relative() {
        let token = segment_path_token("11111111-1111-1111-1111-111111111111");
        assert_eq!(
            segment_relative_object_path(1, &token),
            "001/segment-0001-11111111.parquet"
        );
        assert_eq!(
            segment_object_path("app/items", 101, &token),
            "app/items/002/segment-0101-11111111.parquet"
        );
    }
}
