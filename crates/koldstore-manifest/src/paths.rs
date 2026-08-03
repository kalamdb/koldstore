//! Relative and absolute manifest path helpers for a managed table.
//!
//! Segment folder/token layout lives in [`koldstore_common::segment_paths`].
//! This module owns table-prefix defaults used by export/tests and shard file
//! names. Live flush/scan join via rendered `regular_path_tmpl` +
//! [`koldstore_storage::join_object_key`] / [`koldstore_storage::manifest_object_key`].

use std::path::PathBuf;

use koldstore_common::{manifest_object_key, normalize_table_prefix};
pub use koldstore_common::{
    segment_folder_number, segment_path_token, segment_relative_object_path, SEGMENTS_PER_FOLDER,
    SEGMENT_PATH_TOKEN_LEN,
};

/// Hex characters retained from the shard content SHA-256 in object names.
///
/// The root manifest keeps the complete digest for integrity verification.
pub const MANIFEST_SHARD_PATH_HASH_HEX_LEN: usize = 32;

/// Object-store table prefix `{namespace}/{table_name}` (no trailing slash).
///
/// Prefer a rendered `regular_path_tmpl` prefix for production keys; this helper
/// matches the default `{namespace}/{tableName}/` template only.
#[must_use]
pub fn table_object_prefix(namespace: &str, table_name: &str) -> String {
    format!("{namespace}/{table_name}")
}

/// Relative manifest path under the default table prefix (`…/manifest.json`).
#[must_use]
pub fn relative_manifest_path(namespace: &str, table_name: &str) -> String {
    manifest_object_key(&normalize_table_prefix(&table_object_prefix(
        namespace, table_name,
    )))
}

/// Immutable shard path using a 128-bit prefix of the content SHA-256 digest.
///
/// The complete digest remains in the root manifest and is verified when the
/// shard is loaded. Immutable publication rejects a rare prefix collision when
/// an existing object has different bytes.
#[must_use]
pub fn relative_manifest_shard_content_path(folder: u32, content_sha256: &str) -> String {
    let path_token = content_sha256
        .get(..MANIFEST_SHARD_PATH_HASH_HEX_LEN)
        .unwrap_or(content_sha256);
    format!("{folder:03}/manifest-shard-{path_token}.json")
}

/// First path component of a table-relative segment path (`001/segment-….parquet`).
#[must_use]
pub(crate) fn folder_from_segment_relative_path(path: &str) -> Option<&str> {
    let (folder, file_name) = path.split_once('/')?;
    if parse_folder_name(folder).is_none()
        || file_name.is_empty()
        || file_name.contains('/')
        || file_name == "."
        || file_name == ".."
    {
        return None;
    }
    Some(folder)
}

/// Parses a zero-padded folder name (`001`) into its numeric folder id.
#[must_use]
pub(crate) fn parse_folder_name(folder: &str) -> Option<u32> {
    let number = folder.parse::<u32>().ok()?;
    (number > 0 && folder == format!("{number:03}")).then_some(number)
}

/// Relative and absolute manifest paths for a managed table (default template).
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
    fn path_token_is_short_hex_from_uuid() {
        assert_eq!(
            segment_path_token("a0dbcb97-3976-44fa-9638-48be5e85a778"),
            "a0dbcb97"
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
            koldstore_common::join_object_key(
                "app/items",
                &segment_relative_object_path(101, &token)
            ),
            "app/items/002/segment-0101-11111111.parquet"
        );
    }

    #[test]
    fn shard_paths_and_folder_parse() {
        assert_eq!(
            relative_manifest_shard_content_path(1, "abcd"),
            "001/manifest-shard-abcd.json"
        );
        assert_eq!(
            folder_from_segment_relative_path("001/segment-0001-a.parquet"),
            Some("001")
        );
        assert_eq!(parse_folder_name("001"), Some(1));
        assert_eq!(parse_folder_name("1"), None);
    }

    #[test]
    fn relative_manifest_uses_storage_join() {
        assert_eq!(
            relative_manifest_path("app", "items"),
            "app/items/manifest.json"
        );
    }
}
