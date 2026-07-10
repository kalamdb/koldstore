//! Relative and absolute manifest path helpers for a managed table.

use std::path::PathBuf;

/// Object-store table prefix `{namespace}/{table_name}`.
#[must_use]
pub fn table_object_prefix(namespace: &str, table_name: &str) -> String {
    format!("{namespace}/{table_name}")
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
