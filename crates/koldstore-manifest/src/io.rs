//! Manifest JSON load/write helpers.
//!
//! Durable object-store publish goes through `koldstore-storage`. Local path
//! helpers remain for tests and callers that already resolved an absolute path.
//! PostgreSQL SPI stays in `pg_koldstore`.

use std::path::Path;

use koldstore_storage::{
    open_filesystem_client, publish_mutable_object, ObjectStoreClient, StorageClient,
    StorageClientError,
};

use crate::model::Manifest;

/// Deserializes a manifest from JSON bytes.
///
/// # Errors
///
/// Returns an error when the payload is not a valid manifest document.
pub fn manifest_from_json_bytes(bytes: &[u8]) -> Result<Manifest, String> {
    serde_json::from_slice(bytes).map_err(|error| error.to_string())
}

/// Deserializes a manifest from a JSON string.
///
/// # Errors
///
/// Returns an error when the payload is not a valid manifest document.
pub fn manifest_from_json_str(text: &str) -> Result<Manifest, String> {
    serde_json::from_str(text).map_err(|error| error.to_string())
}

/// Serializes a manifest to compact JSON bytes.
///
/// # Errors
///
/// Returns an error when serialization fails.
pub fn manifest_to_json_bytes(manifest: &Manifest) -> Result<Vec<u8>, String> {
    serde_json::to_vec(manifest).map_err(|error| error.to_string())
}

/// Loads a manifest JSON file when present and valid.
#[must_use]
pub fn load_manifest_from_path(path: &Path) -> Option<Manifest> {
    let contents = std::fs::read(path).ok()?;
    manifest_from_json_bytes(&contents).ok()
}

/// Writes a manifest JSON file via atomic staged put when `base_path` is known.
///
/// Prefer [`write_manifest_with_client`] from flush finalize. This helper keeps
/// the absolute-path API used by tests: it opens a filesystem client rooted at
/// the parent of `path` and publishes the file name as the object key when
/// possible.
///
/// # Errors
///
/// Returns an error when serialization or durable write fails.
pub fn write_manifest_to_path(path: &Path, manifest: &Manifest) -> Result<(), String> {
    let bytes = manifest_to_json_bytes(manifest)?;
    let parent = path
        .parent()
        .ok_or_else(|| format!("manifest path missing parent: {}", path.display()))?;
    let client = open_filesystem_client(parent.to_string_lossy().as_ref())
        .map_err(|error| error.to_string())?;
    let key = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("manifest path missing file name: {}", path.display()))?;
    publish_mutable_object(&client, key, &bytes).map_err(|error| error.to_string())?;
    Ok(())
}

/// Publishes a manifest through an existing storage client.
///
/// # Errors
///
/// Returns an error when serialization or durable put fails.
pub fn write_manifest_with_client(
    client: &ObjectStoreClient,
    object_key: &str,
    manifest: &Manifest,
) -> Result<(), String> {
    let bytes = manifest_to_json_bytes(manifest)?;
    publish_mutable_object(client, object_key, &bytes).map_err(|error| error.to_string())?;
    Ok(())
}

/// Loads a manifest while distinguishing absence from storage or decode failure.
///
/// # Errors
///
/// Returns an error when object storage fails or the manifest bytes are invalid.
pub fn try_load_manifest_with_client(
    client: &dyn StorageClient,
    object_key: &str,
) -> Result<Option<Manifest>, String> {
    match client.get(object_key) {
        Ok(bytes) => manifest_from_json_bytes(&bytes).map(Some),
        Err(StorageClientError::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}
