//! Manifest JSON load/write helpers (folder-sharded export only).
//!
//! Durable object-store publish goes through `koldstore-storage`. Local path
//! helpers remain for tests and callers that already resolved an absolute path.
//! PostgreSQL SPI stays in `pg_koldstore`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use koldstore_storage::{
    content_checksum_sha256_hex, join_object_key, open_filesystem_client, publish_immutable_object,
    publish_mutable_object, temp_object_key, unique_temp_file_name, ObjectStoreClient,
    StorageClient, StorageClientError,
};

use crate::model::{Manifest, ManifestShard, MANIFEST_VERSION};
use crate::shards::{merge_manifest_shards, split_manifest_for_export};

/// Deserializes a root or in-memory manifest from JSON bytes.
///
/// # Errors
///
/// Returns an error when the payload is not a valid manifest document.
pub fn manifest_from_json_bytes(bytes: &[u8]) -> Result<Manifest, String> {
    serde_json::from_slice(bytes).map_err(|error| error.to_string())
}

/// Serializes a manifest to compact JSON bytes.
///
/// # Errors
///
/// Returns an error when serialization fails.
pub fn manifest_to_json_bytes(manifest: &Manifest) -> Result<Vec<u8>, String> {
    serde_json::to_vec(manifest).map_err(|error| error.to_string())
}

/// Deserializes a folder shard document from JSON bytes.
///
/// # Errors
///
/// Returns an error when the payload is not a valid shard document.
pub(crate) fn manifest_shard_from_json_bytes(bytes: &[u8]) -> Result<ManifestShard, String> {
    serde_json::from_slice(bytes).map_err(|error| error.to_string())
}

/// Serializes a folder shard document to compact JSON bytes.
///
/// # Errors
///
/// Returns an error when serialization fails.
pub(crate) fn manifest_shard_to_json_bytes(shard: &ManifestShard) -> Result<Vec<u8>, String> {
    serde_json::to_vec(shard).map_err(|error| error.to_string())
}

/// Loads a root manifest from disk and merges folder shards.
///
/// # Errors
///
/// Returns an error when the root embeds segments, JSON is invalid, or a listed
/// shard file is missing.
pub fn try_load_manifest_from_path(path: &Path) -> Result<Option<Manifest>, String> {
    if !path.exists() {
        return Ok(None);
    }
    let contents = std::fs::read(path).map_err(|error| error.to_string())?;
    let root = manifest_from_json_bytes(&contents)?;
    let parent = path
        .parent()
        .ok_or_else(|| format!("manifest path missing parent: {}", path.display()))?;
    Ok(Some(merge_loaded_root(root, |shard_rel| {
        let shard_path = parent.join(shard_rel.trim_start_matches('/'));
        std::fs::read(&shard_path)
            .map_err(|error| format!("read shard {}: {error}", shard_path.display()))
    })?))
}

/// Splits `manifest` into folder shards and publishes them under the storage
/// root implied by an absolute `…/{ns}/{table}/manifest.json` path.
///
/// # Errors
///
/// Returns an error when the path shape is wrong, split fails, or durable write
/// fails.
pub fn write_manifest_to_path(path: &Path, manifest: &Manifest) -> Result<(), String> {
    let (storage_root, object_key) = storage_root_and_key_from_manifest_path(path)?;
    let client = open_filesystem_client(storage_root.to_string_lossy().as_ref())
        .map_err(|error| error.to_string())?;
    write_manifest_with_client(&client, &object_key, manifest)
}

/// Splits `manifest` into folder shards and publishes shards then the thin root.
///
/// Shard puts complete before the root so a crash mid-write never leaves a root
/// pointing at missing shard bodies. `object_key` is the root `…/manifest.json`.
///
/// # Errors
///
/// Returns an error when split, serialization, or durable put fails.
pub fn write_manifest_with_client(
    client: &ObjectStoreClient,
    object_key: &str,
    manifest: &Manifest,
) -> Result<(), String> {
    let export = split_manifest_for_export(manifest)?;
    let prefix = table_prefix_from_manifest_key(object_key)?;
    let previous_hashes = previous_shard_hashes(client, object_key, &export.root)?;
    for (relative_path, shard) in &export.shards {
        let shard_ref = export
            .root
            .shards
            .iter()
            .find(|shard_ref| shard_ref.path == *relative_path)
            .ok_or_else(|| format!("missing root reference for shard {relative_path}"))?;
        if previous_hashes.get(relative_path) == Some(&shard_ref.content_sha256) {
            continue;
        }
        let shard_key = join_object_key(&prefix, relative_path);
        let bytes = manifest_shard_to_json_bytes(shard)?;
        let temp_key = temp_object_key(
            &prefix,
            "manifest-shard",
            &unique_temp_file_name("manifest-shard.json"),
        );
        publish_immutable_object(client, &temp_key, &shard_key, &bytes)
            .map_err(|error| error.to_string())?;
    }
    let root_bytes = manifest_to_json_bytes(&export.root)?;
    publish_mutable_object(client, object_key, &root_bytes).map_err(|error| error.to_string())?;
    Ok(())
}

/// Loads a root manifest and merges folder shard segment lists.
///
/// # Errors
///
/// Returns an error when storage fails, the root embeds segments, JSON is
/// invalid, or a listed shard is missing.
pub fn try_load_manifest_with_client(
    client: &dyn StorageClient,
    object_key: &str,
) -> Result<Option<Manifest>, String> {
    let root = match client.get(object_key) {
        Ok(bytes) => manifest_from_json_bytes(&bytes)?,
        Err(StorageClientError::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    let prefix = table_prefix_from_manifest_key(object_key)?;
    Ok(Some(merge_loaded_root(root, |shard_rel| {
        let shard_key = join_object_key(&prefix, shard_rel);
        client.get(&shard_key).map_err(|error| error.to_string())
    })?))
}

fn merge_loaded_root(
    root: Manifest,
    mut load_shard: impl FnMut(&str) -> Result<Vec<u8>, String>,
) -> Result<Manifest, String> {
    let mut shards = Vec::with_capacity(root.shards.len());
    for shard_ref in &root.shards {
        let bytes = load_shard(&shard_ref.path)?;
        let actual_hash = content_checksum_sha256_hex(&bytes);
        if actual_hash != shard_ref.content_sha256 {
            return Err(format!(
                "shard {} content checksum mismatch",
                shard_ref.path
            ));
        }
        shards.push(manifest_shard_from_json_bytes(&bytes)?);
    }
    merge_manifest_shards(root, shards)
}

fn previous_shard_hashes(
    client: &dyn StorageClient,
    object_key: &str,
    next_root: &Manifest,
) -> Result<BTreeMap<String, String>, String> {
    let bytes = match client.get(object_key) {
        Ok(bytes) => bytes,
        Err(StorageClientError::NotFound { .. }) => return Ok(BTreeMap::new()),
        Err(error) => return Err(error.to_string()),
    };
    let Ok(root) = manifest_from_json_bytes(&bytes) else {
        return Ok(BTreeMap::new());
    };
    if root.version != MANIFEST_VERSION
        || !root.segments.is_empty()
        || root.table != next_root.table
        || root.namespace != next_root.namespace
    {
        return Ok(BTreeMap::new());
    }
    Ok(root
        .shards
        .into_iter()
        .map(|shard| (shard.path, shard.content_sha256))
        .collect())
}

fn table_prefix_from_manifest_key(object_key: &str) -> Result<String, String> {
    let key = object_key.trim_matches('/');
    key.strip_suffix("/manifest.json")
        .map(str::to_string)
        .ok_or_else(|| format!("manifest object key must end with /manifest.json: {object_key}"))
}

fn storage_root_and_key_from_manifest_path(path: &Path) -> Result<(PathBuf, String), String> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("manifest path missing file name: {}", path.display()))?;
    if file_name != "manifest.json" {
        return Err(format!(
            "manifest path must end with manifest.json: {}",
            path.display()
        ));
    }
    let table = path
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("manifest path missing table directory: {}", path.display()))?;
    let namespace = path
        .parent()
        .and_then(|parent| parent.parent())
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            format!(
                "manifest path missing namespace directory: {}",
                path.display()
            )
        })?;
    let storage_root = path
        .parent()
        .and_then(|parent| parent.parent())
        .and_then(|parent| parent.parent())
        .ok_or_else(|| format!("manifest path missing storage root: {}", path.display()))?
        .to_path_buf();
    Ok((storage_root, format!("{namespace}/{table}/manifest.json")))
}
