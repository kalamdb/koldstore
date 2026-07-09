//! Durable publish helpers for cold segments and manifests.
//!
//! Executes the backend-safe publish sequence without assuming atomic rename
//! across cloud backends. On local disk, `object_store` stages then renames /
//! hard-links with optional fsync; cloud backends rely on atomic `put` /
//! `copy_if_not_exists`.
//!
//! Invariants:
//! - Final segment keys are never written in place (CreateIfAbsent only).
//! - A successful Create that races on retry is treated as success when the
//!   existing final object already matches the expected byte size.
//! - Temp objects are deleted only after the final object validates.
//! - Manifest overwrite still uses atomic staged put (never truncate-in-place).

use uuid::Uuid;

use crate::client::{ObjectStoreClient, PutOutcome, PutPrecondition, StorageClient, StorageResult};
use crate::StorageClientError;

/// Publish action kind (planning surface for tests and recovery docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishAction {
    PutTemp(String),
    CopyTempToFinal { temp: String, final_path: String },
    ValidateFinal(String),
    DeleteTemp(String),
    PutManifest(String),
}

/// Returns a backend-safe publish sequence without assuming atomic rename.
#[must_use]
pub fn backend_safe_publish_actions(
    temp_path: &str,
    final_path: &str,
    manifest_path: &str,
) -> Vec<PublishAction> {
    vec![
        PublishAction::PutTemp(temp_path.to_string()),
        PublishAction::CopyTempToFinal {
            temp: temp_path.to_string(),
            final_path: final_path.to_string(),
        },
        PublishAction::ValidateFinal(final_path.to_string()),
        PublishAction::DeleteTemp(temp_path.to_string()),
        PublishAction::PutManifest(manifest_path.to_string()),
    ]
}

/// Builds a unique temp object key under `{prefix}/.tmp/{writer_id}/`.
///
/// Temp keys intentionally avoid the `object_store` reserved `/#\d+` staging
/// suffix pattern used by [`LocalFileSystem`](object_store::local::LocalFileSystem).
#[must_use]
pub fn temp_object_key(prefix: &str, writer_id: &str, file_name: &str) -> String {
    let prefix = prefix.trim_matches('/');
    let writer_id = writer_id.trim_matches('/');
    let file_name = file_name.trim_matches('/');
    if prefix.is_empty() {
        format!(".tmp/{writer_id}/{file_name}")
    } else {
        format!("{prefix}/.tmp/{writer_id}/{file_name}")
    }
}

/// Builds a unique temp file name for one publish attempt.
#[must_use]
pub fn unique_temp_file_name(stem: &str) -> String {
    format!("{stem}.{}.tmp", Uuid::new_v4())
}

/// Outcome of publishing one immutable cold segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedObject {
    /// Final object key.
    pub final_key: String,
    /// Temp key used during publish (deleted on success).
    pub temp_key: String,
    /// Byte size of the published object.
    pub byte_size: u64,
    /// Optional etag from the final put/head.
    pub etag: Option<String>,
    /// True when an existing final object was reused (idempotent retry).
    pub reused_existing: bool,
}

/// Publishes immutable object bytes to `final_key` via temp → create → validate.
///
/// Protocol:
/// 1. Atomic put to a unique temp key (overwrite OK on temp).
/// 2. If final already exists with **byte-identical** content, treat as success.
/// 3. Otherwise `copy_if_absent` (or Create put of the same bytes) to final.
/// 4. Re-read final and require byte identity (size alone is not enough).
/// 5. Best-effort delete temp.
///
/// # Errors
///
/// Returns an error when any durable step fails, or when an existing final
/// object is not byte-identical to the payload (possible corruption / key
/// collision). Size-only matches are rejected.
pub fn publish_immutable_object(
    client: &ObjectStoreClient,
    temp_key: &str,
    final_key: &str,
    bytes: &[u8],
) -> StorageResult<PublishedObject> {
    let expected_size =
        u64::try_from(bytes.len()).map_err(|error| StorageClientError::Backend {
            message: error.to_string(),
        })?;

    // 1. Stage complete bytes under a unique temp key.
    let _temp: PutOutcome = client.put(temp_key, bytes, PutPrecondition::Overwrite)?;
    ensure_object_bytes(client, temp_key, bytes)?;

    // 2. Idempotent retry: byte-identical final already present.
    match try_reuse_identical_final(client, final_key, bytes)? {
        Some(published) => {
            let _ = client.delete(temp_key);
            return Ok(PublishedObject {
                temp_key: temp_key.to_string(),
                ..published
            });
        }
        None => {}
    }

    // 3. Publish final with Create-if-absent semantics.
    match client.copy_if_absent(temp_key, final_key) {
        Ok(()) => {}
        Err(StorageClientError::AlreadyExists { .. }) => {
            let published =
                try_reuse_identical_final(client, final_key, bytes)?.ok_or_else(|| {
                    StorageClientError::Validation {
                        key: final_key.to_string(),
                        message: "final object exists but content does not match payload"
                            .to_string(),
                    }
                })?;
            let _ = client.delete(temp_key);
            return Ok(PublishedObject {
                temp_key: temp_key.to_string(),
                ..published
            });
        }
        Err(error) => {
            // Some backends lack copy_if_not_exists; fall back to Create put.
            if !is_not_implemented(&error) {
                let _ = client.delete(temp_key);
                return Err(error);
            }
            match client.put(final_key, bytes, PutPrecondition::CreateIfAbsent) {
                Ok(_) => {}
                Err(StorageClientError::AlreadyExists { .. }) => {
                    let published = try_reuse_identical_final(client, final_key, bytes)?
                        .ok_or_else(|| StorageClientError::Validation {
                            key: final_key.to_string(),
                            message: "final object exists but content does not match payload"
                                .to_string(),
                        })?;
                    let _ = client.delete(temp_key);
                    return Ok(PublishedObject {
                        temp_key: temp_key.to_string(),
                        ..published
                    });
                }
                Err(put_error) => {
                    let _ = client.delete(temp_key);
                    return Err(put_error);
                }
            }
        }
    }

    // 4. Re-read final and require byte identity before dropping temp.
    let final_meta = ensure_object_bytes(client, final_key, bytes)?;

    // 5. Best-effort temp cleanup (orphans are recoverable).
    let _ = client.delete(temp_key);

    Ok(PublishedObject {
        final_key: final_key.to_string(),
        temp_key: temp_key.to_string(),
        byte_size: expected_size,
        etag: final_meta.etag,
        reused_existing: false,
    })
}

/// Returns a reused publish result when `final_key` already holds `bytes`.
///
/// # Errors
///
/// Returns [`StorageClientError::Validation`] when the object exists but is not
/// byte-identical. Returns `Ok(None)` when the object is absent.
fn try_reuse_identical_final(
    client: &ObjectStoreClient,
    final_key: &str,
    bytes: &[u8],
) -> StorageResult<Option<PublishedObject>> {
    match client.head(final_key) {
        Ok(existing) => {
            let expected_size =
                u64::try_from(bytes.len()).map_err(|error| StorageClientError::Backend {
                    message: error.to_string(),
                })?;
            let actual_size = existing
                .byte_size
                .ok_or_else(|| StorageClientError::Validation {
                    key: final_key.to_string(),
                    message: "existing final object metadata is missing byte_size".to_string(),
                })?;
            if actual_size != expected_size {
                return Err(StorageClientError::Validation {
                    key: final_key.to_string(),
                    message: format!(
                        "final object exists with size {actual_size}, expected {expected_size}"
                    ),
                });
            }
            let actual = client.get(final_key)?;
            if actual.as_slice() != bytes {
                return Err(StorageClientError::Validation {
                    key: final_key.to_string(),
                    message: "final object exists with matching size but different content"
                        .to_string(),
                });
            }
            Ok(Some(PublishedObject {
                final_key: final_key.to_string(),
                temp_key: String::new(),
                byte_size: expected_size,
                etag: existing.etag,
                reused_existing: true,
            }))
        }
        Err(StorageClientError::NotFound { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Ensures an object exists, has the expected size, and matches `bytes` exactly.
fn ensure_object_bytes(
    client: &dyn StorageClient,
    key: &str,
    bytes: &[u8],
) -> StorageResult<StorageObjectMeta> {
    let expected_size =
        u64::try_from(bytes.len()).map_err(|error| StorageClientError::Backend {
            message: error.to_string(),
        })?;
    let meta = validate_object_size(client, key, expected_size)?;
    let actual = client.get(key)?;
    if actual.as_slice() != bytes {
        return Err(StorageClientError::Validation {
            key: key.to_string(),
            message: "object bytes do not match expected payload".to_string(),
        });
    }
    Ok(meta)
}

/// Atomically writes (or replaces) a mutable object such as `manifest.json`.
///
/// Uses overwrite-mode atomic put. Callers that need generation fencing should
/// layer that on top (catalog generation UUID).
///
/// # Errors
///
/// Returns an error when the put or post-write size validation fails.
pub fn publish_mutable_object(
    client: &ObjectStoreClient,
    key: &str,
    bytes: &[u8],
) -> StorageResult<PutOutcome> {
    let expected_size =
        u64::try_from(bytes.len()).map_err(|error| StorageClientError::Backend {
            message: error.to_string(),
        })?;
    let outcome = client.put(key, bytes, PutPrecondition::Overwrite)?;
    validate_object_size(client, key, expected_size)?;
    Ok(outcome)
}

/// Ensures an object exists and reports the expected byte size.
///
/// # Errors
///
/// Returns [`StorageClientError::Validation`] on size mismatch, or NotFound /
/// Backend from head.
pub fn validate_object_size(
    client: &dyn StorageClient,
    key: &str,
    expected_size: u64,
) -> StorageResult<StorageObjectMeta> {
    let object = client.head(key)?;
    let actual = object
        .byte_size
        .ok_or_else(|| StorageClientError::Validation {
            key: key.to_string(),
            message: "object metadata is missing byte_size".to_string(),
        })?;
    if actual != expected_size {
        return Err(StorageClientError::Validation {
            key: key.to_string(),
            message: format!("size mismatch: actual={actual} expected={expected_size}"),
        });
    }
    Ok(StorageObjectMeta {
        key: object.key,
        etag: object.etag,
        byte_size: actual,
    })
}

/// Lightweight object metadata returned by validation helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageObjectMeta {
    pub key: String,
    pub etag: Option<String>,
    pub byte_size: u64,
}

fn is_not_implemented(error: &StorageClientError) -> bool {
    match error {
        StorageClientError::Backend { message } => {
            let lower = message.to_ascii_lowercase();
            // object_store S3 reports copy-if-not-exists as "not supported".
            lower.contains("not implemented")
                || lower.contains("not support")
                || lower.contains("unsupported")
        }
        _ => false,
    }
}
