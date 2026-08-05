//! Object key parsing shared by ObjectStore client operations.

use object_store::path::Path;

use super::error::{StorageClientError, StorageResult};

pub(super) fn parse_key(key: &str) -> StorageResult<Path> {
    let trimmed = key.trim().trim_start_matches('/');
    if trimmed.is_empty() {
        return Err(StorageClientError::InvalidPath {
            message: "object key must not be empty".to_string(),
        });
    }
    if trimmed.split('/').any(|part| part == "." || part == "..") {
        return Err(StorageClientError::InvalidPath {
            message: format!("object key must not contain '.' or '..' segments: {key}"),
        });
    }
    // object_store LocalFileSystem reserves trailing `/#\d+` staging names.
    if let Some(name) = trimmed.rsplit('/').next() {
        if let Some((_, suffix)) = name.rsplit_once('#') {
            if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
                return Err(StorageClientError::InvalidPath {
                    message: format!(
                        "object key `{key}` uses reserved object_store staging suffix /#\\d+"
                    ),
                });
            }
        }
    }
    Path::parse(trimmed).map_err(|error| StorageClientError::InvalidPath {
        message: error.to_string(),
    })
}

pub(super) fn optional_prefix(prefix: &str) -> StorageResult<Option<Path>> {
    let trimmed = prefix.trim().trim_start_matches('/');
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(parse_key(trimmed)?))
}
