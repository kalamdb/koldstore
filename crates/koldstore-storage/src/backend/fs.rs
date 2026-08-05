//! Filesystem URI / absolute-path parsing for local backends.

use std::path::{Path, PathBuf};

use crate::client::{StorageClientError, StorageResult};

/// Parses `file://…` URIs or absolute paths into a filesystem root.
pub(super) fn parse_filesystem_root(base_path: &str) -> StorageResult<PathBuf> {
    let trimmed = base_path.trim();
    if trimmed.is_empty() {
        return Err(StorageClientError::InvalidPath {
            message: "filesystem base_path must not be empty".to_string(),
        });
    }
    if let Some(rest) = trimmed.strip_prefix("file://") {
        let path = rest.strip_prefix("localhost").unwrap_or(rest);
        if path.is_empty() {
            return Err(StorageClientError::InvalidPath {
                message: format!("invalid file URI: {base_path}"),
            });
        }
        return Ok(PathBuf::from(path));
    }
    let path = Path::new(trimmed);
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Err(StorageClientError::InvalidPath {
            message: format!("filesystem base_path must be absolute or file:// URI: {base_path}"),
        })
    }
}
