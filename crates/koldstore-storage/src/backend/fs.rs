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

/// Creates `base_path` when needed and best-effort chmods it for Docker bind mounts.
///
/// Does not write a probe file — [`super::open::ensure_storage_backend_writable`]
/// performs the object-store put/delete probe for every backend kind.
///
/// # Errors
///
/// Returns [`StorageClientError::InvalidPath`] when the directory cannot be created.
pub fn ensure_filesystem_base_prepared(base_path: &str) -> StorageResult<PathBuf> {
    let root = parse_filesystem_root(base_path)?;
    std::fs::create_dir_all(&root).map_err(|error| StorageClientError::InvalidPath {
        message: format!(
            "cannot create filesystem base_path `{base_path}` (resolved {}): {error}. \
             For Docker Desktop / Windows bind mounts, ensure the host folder is writable \
             inside the container (entrypoint chowns /koldstore-data; or use /tmp/koldstore-demo).",
            root.display()
        ),
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Best-effort only — ignored when the process does not own the directory.
        let _ = std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o777));
    }

    Ok(root)
}

#[cfg(test)]
mod tests {
    use super::ensure_filesystem_base_prepared;

    #[test]
    fn ensure_filesystem_base_prepared_creates_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cold");
        let resolved = ensure_filesystem_base_prepared(path.to_str().unwrap()).expect("prepared");
        assert!(resolved.is_dir());
    }
}
