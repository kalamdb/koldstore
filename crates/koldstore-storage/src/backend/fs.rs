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

/// Maximum sample entry names included in the non-empty base_path error.
const NONEMPTY_SAMPLE_LIMIT: usize = 5;

/// Fails when a filesystem `base_path` already contains entries.
///
/// Used by registration / location checks (`check => true`) so KoldStore does
/// not silently share a directory that already has cold objects or other data.
/// Callers that intentionally reuse a non-empty root must pass `check => false`.
///
/// # Errors
///
/// Returns [`StorageClientError::InvalidPath`] when the directory cannot be
/// listed or is not empty.
pub fn ensure_filesystem_base_empty(base_path: &str) -> StorageResult<()> {
    let root = parse_filesystem_root(base_path)?;
    let entries = std::fs::read_dir(&root).map_err(|error| StorageClientError::InvalidPath {
        message: format!(
            "cannot list filesystem base_path `{base_path}` (resolved {}): {error}",
            root.display()
        ),
    })?;

    let mut samples = Vec::new();
    let mut total = 0usize;
    for entry in entries {
        let entry = entry.map_err(|error| StorageClientError::InvalidPath {
            message: format!(
                "cannot list filesystem base_path `{base_path}` (resolved {}): {error}",
                root.display()
            ),
        })?;
        total += 1;
        if samples.len() < NONEMPTY_SAMPLE_LIMIT {
            samples.push(entry.file_name().to_string_lossy().into_owned());
        }
    }

    if total == 0 {
        return Ok(());
    }

    let sample = samples.join(", ");
    let more = if total > samples.len() {
        format!(", … ({} total entries)", total)
    } else {
        format!(" ({} total entries)", total)
    };
    Err(StorageClientError::InvalidPath {
        message: format!(
            "filesystem base_path `{base_path}` (resolved {}) is not empty{more}: found [{sample}]. \
             Registering storage against a non-empty directory risks mixing unrelated files with \
             KoldStore cold objects. Choose an empty directory, or pass check => false to \
             register_storage / alter_storage_location if you intentionally reuse this path \
             (writability probe and emptiness check are both skipped when check is false).",
            root.display()
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::{ensure_filesystem_base_empty, ensure_filesystem_base_prepared};

    #[test]
    fn ensure_filesystem_base_prepared_creates_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cold");
        let resolved = ensure_filesystem_base_prepared(path.to_str().unwrap()).expect("prepared");
        assert!(resolved.is_dir());
    }

    #[test]
    fn ensure_filesystem_base_empty_accepts_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cold");
        ensure_filesystem_base_prepared(path.to_str().unwrap()).expect("prepared");
        ensure_filesystem_base_empty(path.to_str().unwrap()).expect("empty");
    }

    #[test]
    fn ensure_filesystem_base_empty_rejects_nonempty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cold");
        ensure_filesystem_base_prepared(path.to_str().unwrap()).expect("prepared");
        std::fs::write(path.join("orphan.parquet"), b"x").expect("write");
        let err = ensure_filesystem_base_empty(path.to_str().unwrap()).expect_err("non-empty");
        let message = err.to_string();
        assert!(
            message.contains("is not empty"),
            "expected emptiness failure, got: {message}"
        );
        assert!(
            message.contains("orphan.parquet"),
            "expected sample entry in message, got: {message}"
        );
        assert!(
            message.contains("check => false"),
            "expected bypass hint, got: {message}"
        );
    }
}
