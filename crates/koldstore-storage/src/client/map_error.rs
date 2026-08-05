//! Maps `object_store::Error` into [`StorageClientError`].

use super::error::StorageClientError;

fn map_backend(error: object_store::Error) -> StorageClientError {
    StorageClientError::Backend {
        message: error.to_string(),
    }
}

pub(super) fn map_object_store_error_for_key(
    key: &str,
    error: object_store::Error,
) -> StorageClientError {
    match error {
        object_store::Error::NotFound { .. } => StorageClientError::NotFound {
            key: key.to_string(),
        },
        other => map_backend(other),
    }
}

pub(super) fn map_put_error(key: &str, error: object_store::Error) -> StorageClientError {
    match error {
        object_store::Error::AlreadyExists { .. } => StorageClientError::AlreadyExists {
            key: key.to_string(),
        },
        other => map_object_store_error_for_key(key, other),
    }
}

pub(super) fn map_object_store_error(error: object_store::Error) -> StorageClientError {
    map_backend(error)
}
