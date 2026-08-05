//! Shared catalog JSON and ObjectStore open helpers for cloud backends.

use crate::client::StorageClientError;

/// Reads a non-empty trimmed string from a JSON object field.
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
pub(super) fn json_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

/// Reads a bool from a JSON object field (bool or common string forms).
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
pub(super) fn json_bool(value: &serde_json::Value, key: &str) -> Option<bool> {
    match value.get(key)? {
        serde_json::Value::Bool(flag) => Some(*flag),
        serde_json::Value::String(text) => match text.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// Builds [`object_store::ClientOptions`] with optional timeouts and HTTP allow.
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
pub(super) fn client_options(
    timeout: Option<std::time::Duration>,
    allow_http: bool,
) -> object_store::ClientOptions {
    use std::time::Duration;

    use object_store::ClientOptions;

    // `with_client_options` replaces the builder's ClientOptions entirely, so
    // `allow_http` must live on this options object (not a prior builder call).
    let mut options = ClientOptions::new().with_allow_http(allow_http);
    if let Some(timeout) = timeout.filter(|value| !value.is_zero()) {
        let connect = timeout.min(Duration::from_secs(5));
        options = options.with_timeout(timeout).with_connect_timeout(connect);
    } else {
        options = options
            .with_timeout_disabled()
            .with_connect_timeout_disabled();
    }
    options
}

/// Wraps `store` with [`PrefixStore`](object_store::prefix::PrefixStore) when
/// `key_prefix` is non-empty so catalog `base_path` prefixes become the root.
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
pub(super) fn wrap_prefix(
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    key_prefix: &str,
) -> crate::client::StorageResult<std::sync::Arc<dyn object_store::ObjectStore>> {
    use std::sync::Arc;

    use object_store::path::Path as ObjectPath;
    use object_store::prefix::PrefixStore;

    if key_prefix.is_empty() {
        return Ok(store);
    }
    let prefix =
        ObjectPath::parse(key_prefix).map_err(|error| StorageClientError::InvalidPath {
            message: error.to_string(),
        })?;
    Ok(Arc::new(PrefixStore::new(store, prefix)))
}

/// Applies optional key-prefix wrapping and builds an [`ObjectStoreClient`].
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
pub(super) fn finalize_object_store_client<S>(
    store: S,
    key_prefix: &str,
) -> crate::client::StorageResult<crate::client::ObjectStoreClient>
where
    S: object_store::ObjectStore + 'static,
{
    use std::sync::Arc;

    use object_store::ObjectStore;

    use crate::client::ObjectStoreClient;

    let store: Arc<dyn ObjectStore> = wrap_prefix(Arc::new(store), key_prefix)?;
    Ok(ObjectStoreClient::from_store(store, None))
}

/// Parses `scheme://authority[/optional/prefix]` into authority + key prefix.
///
/// Used for `s3://bucket/...`, `gs://bucket/...`, and `azure://container/...`
/// style catalog `base_path` values.
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure", test))]
pub(super) fn parse_authority_base_path(
    base_path: &str,
    scheme_prefix: &str,
    authority_label: &str,
) -> crate::client::StorageResult<(String, String)> {
    let rest =
        base_path
            .strip_prefix(scheme_prefix)
            .ok_or_else(|| StorageClientError::InvalidPath {
                message: format!(
                    "{authority_label} base_path must start with {scheme_prefix}: {base_path}"
                ),
            })?;
    let rest = rest.trim_matches('/');
    if rest.is_empty() {
        return Err(StorageClientError::InvalidPath {
            message: format!("{authority_label} base_path missing {authority_label}: {base_path}"),
        });
    }
    let (authority, prefix) = match rest.split_once('/') {
        Some((authority, prefix)) => (authority.to_string(), prefix.trim_matches('/').to_string()),
        None => (rest.to_string(), String::new()),
    };
    if authority.is_empty() {
        return Err(StorageClientError::InvalidPath {
            message: format!("{authority_label} base_path missing {authority_label}: {base_path}"),
        });
    }
    Ok((authority, prefix))
}

/// Maps a disabled cargo feature to a clear open-time error.
#[cfg(not(all(feature = "s3", feature = "gcs", feature = "azure")))]
pub(super) fn feature_disabled(backend: &str, feature: &str) -> StorageClientError {
    StorageClientError::Backend {
        message: format!(
            "{backend} backend requires the `{feature}` cargo feature (not enabled in this build)"
        ),
    }
}
