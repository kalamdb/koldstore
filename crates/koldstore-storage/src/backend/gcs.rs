//! GCS client construction via `object_store::gcp::GoogleCloudStorageBuilder`.

use std::time::Duration;

use crate::client::{ObjectStoreClient, StorageResult};

/// Opens a GCS client, applying HTTP timeouts from `timeout`.
#[cfg(feature = "gcs")]
pub(super) fn open_gcs_client(
    base_path: &str,
    credentials: &serde_json::Value,
    config: &serde_json::Value,
    timeout: Option<Duration>,
) -> StorageResult<ObjectStoreClient> {
    use std::sync::Arc;

    use object_store::gcp::GoogleCloudStorageBuilder;
    use object_store::ObjectStore;

    use crate::client::StorageClientError;
    use super::util::{client_options, json_bool, json_string, wrap_prefix};

    crate::ensure_rustls_ring_provider();
    let (bucket, key_prefix) = parse_gs_base_path(base_path)?;
    let endpoint = json_string(config, "endpoint").or_else(|| json_string(config, "base_url"));
    let allow_http = endpoint
        .as_deref()
        .is_some_and(|value| value.starts_with("http://"));
    let skip_signature = json_bool(credentials, "skip_signature")
        .or_else(|| json_bool(config, "skip_signature"))
        .unwrap_or(false);

    let mut builder = GoogleCloudStorageBuilder::new()
        .with_bucket_name(bucket)
        .with_client_options(client_options(timeout, allow_http))
        .with_skip_signature(skip_signature);

    if let Some(endpoint) = endpoint {
        builder = builder.with_base_url(&endpoint);
    }

    builder = apply_gcs_credentials(builder, credentials)?;

    let store = builder
        .build()
        .map_err(|error| StorageClientError::Backend {
            message: error.to_string(),
        })?;

    let store: Arc<dyn ObjectStore> = wrap_prefix(Arc::new(store), &key_prefix)?;
    Ok(ObjectStoreClient::from_store(store, None))
}

#[cfg(feature = "gcs")]
fn apply_gcs_credentials(
    mut builder: object_store::gcp::GoogleCloudStorageBuilder,
    credentials: &serde_json::Value,
) -> StorageResult<object_store::gcp::GoogleCloudStorageBuilder> {
    use super::util::json_string;

    if let Some(path) = json_string(credentials, "service_account_path")
        .or_else(|| json_string(credentials, "service_account_file"))
    {
        builder = builder.with_service_account_path(path);
    } else if let Some(key) = service_account_key_json(credentials) {
        builder = builder.with_service_account_key(key);
    }

    if let Some(path) = json_string(credentials, "application_credentials")
        .or_else(|| json_string(credentials, "application_credentials_path"))
    {
        builder = builder.with_application_credentials(path);
    }

    if let Some(token) =
        json_string(credentials, "bearer_token").or_else(|| json_string(credentials, "token"))
    {
        builder = builder.with_bearer_token(token);
    }

    Ok(builder)
}

/// Accepts `service_account_key` as a JSON string, or a nested `service_account` object.
#[cfg(feature = "gcs")]
fn service_account_key_json(credentials: &serde_json::Value) -> Option<String> {
    use super::util::json_string;

    if let Some(key) = json_string(credentials, "service_account_key") {
        return Some(key);
    }
    match credentials.get("service_account") {
        Some(value) if value.is_object() => Some(value.to_string()),
        Some(serde_json::Value::String(text)) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        _ => None,
    }
}

/// Opens a GCS client when the `gcs` feature is disabled.
#[cfg(not(feature = "gcs"))]
pub(super) fn open_gcs_client(
    _base_path: &str,
    _credentials: &serde_json::Value,
    _config: &serde_json::Value,
    _timeout: Option<Duration>,
) -> StorageResult<ObjectStoreClient> {
    Err(super::util::feature_disabled("gcs", "gcs"))
}

/// Parses `gs://bucket` or `gs://bucket/optional/prefix` into bucket + prefix.
#[cfg(any(feature = "gcs", test))]
pub(super) fn parse_gs_base_path(base_path: &str) -> StorageResult<(String, String)> {
    super::util::parse_authority_base_path(base_path, "gs://", "bucket")
}

#[cfg(test)]
mod tests {
    use super::parse_gs_base_path;

    #[test]
    fn parses_gs_bucket_and_prefix() {
        assert_eq!(
            parse_gs_base_path("gs://koldstore-test").unwrap(),
            ("koldstore-test".to_string(), String::new())
        );
        assert_eq!(
            parse_gs_base_path("gs://koldstore-test/prod/").unwrap(),
            ("koldstore-test".to_string(), "prod".to_string())
        );
    }

    /// Builder smoke test: skip_signature + http endpoint must construct.
    #[cfg(feature = "gcs")]
    #[test]
    fn http_gcs_emulator_endpoint_builds() {
        use crate::client::StorageClient;

        let client = super::open_gcs_client(
            "gs://koldstore-test/probe/",
            &serde_json::json!({ "skip_signature": true }),
            &serde_json::json!({ "endpoint": "http://127.0.0.1:1" }),
            None,
        )
        .expect("GCS client with http endpoint must build");

        let err = client
            .list("")
            .expect_err("nothing should listen on 127.0.0.1:1");
        let message = err.to_string();
        assert!(
            !message.contains("builder error"),
            "allow_http must survive ClientOptions replacement; got: {message}"
        );
    }
}
