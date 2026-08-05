//! S3 / MinIO client construction and URI helpers.

use std::time::Duration;

use crate::client::{ObjectStoreClient, StorageClientError, StorageResult};

/// Opens an S3-compatible client, applying HTTP timeouts from `timeout`.
#[cfg(feature = "s3")]
pub(super) fn open_s3_client(
    base_path: &str,
    credentials: &serde_json::Value,
    config: &serde_json::Value,
    timeout: Option<Duration>,
) -> StorageResult<ObjectStoreClient> {
    use std::sync::Arc;

    use object_store::aws::AmazonS3Builder;
    use object_store::client::ClientOptions;
    use object_store::path::Path as ObjectPath;
    use object_store::prefix::PrefixStore;
    use object_store::ObjectStore;

    crate::ensure_rustls_ring_provider();
    let (bucket, key_prefix) = parse_s3_base_path(base_path)?;
    let access_key = json_string(credentials, "access_key_id")
        .or_else(|| json_string(credentials, "access_key"));
    let secret_key = json_string(credentials, "secret_access_key")
        .or_else(|| json_string(credentials, "secret_key"));
    let (access_key, secret_key) = match (access_key, secret_key) {
        (Some(access), Some(secret)) => (access, secret),
        _ => {
            return Err(StorageClientError::Backend {
                message: "s3 credentials require access_key_id and secret_access_key".to_string(),
            })
        }
    };
    let region = json_string(config, "region").unwrap_or_else(|| "us-east-1".to_string());
    let endpoint = json_string(config, "endpoint");
    let path_style = json_bool(config, "path_style").unwrap_or(endpoint.is_some());
    let allow_http = endpoint
        .as_deref()
        .is_some_and(|value| value.starts_with("http://"));

    // `with_client_options` replaces the builder's ClientOptions entirely, so
    // `allow_http` must live on this options object (not a prior builder call).
    let mut client_options = ClientOptions::new().with_allow_http(allow_http);
    if let Some(timeout) = timeout.filter(|value| !value.is_zero()) {
        let connect = timeout.min(Duration::from_secs(5));
        client_options = client_options
            .with_timeout(timeout)
            .with_connect_timeout(connect);
    } else {
        client_options = client_options
            .with_timeout_disabled()
            .with_connect_timeout_disabled();
    }

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_virtual_hosted_style_request(!path_style)
        .with_client_options(client_options);
    if let Some(endpoint) = endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    let store = builder
        .build()
        .map_err(|error| StorageClientError::Backend {
            message: error.to_string(),
        })?;

    let store: Arc<dyn ObjectStore> = if key_prefix.is_empty() {
        Arc::new(store)
    } else {
        let prefix =
            ObjectPath::parse(&key_prefix).map_err(|error| StorageClientError::InvalidPath {
                message: error.to_string(),
            })?;
        Arc::new(PrefixStore::new(store, prefix))
    };
    Ok(ObjectStoreClient::from_store(store, None))
}

/// Opens an S3 client when the `s3` feature is disabled.
#[cfg(not(feature = "s3"))]
pub(super) fn open_s3_client(
    _base_path: &str,
    _credentials: &serde_json::Value,
    _config: &serde_json::Value,
    _timeout: Option<Duration>,
) -> StorageResult<ObjectStoreClient> {
    Err(StorageClientError::Backend {
        message: "s3 backend requires the `s3` cargo feature (not enabled in this build)"
            .to_string(),
    })
}

/// Parses `s3://bucket` or `s3://bucket/optional/prefix` into bucket + prefix.
#[cfg(any(feature = "s3", test))]
pub(super) fn parse_s3_base_path(base_path: &str) -> StorageResult<(String, String)> {
    let rest = base_path
        .strip_prefix("s3://")
        .ok_or_else(|| StorageClientError::InvalidPath {
            message: format!("s3 base_path must start with s3://: {base_path}"),
        })?;
    let rest = rest.trim_matches('/');
    if rest.is_empty() {
        return Err(StorageClientError::InvalidPath {
            message: format!("s3 base_path missing bucket: {base_path}"),
        });
    }
    let (bucket, prefix) = match rest.split_once('/') {
        Some((bucket, prefix)) => (bucket.to_string(), prefix.trim_matches('/').to_string()),
        None => (rest.to_string(), String::new()),
    };
    if bucket.is_empty() {
        return Err(StorageClientError::InvalidPath {
            message: format!("s3 base_path missing bucket: {base_path}"),
        });
    }
    Ok((bucket, prefix))
}

#[cfg(feature = "s3")]
fn json_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

#[cfg(feature = "s3")]
fn json_bool(value: &serde_json::Value, key: &str) -> Option<bool> {
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

#[cfg(test)]
mod tests {
    use super::parse_s3_base_path;

    #[test]
    fn parses_s3_bucket_and_prefix() {
        assert_eq!(
            parse_s3_base_path("s3://koldstore-test").unwrap(),
            ("koldstore-test".to_string(), String::new())
        );
        assert_eq!(
            parse_s3_base_path("s3://koldstore-test/prod/").unwrap(),
            ("koldstore-test".to_string(), "prod".to_string())
        );
    }

    /// Regression: `with_client_options` must not wipe `allow_http` for MinIO.
    #[cfg(feature = "s3")]
    #[test]
    fn http_minio_endpoint_does_not_fail_with_builder_error() {
        use crate::client::StorageClient;

        let client = super::open_s3_client(
            "s3://koldstore-test/probe/",
            &serde_json::json!({
                "access_key_id": "minioadmin",
                "secret_access_key": "minioadmin",
            }),
            &serde_json::json!({
                "endpoint": "http://127.0.0.1:1",
                "region": "us-east-1",
                "path_style": true,
            }),
            None,
        )
        .expect("S3 client with http endpoint must build");

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
