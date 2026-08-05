//! Azure Blob client construction via `object_store::azure::MicrosoftAzureBuilder`.

use std::time::Duration;

use crate::client::{ObjectStoreClient, StorageResult};

/// Opens an Azure Blob client, applying HTTP timeouts from `timeout`.
#[cfg(feature = "azure")]
pub(super) fn open_azure_client(
    base_path: &str,
    credentials: &serde_json::Value,
    config: &serde_json::Value,
    timeout: Option<Duration>,
) -> StorageResult<ObjectStoreClient> {
    use std::sync::Arc;

    use object_store::azure::MicrosoftAzureBuilder;
    use object_store::ObjectStore;

    use crate::client::StorageClientError;
    use super::util::{client_options, json_bool, json_string, wrap_prefix};

    crate::ensure_rustls_ring_provider();
    let (container, key_prefix, account_from_url) = parse_azure_location(base_path)?;
    let endpoint = json_string(config, "endpoint");
    let use_emulator = json_bool(config, "use_emulator")
        .or_else(|| json_bool(credentials, "use_emulator"))
        .unwrap_or(false);
    let allow_http = use_emulator
        || endpoint
            .as_deref()
            .is_some_and(|value| value.starts_with("http://"));

    let mut builder = MicrosoftAzureBuilder::new()
        .with_container_name(container)
        .with_client_options(client_options(timeout, allow_http))
        .with_use_emulator(use_emulator);

    let account = account_from_url
        .or_else(|| json_string(credentials, "account_name"))
        .or_else(|| json_string(config, "account_name"));
    if let Some(account) = account {
        builder = builder.with_account(account);
    }

    if let Some(endpoint) = endpoint {
        builder = builder.with_endpoint(endpoint);
    }

    builder = apply_azure_credentials(builder, credentials)?;

    let store = builder
        .build()
        .map_err(|error| StorageClientError::Backend {
            message: error.to_string(),
        })?;

    let store: Arc<dyn ObjectStore> = wrap_prefix(Arc::new(store), &key_prefix)?;
    Ok(ObjectStoreClient::from_store(store, None))
}

#[cfg(feature = "azure")]
fn apply_azure_credentials(
    mut builder: object_store::azure::MicrosoftAzureBuilder,
    credentials: &serde_json::Value,
) -> StorageResult<object_store::azure::MicrosoftAzureBuilder> {
    use object_store::azure::AzureConfigKey;

    use crate::client::StorageClientError;
    use super::util::{json_bool, json_string};

    if let Some(key) = json_string(credentials, "access_key")
        .or_else(|| json_string(credentials, "account_key"))
        .or_else(|| json_string(credentials, "azure_storage_account_key"))
    {
        builder = builder.with_access_key(key);
    }

    if let Some(sas) = json_string(credentials, "sas_token")
        .or_else(|| json_string(credentials, "sas_key"))
        .or_else(|| json_string(credentials, "azure_storage_sas_token"))
    {
        builder = builder.with_config(AzureConfigKey::SasKey, sas);
    }

    if let Some(token) = json_string(credentials, "bearer_token")
        .or_else(|| json_string(credentials, "token"))
        .or_else(|| json_string(credentials, "azure_storage_token"))
    {
        builder = builder.with_bearer_token_authorization(token);
    }

    let client_id = json_string(credentials, "client_id")
        .or_else(|| json_string(credentials, "azure_storage_client_id"));
    let client_secret = json_string(credentials, "client_secret")
        .or_else(|| json_string(credentials, "azure_storage_client_secret"));
    let tenant_id = json_string(credentials, "tenant_id")
        .or_else(|| json_string(credentials, "authority_id"))
        .or_else(|| json_string(credentials, "azure_storage_tenant_id"));
    match (client_id, client_secret, tenant_id) {
        (Some(client_id), Some(client_secret), Some(tenant_id)) => {
            builder =
                builder.with_client_secret_authorization(client_id, client_secret, tenant_id);
        }
        (None, None, None) => {}
        _ => {
            return Err(StorageClientError::Backend {
                message: "azure client-secret credentials require client_id, client_secret, and tenant_id"
                    .to_string(),
            });
        }
    }

    if json_bool(credentials, "use_azure_cli").unwrap_or(false) {
        builder = builder.with_config(AzureConfigKey::UseAzureCli, "true");
    }

    Ok(builder)
}

/// Opens an Azure client when the `azure` feature is disabled.
#[cfg(not(feature = "azure"))]
pub(super) fn open_azure_client(
    _base_path: &str,
    _credentials: &serde_json::Value,
    _config: &serde_json::Value,
    _timeout: Option<Duration>,
) -> StorageResult<ObjectStoreClient> {
    Err(super::util::feature_disabled("azure", "azure"))
}

/// Parses Azure catalog `base_path` into `(container, key_prefix, account_from_url)`.
///
/// Supports:
/// - `azure://container[/prefix]` / `az://…` / `adl://…`
/// - `abfs[s]://container[/prefix]` (fsspec)
/// - `abfs[s]://filesystem@account.dfs.core.windows.net[/prefix]`
#[cfg(any(feature = "azure", test))]
pub(super) fn parse_azure_location(
    base_path: &str,
) -> StorageResult<(String, String, Option<String>)> {
    use crate::client::StorageClientError;

    let lower = base_path.to_ascii_lowercase();
    let scheme_end = base_path
        .find("://")
        .ok_or_else(|| StorageClientError::InvalidPath {
            message: format!("azure base_path missing scheme: {base_path}"),
        })?;
    let scheme = &lower[..scheme_end];
    if !matches!(scheme, "azure" | "az" | "adl" | "abfs" | "abfss") {
        return Err(StorageClientError::InvalidPath {
            message: format!(
                "azure base_path must start with azure://, az://, adl://, or abfs[s]://: {base_path}"
            ),
        });
    }

    let rest = &base_path[scheme_end + 3..];
    if let Some((user, host_and_path)) = rest.split_once('@') {
        // abfs://filesystem@account.dfs.core.windows.net/prefix
        let container = user.trim().to_string();
        if container.is_empty() || container.contains('/') {
            return Err(StorageClientError::InvalidPath {
                message: format!("azure base_path missing container: {base_path}"),
            });
        }
        let (host, prefix) = match host_and_path.split_once('/') {
            Some((host, prefix)) => (host, prefix.trim_matches('/').to_string()),
            None => (host_and_path, String::new()),
        };
        let account = host
            .split_once('.')
            .map(|(account, _)| account.to_string())
            .filter(|account| !account.is_empty())
            .ok_or_else(|| StorageClientError::InvalidPath {
                message: format!("azure base_path missing account host: {base_path}"),
            })?;
        return Ok((container, prefix, Some(account)));
    }

    let scheme_prefix = format!("{scheme}://");
    let (container, prefix) =
        super::util::parse_authority_base_path(base_path, &scheme_prefix, "container")?;
    Ok((container, prefix, None))
}

#[cfg(test)]
mod tests {
    use super::parse_azure_location;

    #[test]
    fn parses_azure_container_and_prefix() {
        assert_eq!(
            parse_azure_location("azure://koldstore-test").unwrap(),
            ("koldstore-test".to_string(), String::new(), None)
        );
        assert_eq!(
            parse_azure_location("az://koldstore-test/prod/").unwrap(),
            ("koldstore-test".to_string(), "prod".to_string(), None)
        );
    }

    #[test]
    fn parses_abfs_account_host_form() {
        assert_eq!(
            parse_azure_location("abfs://data@myaccount.dfs.core.windows.net/prod/cold/").unwrap(),
            (
                "data".to_string(),
                "prod/cold".to_string(),
                Some("myaccount".to_string())
            )
        );
    }

    /// Builder smoke test: Azurite-style emulator config must construct.
    #[cfg(feature = "azure")]
    #[test]
    fn azurite_emulator_client_builds() {
        use crate::client::StorageClient;

        let client = super::open_azure_client(
            "azure://koldstore-test/probe/",
            &serde_json::json!({
                "account_name": "devstoreaccount1",
                "access_key": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
                "use_emulator": true,
            }),
            &serde_json::json!({
                "endpoint": "http://127.0.0.1:1/devstoreaccount1",
                "use_emulator": true,
            }),
            None,
        )
        .expect("Azure emulator client must build");

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
