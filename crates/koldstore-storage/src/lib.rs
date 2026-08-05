//! Object-store backend and path-template helpers.
//!
//! Owns backend configuration, durable `object_store` client construction,
//! publish-safe action planning/execution, object metadata, and the storage
//! client trait. Must not depend on `pgrx`.
//!
//! Cloud backends are opt-in cargo features (`s3`, `gcs`, `azure`; all on by
//! default for this crate). Each uses the matching `object_store` `*-base`
//! feature with rustls (ring crypto provider) — not the full `aws`/`gcp`/
//! `azure` features that pull aws-lc. Dependents that want a filesystem-only
//! build should use `default-features = false` and omit those features.

pub mod backend;
pub mod client;
pub mod object;
pub mod path_template;
pub mod publish;
pub mod registration;
pub mod runtime;

pub use backend::{
    open_client_from_catalog_fields, open_client_from_catalog_fields_with_timeout,
    open_filesystem_client, open_storage_client, open_storage_client_with_timeout, BackendConfig,
    StorageBackendKind,
};
pub use client::{
    ObjectStoreClient, PutOutcome, PutPrecondition, StorageClient, StorageClientError,
    StorageResult,
};
pub use object::StorageObject;
pub use path_template::{
    join_object_key, manifest_object_key, normalize_table_prefix, render_regular_table_prefix,
    PathTemplate,
};
pub use publish::{
    backend_safe_publish_actions, content_checksum_sha256_hex, publish_immutable_object,
    publish_mutable_object, temp_object_key, unique_temp_file_name, validate_object_size,
    PublishAction, PublishedObject, StorageObjectMeta,
};
pub use registration::{
    alter_storage_credentials_plan, alter_storage_location_plan, generate_storage_id,
    AlterStorageCredentialsPlan, AlterStorageLocationPlan, DdlError, DdlResult,
    StorageRegistration, StorageRegistrationPlan, DEFAULT_REGULAR_PATH_TMPL,
    DEFAULT_SCOPED_PATH_TMPL, SUPPORTED_STORAGE_TYPES,
};
pub use runtime::{block_on as block_on_object_store, set_interrupt_hook, Elapsed};

/// Installs the rustls `ring` crypto provider once.
///
/// Required when reqwest is built with `rustls-no-provider` (no aws-lc). Safe
/// to call repeatedly; later calls are no-ops if a provider is already set.
///
/// Call from extension `_PG_init` and before the first cloud HTTPS request.
#[cfg(feature = "cloud-http")]
pub fn ensure_rustls_ring_provider() {
    use std::sync::Once;
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        // Ignore `Err` when another crate already installed a provider.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
