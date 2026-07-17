//! Object-store backend and path-template helpers.
//!
//! Owns backend configuration, durable `object_store` client construction,
//! publish-safe action planning/execution, object metadata, and the storage
//! client trait. Must not depend on `pgrx`.
//!
//! The `s3` feature (on by default for this crate) enables S3/MinIO via
//! `object_store` `aws-base` with rustls (ring crypto provider) and `ring`
//! for SigV4. Dependents that want a filesystem-only build should use
//! `default-features = false` and omit `s3`.

pub mod backend;
pub mod client;
pub mod object;
pub mod path_template;
pub mod publish;
pub mod registration;

pub use backend::{
    open_client_from_catalog_fields, open_filesystem_client, open_storage_client, BackendConfig,
    StorageBackend, StorageBackendKind,
};
pub use client::{
    ObjectStoreClient, PutOutcome, PutPrecondition, StorageClient, StorageClientError,
    StorageResult,
};
pub use object::StorageObject;
pub use path_template::PathTemplate;
pub use publish::{
    backend_safe_publish_actions, content_checksum_sha256_hex, publish_immutable_object,
    publish_mutable_object, temp_object_key, unique_temp_file_name, validate_object_size,
    PublishAction, PublishedObject, StorageObjectMeta,
};
pub use registration::{
    alter_storage_credentials_plan, alter_storage_location_plan, AlterStorageCredentialsPlan,
    AlterStorageLocationPlan, DdlError, DdlResult, StorageRegistration, StorageRegistrationPlan,
    DEFAULT_SHARED_PATH_TEMPLATE, DEFAULT_USER_PATH_TEMPLATE, SUPPORTED_STORAGE_TYPES,
};

/// Installs the rustls `ring` crypto provider once.
///
/// Required when reqwest is built with `rustls-no-provider` (no aws-lc). Safe
/// to call repeatedly; later calls are no-ops if a provider is already set.
///
/// Call from extension `_PG_init` and before the first S3 HTTPS request.
#[cfg(feature = "s3")]
pub fn ensure_rustls_ring_provider() {
    use std::sync::Once;
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        // Ignore `Err` when another crate already installed a provider.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
