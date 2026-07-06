//! Object-store backend and path-template helpers.
//!
//! Owns backend configuration, publish-safe action planning, object metadata, and
//! the storage client trait. Must not depend on `pgrx`.

pub mod backend;
pub mod client;
pub mod object;
pub mod path_template;
pub mod publish;
pub mod registration;

pub use backend::{BackendConfig, StorageBackend, StorageBackendKind};
pub use client::{StorageClient, StorageClientError, StorageResult};
pub use object::StorageObject;
pub use path_template::PathTemplate;
pub use publish::{backend_safe_publish_actions, ConditionalPut, PublishAction};
pub use registration::{
    alter_storage_credentials_plan, alter_storage_location_plan, register_storage_name_only,
    AlterStorageCredentialsPlan, AlterStorageLocationPlan, DdlError, DdlResult,
    StorageRegistration, StorageRegistrationPlan, DEFAULT_SHARED_PATH_TEMPLATE,
    DEFAULT_USER_PATH_TEMPLATE, SUPPORTED_STORAGE_TYPES,
};
