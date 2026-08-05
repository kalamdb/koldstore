//! Storage backend configuration and client factory.

mod config;
mod fs;
mod kind;
mod open;
mod s3;

pub use config::{BackendConfig, StorageBackend};
pub use kind::StorageBackendKind;
pub use open::{
    open_client_from_catalog_fields, open_client_from_catalog_fields_with_timeout,
    open_filesystem_client, open_storage_client, open_storage_client_with_timeout,
};
