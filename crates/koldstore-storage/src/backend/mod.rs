//! Storage backend configuration and client factory.

mod azure;
mod config;
mod fs;
mod gcs;
mod kind;
mod open;
mod s3;
mod util;

pub use config::BackendConfig;
pub use fs::{ensure_filesystem_base_empty, ensure_filesystem_base_prepared};
pub use kind::StorageBackendKind;
pub use open::{
    ensure_storage_backend_writable, open_client_from_catalog_fields,
    open_client_from_catalog_fields_with_timeout, open_filesystem_client, open_storage_client,
    open_storage_client_with_timeout, STORAGE_WRITE_PROBE_KEY,
};
