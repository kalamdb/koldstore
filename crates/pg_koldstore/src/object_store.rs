//! Object-store open helpers that apply session GUCs and interrupt hooks.

use koldstore_storage::{
    open_client_from_catalog_fields_with_timeout, ObjectStoreClient, StorageResult,
};

/// Opens a catalog-configured client with [`crate::guc::object_store_timeout`].
///
/// # Errors
///
/// Returns storage client construction errors.
pub fn open_managed_object_store_client(
    storage_type: &str,
    base_path: &str,
    credentials: &serde_json::Value,
    config: &serde_json::Value,
) -> StorageResult<ObjectStoreClient> {
    open_client_from_catalog_fields_with_timeout(
        storage_type,
        base_path,
        credentials,
        config,
        crate::guc::object_store_timeout(),
    )
}

/// Registers Postgres interrupt checking for ObjectStore waits.
///
/// Call once from `_PG_init` so query cancel drops in-flight HTTP/futures.
#[cfg(feature = "pg")]
pub fn install_interrupt_hook() {
    koldstore_storage::set_interrupt_hook(Some(check_object_store_interrupts));
}

#[cfg(feature = "pg")]
fn check_object_store_interrupts() {
    pgrx::check_for_interrupts!();
}
