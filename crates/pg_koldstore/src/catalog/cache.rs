//! Backend-local managed-table snapshot cache with SPI loading.

#[cfg(feature = "pg")]
use koldstore_catalog::{decode_managed_table_snapshot, ManagedTableSnapshot};

#[cfg(feature = "pg")]
use crate::spi::{map_spi_error, select_one, SpiResult};

#[cfg(feature = "pg")]
thread_local! {
    static MANAGED_TABLE_CACHE: std::cell::RefCell<
        std::collections::HashMap<u32, ManagedTableSnapshot>
    > = std::cell::RefCell::new(std::collections::HashMap::new());
}

/// Invalidates one managed-table snapshot in the current backend.
#[cfg(feature = "pg")]
pub fn invalidate_table(table_oid: pgrx::pg_sys::Oid) {
    MANAGED_TABLE_CACHE.with(|cache| {
        cache.borrow_mut().remove(&table_oid.to_u32());
    });
}

/// Invalidates all managed-table snapshots in the current backend.
#[cfg(feature = "pg")]
pub fn invalidate_all() {
    MANAGED_TABLE_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// Loads a managed-table snapshot from cache or catalog.
///
/// # Errors
///
/// Returns an error when SPI execution or snapshot decoding fails.
#[cfg(feature = "pg")]
pub fn managed_table_snapshot(
    table_oid: pgrx::pg_sys::Oid,
) -> SpiResult<Option<ManagedTableSnapshot>> {
    let key = table_oid.to_u32();
    if let Some(snapshot) = MANAGED_TABLE_CACHE.with(|cache| cache.borrow().get(&key).cloned()) {
        return Ok(Some(snapshot));
    }

    let snapshot = load_managed_table_snapshot(table_oid)?;
    if let Some(snapshot) = snapshot.as_ref() {
        MANAGED_TABLE_CACHE.with(|cache| {
            cache.borrow_mut().insert(key, snapshot.clone());
        });
    }
    Ok(snapshot)
}

#[cfg(feature = "pg")]
fn load_managed_table_snapshot(
    table_oid: pgrx::pg_sys::Oid,
) -> SpiResult<Option<ManagedTableSnapshot>> {
    let statement = koldstore_catalog::queries::plan_managed_table_snapshot()?;
    let json = select_one::<String>(&statement, &[pgrx::datum::DatumWithOid::from(table_oid)])?;
    json.map(|json| {
        serde_json::from_str::<serde_json::Value>(&json)
            .map_err(|error| map_spi_error(&statement.operation, &error.to_string()))
            .and_then(|value| {
                decode_managed_table_snapshot(&value)
                    .map_err(|error| map_spi_error(&statement.operation, &error))
            })
    })
    .transpose()
}
