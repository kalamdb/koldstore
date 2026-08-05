//! SPI introspection probes and catalog decoding for table migration.
#[cfg(feature = "pg")]
use koldstore_migrate::introspection;
/// Returns the migration catalog, preferring the backend-local cache used by merge scan.
#[cfg(feature = "pg")]
pub(crate) fn migration_catalog(
    table_oid: u32,
) -> Result<std::sync::Arc<koldstore_migrate::ExistingTableCatalog>, String> {
    crate::catalog::cache::cached_migration_catalog(pgrx::pg_sys::Oid::from(table_oid))
}

/// Loads the migration catalog via SPI introspection (uncached).
#[cfg(feature = "pg")]
pub(crate) fn load_migration_catalog(
    table_oid: u32,
) -> Result<koldstore_migrate::ExistingTableCatalog, String> {
    use pgrx::datum::DatumWithOid;

    let oid = pgrx::pg_sys::Oid::from(table_oid);
    let primary_key_json = pgrx::Spi::get_one_with_args::<String>(
        &introspection::plan_primary_key_columns_probe()
            .map_err(|error| error.to_string())?
            .sql,
        &[DatumWithOid::from(oid)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or_else(|| "[]".to_string());
    let columns_json = pgrx::Spi::get_one_with_args::<String>(
        &introspection::plan_table_columns_probe()
            .map_err(|error| error.to_string())?
            .sql,
        &[DatumWithOid::from(oid)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or_else(|| "[]".to_string());
    let indexed_columns_json = pgrx::Spi::get_one_with_args::<String>(
        &introspection::plan_indexed_columns_probe()
            .map_err(|error| error.to_string())?
            .sql,
        &[DatumWithOid::from(oid)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or_else(|| "[]".to_string());

    introspection::decode_existing_table_catalog(
        &primary_key_json,
        &columns_json,
        &indexed_columns_json,
    )
    .map_err(|error| error.to_string())
}

#[cfg(feature = "pg")]
pub(super) fn manage_table_constraints_catalog(
    table_oid: u32,
) -> Result<koldstore_migrate::constraints::ManageTableConstraintsCatalog, String> {
    use pgrx::datum::DatumWithOid;

    let json = pgrx::Spi::get_one_with_args::<String>(
        &introspection::plan_manage_table_constraints_probe()
            .map_err(|error| error.to_string())?
            .sql,
        &[DatumWithOid::from(pgrx::pg_sys::Oid::from(table_oid))],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or_else(|| "{\"unique_constraints\":[],\"foreign_keys\":[]}".to_string());
    introspection::decode_manage_table_constraints_catalog(&json).map_err(|error| error.to_string())
}
