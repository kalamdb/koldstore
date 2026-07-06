//! PostgreSQL-backed catalog resolvers.

use uuid::Uuid;

use koldstore_migrate::QualifiedTableName;

use crate::{
    catalog::{decode, queries},
    spi,
};

/// Resolves a fully qualified relation name by relation OID.
///
/// # Errors
///
/// Returns an error when SPI execution fails or the relation does not exist.
pub fn qualified_relation_name(table_oid: pgrx::pg_sys::Oid) -> Result<String, String> {
    let statement = queries::plan_qualified_relation_by_oid().map_err(|error| error.to_string())?;
    spi::select_one::<String>(&statement, &[pgrx::datum::DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("table oid {} does not exist", table_oid.to_u32()))
}

/// Resolves namespace and relation name by relation OID.
///
/// # Errors
///
/// Returns an error when SPI execution or JSON decoding fails.
pub fn relation_context(table_oid: pgrx::pg_sys::Oid) -> Result<decode::RelationContext, String> {
    let statement = queries::plan_relation_context_by_oid().map_err(|error| error.to_string())?;
    let value = spi::select_json_one(&statement, &[pgrx::datum::DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?;
    decode::relation_context(&value)
}

/// Resolves the active mirror relation for a managed table OID.
///
/// # Errors
///
/// Returns an error when SPI execution fails or the relation cannot be parsed.
pub fn mirror_relation_by_table_oid(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<Option<QualifiedTableName>, String> {
    let statement =
        queries::plan_mirror_relation_by_table_oid().map_err(|error| error.to_string())?;
    let relation =
        spi::select_one::<String>(&statement, &[pgrx::datum::DatumWithOid::from(table_oid)])
            .map_err(|error| error.to_string())?;

    relation
        .map(|relation| QualifiedTableName::parse(&relation).map_err(|error| error.to_string()))
        .transpose()
}

/// Resolves a registered storage ID by name.
///
/// # Errors
///
/// Returns an error when SPI execution fails.
pub fn storage_id_by_name(name: &str) -> Result<Option<Uuid>, String> {
    let statement = queries::plan_storage_id_by_name().map_err(|error| error.to_string())?;
    let id = spi::select_one::<pgrx::Uuid>(&statement, &[pgrx::datum::DatumWithOid::from(name)])
        .map_err(|error| error.to_string())?;
    Ok(id.map(|id| Uuid::from_bytes(*id.as_bytes())))
}

/// Resolves active schema/storage metadata required by flush.
///
/// # Errors
///
/// Returns an error when SPI execution or JSON decoding fails.
pub fn active_flush_storage_context(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<decode::FlushStorageContext, String> {
    let statement =
        queries::plan_active_flush_storage_context().map_err(|error| error.to_string())?;
    let value = spi::select_json_one(&statement, &[pgrx::datum::DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?;
    decode::flush_storage_context(&value)
}
