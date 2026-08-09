//! PostgreSQL-backed catalog resolvers.

use koldstore_common::StorageId;
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

/// Returns whether another active managed table references `mirror_relation`.
///
/// # Errors
///
/// Returns an error when PostgreSQL cannot inspect the managed-schema catalog.
pub fn mirror_has_other_active_owner(
    table_oid: pgrx::pg_sys::Oid,
    mirror_relation: &QualifiedTableName,
) -> Result<bool, String> {
    use pgrx::datum::DatumWithOid;

    let mirror_relation = mirror_relation.quoted();
    pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (\
           SELECT 1 \
           FROM koldstore.schemas \
           WHERE active \
             AND table_oid <> $1::oid \
             AND mirror_relation = pg_catalog.to_regclass($2)\
         )",
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(mirror_relation.as_str()),
        ],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "mirror ownership query returned no result".to_string())
}

/// Resolves a registered storage ID by name.
///
/// # Errors
///
/// Returns an error when SPI execution fails.
pub fn storage_id_by_name(name: &str) -> Result<Option<StorageId>, String> {
    let statement = queries::plan_storage_id_by_name().map_err(|error| error.to_string())?;
    let id = spi::select_one::<String>(&statement, &[pgrx::datum::DatumWithOid::from(name)])
        .map_err(|error| error.to_string())?;
    id.map(|value| StorageId::new(value).map_err(|error| error.to_string()))
        .transpose()
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
