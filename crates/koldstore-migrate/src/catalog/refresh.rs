//! Active schema refresh planning for managed tables.

use serde::Deserialize;
use uuid::Uuid;

use koldstore_common::{PrimaryKeyShape, SqlStatement};
use koldstore_schema::{MirrorInitializationState, SchemaColumn};

use crate::plan::ExistingTableCatalog;
use crate::register::{
    capture_type_matrix, plan_schema_registry_insert_prepared, schema_columns_from_catalog,
    RegistrationMetadata, RegistryError, RegistryResult, SchemaRegistryPlan,
};
use crate::rehydrate::plan_catalog_deactivation;

/// Active schema row loaded before refresh planning.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ActiveSchemaRefreshContext {
    /// Active schema version.
    pub version: i32,
    /// Managed table type.
    pub table_type: String,
    /// Registered storage id.
    pub storage_id: String,
    /// Optional scope column.
    pub scope_column: Option<String>,
    /// Mirror relation oid as text.
    pub mirror_relation: String,
    /// Active primary-key columns.
    pub primary_key: Vec<String>,
    /// Active schema columns.
    pub columns: Vec<SchemaColumn>,
    /// Active indexed columns.
    pub indexed_columns: Vec<String>,
    /// Active schema options.
    pub options: serde_json::Value,
}

/// Planned schema refresh statements.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaRefreshPlan {
    /// Schema registry row id for the refreshed version.
    pub schema_id: Uuid,
    /// Next schema version.
    pub next_version: u32,
    /// Metadata deactivation statement.
    pub deactivate: SqlStatement,
    /// Refreshed schema insert statement.
    pub insert: SchemaRegistryPlan,
}

/// Builds registration metadata for a refreshed schema version.
#[must_use]
pub fn registration_metadata_for_refresh(
    table_oid: u32,
    active: &ActiveSchemaRefreshContext,
    catalog: &ExistingTableCatalog,
    primary_key_shape: &PrimaryKeyShape,
) -> RegistrationMetadata {
    RegistrationMetadata {
        table_oid,
        table_type: active.table_type.clone(),
        storage_id: Uuid::parse_str(&active.storage_id).unwrap_or(Uuid::nil()),
        scope_column: active.scope_column.clone(),
        mirror_relation: Some(active.mirror_relation.clone()),
        primary_key_shape: Some(primary_key_shape.clone()),
        initialization_state: MirrorInitializationState::Complete,
        active: true,
        primary_key: catalog.primary_key.columns.clone(),
        columns: schema_columns_from_catalog(&catalog.columns),
        indexed_columns: catalog.indexed_columns.clone(),
        type_matrix: capture_type_matrix(&schema_columns_from_catalog(&catalog.columns)),
        options: serde_json::from_value(active.options.clone()).unwrap_or_default(),
    }
}

/// Plans deactivation of the active schema row and insertion of the refreshed version.
///
/// # Errors
///
/// Returns an error when metadata is invalid or SQL statement metadata cannot be prepared.
pub fn plan_schema_refresh(
    metadata: RegistrationMetadata,
    active_version: i32,
    schema_id: Uuid,
) -> RegistryResult<SchemaRefreshPlan> {
    let next_version = u32::try_from(
        active_version
            .checked_add(1)
            .ok_or_else(|| RegistryError::Spi("schema version overflow".to_string()))?,
    )
    .map_err(|error| RegistryError::Spi(error.to_string()))?;
    let mut prepared = metadata.prepare()?;
    prepared.version = next_version;
    prepared.active = true;
    let deactivate = plan_catalog_deactivation(metadata.table_oid)
        .map_err(|error| RegistryError::Spi(error.to_string()))?;
    let insert = plan_schema_registry_insert_prepared(schema_id, prepared)?;
    Ok(SchemaRefreshPlan {
        schema_id,
        next_version,
        deactivate,
        insert,
    })
}
