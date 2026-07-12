//! Active schema refresh planning for managed tables.
//!
//! Owns the migration-only schema refresh **read** planner and registration
//! metadata assembly. Cross-runtime catalog reads stay in `koldstore-catalog`.

use serde::Deserialize;
use uuid::Uuid;

use koldstore_catalog::{allocate_column_id, SchemaColumn};
use koldstore_common::{ColumnId, PrimaryKeyShape, SqlParamType, SqlResult, SqlStatement};
use koldstore_schema::MirrorInitializationState;

use crate::plan::ExistingTableCatalog;
use crate::register::{
    capture_type_matrix, plan_schema_registry_insert_prepared, RegistrationMetadata, RegistryError,
    RegistryResult, SchemaRegistryPlan,
};
use crate::rehydrate::plan_catalog_deactivation;

/// Builds the active managed-schema refresh context lookup.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_active_schema_refresh_context_json() -> SqlResult<SqlStatement> {
    SqlStatement::read_with_params(
        "resolve active schema refresh context",
        r#"
SELECT jsonb_build_object(
    'version', version,
    'table_type', table_type,
    'storage_id', storage_id::text,
    'scope_column', scope_column,
    'mirror_relation', mirror_relation::text,
    'primary_key', primary_key,
    'columns', columns,
    'next_column_id', next_column_id,
    'indexed_columns', indexed_columns,
    'options', options
)::text
FROM koldstore.schemas
WHERE table_oid = $1::oid
  AND active
  AND initialization_state = 'complete'
ORDER BY version DESC
LIMIT 1
"#,
        [SqlParamType::Oid],
    )
}

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
    /// First unallocated stable column id.
    pub next_column_id: ColumnId,
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
    let (columns, next_column_id) = refreshed_schema_columns(active, catalog);
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
        type_matrix: capture_type_matrix(&columns),
        columns,
        next_column_id,
        indexed_columns: catalog.indexed_columns.clone(),
        options: serde_json::from_value(active.options.clone()).unwrap_or_default(),
    }
}

fn refreshed_schema_columns(
    active: &ActiveSchemaRefreshContext,
    catalog: &ExistingTableCatalog,
) -> (Vec<SchemaColumn>, ColumnId) {
    let mut next_column_id = active.next_column_id;
    let columns = catalog
        .columns
        .iter()
        .enumerate()
        .map(|(index, current)| {
            let existing = active.columns.iter().find(|column| {
                column.active
                    && column
                        .attnum
                        .is_some_and(|attnum| attnum == current.attnum && attnum > 0)
            });
            let mut column = if let Some(existing) = existing {
                existing.clone()
            } else {
                let (allocated, new_next) = allocate_column_id(next_column_id);
                next_column_id = new_next;
                let mut created = SchemaColumn::typed(
                    allocated,
                    current.name.clone(),
                    current.pg_type,
                    current.catalog_type_name(),
                    true,
                    false,
                );
                // Freeze the ADD-time default for older cold files; insert default
                // may change later without rewriting history.
                created.initial_default = current.default_expr.clone();
                created.insert_default = current.default_expr.clone();
                created
            };
            column.name.clone_from(&current.name);
            column.pg_type = current.pg_type;
            column
                .catalog_type_name
                .clone_from(&current.catalog_type_name);
            column.active = true;
            column.attnum = (current.attnum > 0).then_some(current.attnum);
            column.ordinal = u32::try_from(index + 1).unwrap_or(u32::MAX);
            column.insert_default = current.default_expr.clone();
            column
        })
        .collect();
    (columns, next_column_id)
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
