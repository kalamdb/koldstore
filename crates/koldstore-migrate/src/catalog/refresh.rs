//! Active schema refresh planning for managed tables.
//!
//! Owns migration-only registration metadata assembly and refresh statement
//! sequencing. The active-schema context **read** lives in
//! [`koldstore_catalog::queries::plan_active_schema_refresh_context_json`].

use serde::Deserialize;
use uuid::Uuid;

use koldstore_common::{
    ColumnId, ColumnRef, ManageTableOptions, PrimaryKeyShape, SqlStatement, StorageId, TableOid,
};
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
    pub storage_id: StorageId,
    /// Optional scope column.
    pub scope_column: Option<String>,
    /// Mirror relation oid as text.
    pub mirror_relation: String,
    /// Active primary-key columns.
    pub primary_key: Vec<ColumnRef>,
    /// Active schema columns.
    pub columns: Vec<SchemaColumn>,
    /// Active indexed columns.
    pub indexed_columns: Vec<ColumnRef>,
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
///
/// Scope column display name is resolved from the live catalog via
/// `options.scope_column_id` so renames do not leave a stale `schemas.scope_column`.
#[must_use]
pub fn registration_metadata_for_refresh(
    table_oid: TableOid,
    active: &ActiveSchemaRefreshContext,
    catalog: &ExistingTableCatalog,
    primary_key_shape: &PrimaryKeyShape,
) -> RegistrationMetadata {
    let columns = schema_columns_from_catalog(&catalog.columns);
    let options: ManageTableOptions =
        serde_json::from_value(active.options.clone()).unwrap_or_default();
    let scope_column = resolve_scope_column_name(active, catalog, &options);
    RegistrationMetadata {
        table_oid,
        table_type: active.table_type.clone(),
        storage_id: active.storage_id.clone(),
        scope_column,
        mirror_relation: Some(active.mirror_relation.clone()),
        primary_key_shape: Some(primary_key_shape.clone()),
        initialization_state: MirrorInitializationState::Complete,
        active: true,
        primary_key: catalog.primary_key.columns.clone(),
        type_matrix: capture_type_matrix(&columns),
        columns,
        indexed_columns: catalog.indexed_columns.clone(),
        options,
    }
}

/// Resolves the current scope column name for a refreshed schema row.
///
/// Looks up `options.scope_column_id` in the live catalog. Tables without a
/// stored scope column ID have no scope (legacy name-only rows are not accepted).
#[must_use]
pub fn resolve_scope_column_name(
    _active: &ActiveSchemaRefreshContext,
    catalog: &ExistingTableCatalog,
    options: &ManageTableOptions,
) -> Option<String> {
    let column_id = options.scope_column_id?;
    catalog
        .columns
        .iter()
        .find(|column| column.column_id.get() == column_id)
        .map(|column| column.name.clone())
}

/// Primary-key column renames detected between the active schema and live catalog.
///
/// Each entry is `(old_name, new_name)` matched by stable [`ColumnId`].
#[must_use]
pub fn primary_key_renames(
    active: &ActiveSchemaRefreshContext,
    catalog: &ExistingTableCatalog,
) -> Vec<(String, String)> {
    let mut renames = Vec::new();
    for current in &catalog.primary_key.columns {
        let Some(previous) = active
            .primary_key
            .iter()
            .find(|column| column.column_id == current.column_id)
        else {
            continue;
        };
        if previous.name != current.name {
            renames.push((previous.name.clone(), current.name.clone()));
        }
    }
    renames
}

/// True when rename-sensitive runtime artifacts (mirror PK columns, capture SQL,
/// RLS, async publication column lists) must be rebuilt after a schema refresh.
#[must_use]
pub fn runtime_artifacts_need_sync(
    active: &ActiveSchemaRefreshContext,
    catalog: &ExistingTableCatalog,
    options: &ManageTableOptions,
) -> bool {
    if !primary_key_renames(active, catalog).is_empty() {
        return true;
    }
    let new_scope = resolve_scope_column_name(active, catalog, options);
    if active.scope_column != new_scope {
        return true;
    }
    let Some(order_id) = options.segment_order_column_id.map(ColumnId::from_attnum) else {
        return false;
    };
    let old_order = active
        .columns
        .iter()
        .find(|column| column.column_id == order_id)
        .map(|column| column.name.as_str());
    let new_order = catalog
        .columns
        .iter()
        .find(|column| column.column_id == order_id)
        .map(|column| column.name.as_str());
    old_order != new_order
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::{CatalogColumn, CatalogPrimaryKey};
    use koldstore_common::{
        ColumnId, ColumnRef, ManageTableOptions, PgTypeName, PgTypeOid, PgTypmod, PkColumn,
        PkOrdinal, PrimaryKeyColumnShape, StorageId, TableOid,
    };
    use koldstore_schema::SchemaColumn;

    fn active_context() -> ActiveSchemaRefreshContext {
        ActiveSchemaRefreshContext {
            version: 1,
            table_type: "user".to_string(),
            storage_id: StorageId::new("a1b2c3d4").unwrap(),
            scope_column: Some("user_id".to_string()),
            mirror_relation: "koldstore.notes__cl".to_string(),
            primary_key: vec![ColumnRef::new(ColumnId::from_attnum(1), "id")],
            columns: vec![
                SchemaColumn::app(1, "id", "bigint", false),
                SchemaColumn::app(2, "user_id", "text", false),
                SchemaColumn::app(3, "event_time", "timestamptz", false),
            ],
            indexed_columns: vec![],
            options: serde_json::json!({
                "scope_column_id": 2,
                "segment_order_column_id": 3
            }),
        }
    }

    fn catalog_with_renames() -> ExistingTableCatalog {
        ExistingTableCatalog::new(
            CatalogPrimaryKey::single(1, "note_id"),
            vec![
                CatalogColumn::bigint(1, "note_id"),
                CatalogColumn::text(2, "owner_id"),
                CatalogColumn::timestamp(3, "occurred_at"),
            ],
            vec![],
        )
    }

    fn pk_shape(name: &str) -> PrimaryKeyShape {
        PrimaryKeyShape::new(vec![PrimaryKeyColumnShape::new(
            ColumnId::from_attnum(1),
            PkColumn::new(name).unwrap(),
            PkOrdinal::new(1).unwrap(),
            PgTypeOid::new(20).unwrap(),
            PgTypeName::new("bigint").unwrap(),
            PgTypmod::new(-1),
            None,
            None,
            true,
        )])
        .unwrap()
    }

    #[test]
    fn refresh_metadata_resolves_scope_name_from_column_id() {
        let active = active_context();
        let catalog = catalog_with_renames();
        let metadata =
            registration_metadata_for_refresh(
                TableOid::from_raw(42),
                &active,
                &catalog,
                &pk_shape("note_id"),
            );
        assert_eq!(metadata.scope_column.as_deref(), Some("owner_id"));
        assert_eq!(metadata.primary_key[0].name, "note_id");
    }

    #[test]
    fn runtime_sync_detects_pk_scope_and_order_renames() {
        let active = active_context();
        let catalog = catalog_with_renames();
        let options: ManageTableOptions = serde_json::from_value(active.options.clone()).unwrap();
        assert_eq!(
            primary_key_renames(&active, &catalog),
            vec![("id".to_string(), "note_id".to_string())]
        );
        assert!(runtime_artifacts_need_sync(&active, &catalog, &options));
    }
}
