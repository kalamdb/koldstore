//! Schema-version registration, refresh, and runtime artifact synchronization.
#[cfg(feature = "pg")]
use super::introspection_spi::load_migration_catalog;
#[cfg(feature = "pg")]
use super::manage::{apply_user_scope_policy, primary_key_shape};
#[cfg(feature = "pg")]
use koldstore_common::{ManageTableOptions, MigrationStatus};
#[cfg(feature = "pg")]
use uuid::Uuid;
#[cfg(feature = "pg")]
pub(super) struct SchemaRegistrationInput<'a> {
    pub(super) table_oid: koldstore_common::TableOid,
    pub(super) table_type: &'a str,
    pub(super) storage_id: koldstore_common::StorageId,
    pub(super) scope_column: Option<&'a str>,
    pub(super) mirror_relation: &'a koldstore_migrate::QualifiedTableName,
    pub(super) primary_key_shape: &'a koldstore_common::PrimaryKeyShape,
    pub(super) initialization_state: koldstore_schema::MirrorInitializationState,
    pub(super) primary_key: &'a [koldstore_common::ColumnRef],
    pub(super) columns: &'a [koldstore_migrate::order::CatalogColumn],
    pub(super) indexed_columns: &'a [koldstore_common::ColumnRef],
    pub(super) options: &'a ManageTableOptions,
    pub(super) active: bool,
    pub(super) migration_status: MigrationStatus,
}

#[cfg(feature = "pg")]
fn execute_schema_registry_insert(
    plan: &koldstore_migrate::register::SchemaRegistryPlan,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    let prepared = &plan.metadata;
    pgrx::Spi::run_with_args(
        &plan.statement.sql,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(plan.schema_id)),
            DatumWithOid::from(pgrx::pg_sys::Oid::from(prepared.table_oid.get())),
            DatumWithOid::from(i32::try_from(prepared.version).unwrap_or(i32::MAX)),
            DatumWithOid::from(prepared.active),
            DatumWithOid::from(prepared.table_type.as_str()),
            DatumWithOid::from(pgrx::JsonB(prepared.columns.clone())),
            DatumWithOid::from(pgrx::JsonB(prepared.primary_key.clone())),
            DatumWithOid::from(prepared.scope_column.as_deref()),
            DatumWithOid::from(prepared.mirror_relation.as_deref().unwrap_or("")),
            DatumWithOid::from(pgrx::JsonB(prepared.primary_key_shape.clone())),
            DatumWithOid::from(prepared.initialization_state.as_str()),
            DatumWithOid::from(pgrx::JsonB(prepared.indexed_columns.clone())),
            DatumWithOid::from(pgrx::JsonB(prepared.type_matrix.clone())),
            DatumWithOid::from(pgrx::JsonB(prepared.options.clone())),
            DatumWithOid::from(prepared.storage_id.as_str()),
        ],
    )
    .map_err(|error| error.to_string())
}

#[cfg(feature = "pg")]
pub(super) fn register_schema_version(input: SchemaRegistrationInput<'_>) -> Result<(), String> {
    use koldstore_migrate::register::{
        plan_schema_registry_insert_with_id, schema_columns_from_catalog, RegistrationMetadata,
    };

    let options = input
        .options
        .clone()
        .with_migration_status(input.migration_status);
    let metadata = RegistrationMetadata {
        table_oid: input.table_oid,
        table_type: input.table_type.to_string(),
        storage_id: input.storage_id,
        scope_column: input.scope_column.map(str::to_string),
        mirror_relation: Some(input.mirror_relation.quoted()),
        primary_key_shape: Some(input.primary_key_shape.clone()),
        initialization_state: input.initialization_state,
        active: input.active,
        primary_key: input.primary_key.to_vec(),
        columns: schema_columns_from_catalog(input.columns),
        indexed_columns: input.indexed_columns.to_vec(),
        type_matrix: serde_json::Value::Null,
        options,
    };
    let plan = plan_schema_registry_insert_with_id(&metadata, Uuid::new_v4())
        .map_err(|error| error.to_string())?;
    execute_schema_registry_insert(&plan)?;
    let table_oid = pgrx::pg_sys::Oid::from(input.table_oid.get());
    crate::catalog::cache::invalidate_table(table_oid);
    crate::spi::invalidate_all_prepared_plans();
    Ok(())
}

#[cfg(feature = "pg")]
pub(crate) fn refresh_active_schema_if_changed(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<bool, String> {
    // shared_preload ProcessUtility can reach here before CREATE EXTENSION.
    if !crate::catalog::cache::managed_catalog_ready() {
        return Ok(false);
    }
    let table_oid_u32 = table_oid.to_u32();
    let Some(active) = active_schema_refresh_context(table_oid)? else {
        return Ok(false);
    };
    // Always re-introspect: the merge-scan migration catalog cache can still
    // hold the pre-ALTER shape, which would hide unsupported type additions.
    let catalog = load_migration_catalog(table_oid_u32)?;
    let current_columns = catalog
        .columns
        .iter()
        .map(|column| koldstore_schema::CatalogColumnShape {
            column_id: column.column_id,
            name: column.name.as_str(),
            pg_type: column.pg_type,
            catalog_type_name: column.catalog_type_name(),
        })
        .collect::<Vec<_>>();
    let active_primary_key = active
        .primary_key
        .iter()
        .map(|column| column.column_id)
        .collect::<Vec<_>>();
    let active_indexed_columns = active
        .indexed_columns
        .iter()
        .map(|column| column.column_id)
        .collect::<Vec<_>>();
    let current_primary_key = catalog
        .primary_key
        .columns
        .iter()
        .map(|column| column.column_id)
        .collect::<Vec<_>>();
    let current_indexed_columns = catalog
        .indexed_columns
        .iter()
        .map(|column| column.column_id)
        .collect::<Vec<_>>();
    let action = koldstore_schema::plan_schema_evolution(&koldstore_schema::SchemaEvolutionInput {
        active_primary_key: &active_primary_key,
        active_columns: &active.columns,
        active_indexed_columns: &active_indexed_columns,
        current_primary_key: &current_primary_key,
        current_columns: &current_columns,
        current_indexed_columns: &current_indexed_columns,
    })
    .map_err(|error| error.to_string())?;
    if action == koldstore_schema::SchemaEvolutionAction::Unchanged {
        return Ok(false);
    }

    let primary_key_shape = primary_key_shape(table_oid_u32)?;
    insert_refreshed_schema_version(
        table_oid,
        table_oid_u32,
        &active,
        &catalog,
        &primary_key_shape,
    )?;
    // Make the new schemas row visible to subsequent SPI in this command.
    unsafe {
        pgrx::pg_sys::CommandCounterIncrement();
    }
    sync_runtime_artifacts_after_schema_refresh(table_oid, &active, &catalog, &primary_key_shape)?;
    crate::catalog::cache::invalidate_table_globally(table_oid);
    crate::spi::invalidate_all_prepared_plans();
    Ok(true)
}

#[cfg(feature = "pg")]
fn active_schema_refresh_context(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<Option<koldstore_migrate::ActiveSchemaRefreshContext>, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_migrate::plan_active_schema_refresh_context_json()
        .map_err(|error| error.to_string())?;
    let json = crate::spi::select_one::<String>(&statement, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?;
    json.map(|json| serde_json::from_str(&json).map_err(|error| error.to_string()))
        .transpose()
}

#[cfg(feature = "pg")]
fn insert_refreshed_schema_version(
    table_oid: pgrx::pg_sys::Oid,
    table_oid_u32: u32,
    active: &koldstore_migrate::ActiveSchemaRefreshContext,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    primary_key_shape: &koldstore_common::PrimaryKeyShape,
) -> Result<(), String> {
    use koldstore_migrate::{plan_schema_refresh, registration_metadata_for_refresh};
    use pgrx::datum::DatumWithOid;

    let metadata = registration_metadata_for_refresh(
        koldstore_common::TableOid::from_raw(table_oid_u32),
        active,
        catalog,
        primary_key_shape,
    );
    let refresh = plan_schema_refresh(metadata, active.version, Uuid::new_v4())
        .map_err(|error| error.to_string())?;
    crate::spi::update(&refresh.deactivate, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?;
    execute_schema_registry_insert(&refresh.insert)?;
    Ok(())
}

/// Rebuilds rename-sensitive runtime artifacts after a schema version bump.
///
/// Mirror PK column names, PK/order guards, user-scope RLS, and async
/// publication column lists are name-bound at creation time. After a source
/// rename they must be rewritten from the live catalog so DML and flush keep
/// working without operator intervention.
#[cfg(feature = "pg")]
fn sync_runtime_artifacts_after_schema_refresh(
    table_oid: pgrx::pg_sys::Oid,
    active: &koldstore_migrate::ActiveSchemaRefreshContext,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    primary_key_shape: &koldstore_common::PrimaryKeyShape,
) -> Result<(), String> {
    use koldstore_common::ManageTableOptions;
    use koldstore_migrate::{
        primary_key_renames, resolve_scope_column_name, runtime_artifacts_need_sync,
        QualifiedTableName,
    };
    use koldstore_mirror::MirrorRelation;

    let options: ManageTableOptions =
        serde_json::from_value(active.options.clone()).unwrap_or_default();
    if !runtime_artifacts_need_sync(active, catalog, &options) {
        return Ok(());
    }

    let source_name = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let source = QualifiedTableName::parse(&source_name).map_err(|error| error.to_string())?;
    let mirror =
        QualifiedTableName::parse(&active.mirror_relation).map_err(|error| error.to_string())?;
    let mirror_storage =
        MirrorRelation::new(mirror.as_table_name().map_err(|error| error.to_string())?);

    let renames = primary_key_renames(active, catalog);
    if !renames.is_empty() {
        let statements = koldstore_mirror::plan_mirror_pk_column_renames(&mirror_storage, &renames)
            .map_err(|error| error.to_string())?;
        for statement in statements {
            pgrx::Spi::run(&statement.sql).map_err(|error| error.to_string())?;
        }
        unsafe {
            pgrx::pg_sys::CommandCounterIncrement();
        }
    }

    let order_column_name = options.segment_order_column_id.and_then(|column_id| {
        catalog
            .columns
            .iter()
            .find(|column| column.column_id.get() == column_id)
            .map(|column| column.name.as_str())
    });
    let pk_guard = koldstore_mirror::plan_mirror_pk_guard(
        &source,
        &mirror,
        primary_key_shape.columns(),
        order_column_name,
    )
    .map_err(|error| error.to_string())?;
    // Drop any leftover legacy DML capture triggers from pre-WAL-only installs.
    for statement in koldstore_mirror::plan_mirror_source_teardown(&source, &mirror)
        .map_err(|error| error.to_string())?
    {
        pgrx::Spi::run(&statement.sql).map_err(|error| error.to_string())?;
    }
    for statement in pk_guard.create_statements() {
        pgrx::Spi::run(&statement.sql).map_err(|error| error.to_string())?;
    }

    let scope_column = resolve_scope_column_name(active, catalog, &options);
    apply_user_scope_policy(&source, scope_column.as_deref())?;

    crate::mirror::lifecycle::activate_table(
        &source,
        &mirror,
        primary_key_shape,
        order_column_name,
    )?;
    Ok(())
}
