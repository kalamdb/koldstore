//! PostgreSQL table management and unmanagement SQL entrypoints.

#[cfg(feature = "pg")]
use koldstore_common::{ManageTableOptions, MigrationStatus};
#[cfg(feature = "pg")]
use koldstore_migrate::rehydrate::DemigrateOptions;
#[cfg(feature = "pg")]
use koldstore_migrate::{introspection, DemigrateTableRequest, MigrateTableRequest};
#[cfg(feature = "pg")]
use uuid::Uuid;

/// Manages a heap table with structured hot/cold flush settings.
///
/// SQL contract:
/// `koldstore.manage_table(table_name, storage, hot_row_limit, min_flush_rows default 1000, max_rows_per_file default 1000, table_type default 'shared', scope_column default null, migration_order_by default null, compression default null, target_file_size_mb default null)`.
#[cfg(feature = "pg")]
#[allow(clippy::too_many_arguments)]
#[pgrx::pg_extern(name = "manage_table", schema = "koldstore", security_definer)]
pub fn manage_table_pg(
    table_name: pgrx::pg_sys::Oid,
    storage: &str,
    hot_row_limit: Option<i64>,
    min_flush_rows: pgrx::default!(i64, 1000),
    max_rows_per_file: pgrx::default!(i64, 1000),
    table_type: pgrx::default!(&str, "'shared'"),
    scope_column: pgrx::default!(Option<&str>, "NULL"),
    migration_order_by: pgrx::default!(Option<&str>, "NULL"),
    compression: pgrx::default!(Option<&str>, "NULL"),
    target_file_size_mb: pgrx::default!(Option<i64>, "NULL"),
) -> pgrx::Uuid {
    manage_table_pg_impl(
        table_name,
        table_type,
        storage,
        scope_column,
        migration_order_by,
        compression,
        target_file_size_mb,
        hot_row_limit,
        min_flush_rows,
        max_rows_per_file,
    )
}

#[cfg(feature = "pg")]
#[allow(clippy::too_many_arguments)]
fn manage_table_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    table_type: &str,
    storage_name: &str,
    scope_column: Option<&str>,
    migration_order_by: Option<&str>,
    compression: Option<&str>,
    target_file_size_mb: Option<i64>,
    hot_row_limit: Option<i64>,
    min_flush_rows: i64,
    max_rows_per_file: i64,
) -> pgrx::Uuid {
    let table_oid_u32 = table_oid.to_u32();
    let table_oid = pgrx::pg_sys::Oid::from(table_oid_u32);
    crate::sql::job_lock_pg::lock_table_job(table_oid)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let storage_id = crate::catalog::resolve::storage_id_by_name(storage_name)
        .unwrap_or_else(|error| pgrx::error!("storage lookup failed: {error}"));
    let catalog = migration_catalog(table_oid_u32)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let registry_catalog = catalog.clone();
    let constraints = manage_table_constraints_catalog(table_oid_u32)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let already_managed = table_is_already_managed(table_oid)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let validation =
        koldstore_migrate::manage_table::validate_manage_table(manage_table_validation_context(
            table_type,
            scope_column,
            storage_id.is_some(),
            already_managed,
            migration_order_by,
            compression,
            target_file_size_mb,
            hot_row_limit,
            min_flush_rows,
            max_rows_per_file,
            &catalog,
            constraints,
        ))
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let options = validation.options;
    let storage_id = storage_id
        .unwrap_or_else(|| unreachable!("validated storage registration must have an id"));
    let primary_key_shape = primary_key_shape(table_oid_u32)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let job_id = Uuid::new_v4();
    let request = MigrateTableRequest {
        table_name: relation,
        table_type: table_type.to_string(),
        storage_name: storage_name.to_string(),
        scope_column: scope_column.map(ToString::to_string),
        options,
    };
    let empty_plan = koldstore_migrate::plan_empty_table_migration(
        &request,
        koldstore_migrate::MigrationTableContext {
            table_oid: table_oid_u32,
            storage_id,
        },
    )
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));

    let has_existing_rows = table_has_rows(&empty_plan.table)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let mirror_plan =
        koldstore_migrate::plan_change_log_mirror(&empty_plan.table, &primary_key_shape)
            .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    if !has_existing_rows {
        for statement in mirror_plan.create_statements() {
            pgrx::Spi::run(&statement.sql)
                .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
        }
        register_schema_version(SchemaRegistrationInput {
            table_oid: table_oid_u32,
            table_type,
            storage_id,
            scope_column: empty_plan.effective_scope_column.as_deref(),
            mirror_relation: &mirror_plan.mirror_table,
            primary_key_shape: &primary_key_shape,
            initialization_state: koldstore_schema::MirrorInitializationState::Complete,
            primary_key: &registry_catalog.primary_key.columns,
            columns: &registry_catalog.columns,
            indexed_columns: &registry_catalog.indexed_columns,
            options: &request.options,
            active: true,
            migration_status: MigrationStatus::Active,
        })
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
        apply_user_scope_policy(
            &empty_plan.table,
            empty_plan.effective_scope_column.as_deref(),
        )
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
        insert_completed_empty_migration_job(
            job_id,
            table_oid_u32,
            table_type,
            storage_id,
            empty_plan.effective_scope_column.as_deref(),
            &empty_plan.table,
        )
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
        refresh_managed_table_row_counters(
            table_oid_u32,
            &empty_plan.table,
            &mirror_plan.mirror_table,
        )
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
        return pgrx::Uuid::from_bytes(*job_id.as_bytes());
    }

    let plan = koldstore_migrate::plan_existing_table_migration(
        &request,
        koldstore_migrate::MigrationTableContext {
            table_oid: table_oid_u32,
            storage_id,
        },
        catalog,
        job_id,
    )
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));

    for statement in mirror_plan.create_statements() {
        pgrx::Spi::run(&statement.sql)
            .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    }
    register_schema_version(SchemaRegistrationInput {
        table_oid: table_oid_u32,
        table_type,
        storage_id,
        scope_column: plan.effective_scope_column.as_deref(),
        mirror_relation: &mirror_plan.mirror_table,
        primary_key_shape: &primary_key_shape,
        initialization_state: koldstore_schema::MirrorInitializationState::Capturing,
        primary_key: &registry_catalog.primary_key.columns,
        columns: &registry_catalog.columns,
        indexed_columns: &registry_catalog.indexed_columns,
        options: &request.options,
        active: false,
        migration_status: MigrationStatus::MirrorInitializing,
    })
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    enqueue_migration_job(&plan)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let processed_rows =
        run_existing_table_mirror_initialization_inline(&plan, &mirror_plan, &primary_key_shape)
            .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    apply_user_scope_policy(&plan.table, plan.effective_scope_column.as_deref())
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    complete_migration_job(job_id, table_oid_u32, processed_rows)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    refresh_managed_table_row_counters(table_oid_u32, &plan.table, &mirror_plan.mirror_table)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));

    pgrx::Uuid::from_bytes(*job_id.as_bytes())
}

#[cfg(feature = "pg")]
fn refresh_managed_table_row_counters(
    table_oid: u32,
    table: &koldstore_common::QualifiedTableName,
    mirror: &koldstore_common::QualifiedTableName,
) -> Result<(), String> {
    crate::sql::flush::counters::refresh_table_row_counters(
        pgrx::pg_sys::Oid::from(table_oid),
        table,
        mirror,
    )
}

#[cfg(feature = "pg")]
fn apply_user_scope_policy(
    table: &koldstore_migrate::QualifiedTableName,
    scope_column: Option<&str>,
) -> Result<(), String> {
    let Some(scope_column) = scope_column else {
        return Ok(());
    };
    let policy = koldstore_migrate::scope::plan_user_scope_policy(table, scope_column)
        .map_err(|error| error.to_string())?;
    for statement in &policy.statements {
        pgrx::Spi::run(&statement.sql).map_err(|error| error.to_string())?;
    }
    Ok(())
}

#[cfg(feature = "pg")]
fn table_has_rows(table: &koldstore_migrate::QualifiedTableName) -> Result<bool, pgrx::spi::Error> {
    pgrx::Spi::get_one::<bool>(&format!(
        "SELECT EXISTS (SELECT 1 FROM ONLY {} LIMIT 1)",
        table.quoted()
    ))
    .map(|value| value.unwrap_or(false))
}

/// Returns the migration catalog, preferring the backend-local cache used by merge scan.
#[cfg(feature = "pg")]
pub(crate) fn migration_catalog(
    table_oid: u32,
) -> Result<koldstore_migrate::ExistingTableCatalog, String> {
    crate::catalog::cache::cached_migration_catalog(pgrx::pg_sys::Oid::from(table_oid))
        .map(|catalog| (*catalog).clone())
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
fn manage_table_constraints_catalog(
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

#[cfg(feature = "pg")]
fn table_is_already_managed(table_oid: pgrx::pg_sys::Oid) -> Result<bool, String> {
    use pgrx::datum::DatumWithOid;

    pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM koldstore.schemas WHERE table_oid = $1::oid)",
        &[DatumWithOid::from(table_oid)],
    )
    .map(|value| value.unwrap_or(false))
    .map_err(|error| error.to_string())
}

#[cfg(feature = "pg")]
#[allow(clippy::too_many_arguments)]
fn manage_table_validation_context<'a>(
    table_type: &str,
    scope_column: Option<&str>,
    storage_exists: bool,
    already_managed: bool,
    migration_order_by: Option<&'a str>,
    compression: Option<&'a str>,
    target_file_size_mb: Option<i64>,
    hot_row_limit: Option<i64>,
    min_flush_rows: i64,
    max_rows_per_file: i64,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    constraints: koldstore_migrate::constraints::ManageTableConstraintsCatalog,
) -> koldstore_migrate::manage_table::ManageTableValidationContext<'a> {
    use koldstore_migrate::constraints::{ColumnDefinition, MigrationValidationInput};

    let columns = catalog
        .columns
        .iter()
        .map(|column| {
            ColumnDefinition::typed(
                column.name.clone(),
                column.pg_type,
                column.catalog_type_name().to_string(),
                true,
                column.generated,
            )
        })
        .collect();
    let min_max_rows_per_file = u64::try_from(crate::guc::min_max_rows_per_file())
        .unwrap_or(koldstore_common::DEFAULT_MIN_MAX_ROWS_PER_FILE);

    koldstore_migrate::manage_table::ManageTableValidationContext {
        migration: MigrationValidationInput {
            table_type: table_type.to_string(),
            scope_column: scope_column.map(str::to_string),
            storage_exists,
            flush_enabled: hot_row_limit.is_some(),
            allow_fk_hot_only: false,
            columns,
            primary_key: catalog.primary_key.columns.clone(),
            expression_primary_key: false,
            indexes: Vec::new(),
            check_constraints: Vec::new(),
            not_null_columns: catalog.primary_key.columns.clone(),
            unique_constraints: constraints.unique_constraints,
            foreign_keys: constraints.foreign_keys,
        },
        already_managed,
        migration_order_by,
        compression,
        policy: koldstore_migrate::manage_table::ManageTablePolicyInput {
            hot_row_limit,
            min_flush_rows,
            max_rows_per_file,
            target_file_size_mb,
            min_max_rows_per_file,
        },
    }
}

#[cfg(feature = "pg")]
fn primary_key_shape(table_oid: u32) -> Result<koldstore_common::PrimaryKeyShape, String> {
    use pgrx::datum::DatumWithOid;

    let probe = koldstore_migrate::register::primary_key_shape_probe_plan(table_oid)
        .map_err(|error| error.to_string())?;
    let json = pgrx::Spi::get_one_with_args::<String>(
        &probe.sql,
        &[DatumWithOid::from(pgrx::pg_sys::Oid::from(table_oid))],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or_else(|| "[]".to_string());

    introspection::decode_primary_key_shape_catalog(&json).map_err(|error| error.to_string())
}

#[cfg(feature = "pg")]
struct SchemaRegistrationInput<'a> {
    table_oid: u32,
    table_type: &'a str,
    storage_id: Uuid,
    scope_column: Option<&'a str>,
    mirror_relation: &'a koldstore_migrate::QualifiedTableName,
    primary_key_shape: &'a koldstore_common::PrimaryKeyShape,
    initialization_state: koldstore_schema::MirrorInitializationState,
    primary_key: &'a [String],
    columns: &'a [koldstore_migrate::order::CatalogColumn],
    indexed_columns: &'a [String],
    options: &'a ManageTableOptions,
    active: bool,
    migration_status: MigrationStatus,
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
            DatumWithOid::from(pgrx::Uuid::from_bytes(*plan.schema_id.as_bytes())),
            DatumWithOid::from(pgrx::pg_sys::Oid::from(prepared.table_oid)),
            DatumWithOid::from(i32::try_from(prepared.version).unwrap_or(i32::MAX)),
            DatumWithOid::from(prepared.active),
            DatumWithOid::from(prepared.table_type.as_str()),
            DatumWithOid::from(pgrx::JsonB(prepared.columns.clone())),
            DatumWithOid::from(i64::try_from(prepared.next_column_id.get()).unwrap_or(i64::MAX)),
            DatumWithOid::from(pgrx::JsonB(prepared.primary_key.clone())),
            DatumWithOid::from(prepared.scope_column.as_deref()),
            DatumWithOid::from(prepared.mirror_relation.as_deref().unwrap_or("")),
            DatumWithOid::from(pgrx::JsonB(prepared.primary_key_shape.clone())),
            DatumWithOid::from(prepared.initialization_state.as_str()),
            DatumWithOid::from(pgrx::JsonB(prepared.indexed_columns.clone())),
            DatumWithOid::from(pgrx::JsonB(prepared.type_matrix.clone())),
            DatumWithOid::from(pgrx::JsonB(prepared.options.clone())),
            DatumWithOid::from(pgrx::Uuid::from_bytes(*prepared.storage_id.as_bytes())),
        ],
    )
    .map_err(|error| error.to_string())
}

#[cfg(feature = "pg")]
fn register_schema_version(input: SchemaRegistrationInput<'_>) -> Result<(), String> {
    use koldstore_migrate::register::{
        plan_schema_registry_insert_with_id, schema_columns_from_catalog, RegistrationMetadata,
    };

    let options = input
        .options
        .clone()
        .with_migration_status(input.migration_status);
    let (columns, next_column_id) = schema_columns_from_catalog(input.columns);
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
        columns,
        next_column_id,
        indexed_columns: input.indexed_columns.to_vec(),
        type_matrix: serde_json::Value::Null,
        options,
    };
    let plan = plan_schema_registry_insert_with_id(&metadata, Uuid::new_v4())
        .map_err(|error| error.to_string())?;
    execute_schema_registry_insert(&plan)?;
    let table_oid = pgrx::pg_sys::Oid::from(input.table_oid);
    crate::catalog::cache::invalidate_table(table_oid);
    crate::spi::invalidate_all_prepared_plans();
    Ok(())
}

#[cfg(feature = "pg")]
pub(crate) fn refresh_active_schema_if_changed(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<bool, String> {
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
            attnum: column.attnum,
            name: column.name.as_str(),
            pg_type: column.pg_type,
            catalog_type_name: column.catalog_type_name(),
        })
        .collect::<Vec<_>>();
    let active_columns = active
        .columns
        .iter()
        .filter(|column| column.active)
        .map(|column| {
            Ok(koldstore_schema::ActiveColumnShape {
                column_id: column.column_id,
                attnum: column.attnum.ok_or_else(|| {
                    format!(
                        "managed schema column `{}` is missing attnum correlation",
                        column.name
                    )
                })?,
                name: column.name.as_str(),
                pg_type: column.pg_type,
                catalog_type_name: column.catalog_type_name(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let action = koldstore_schema::plan_schema_evolution(&koldstore_schema::SchemaEvolutionInput {
        active_primary_key: &active.primary_key,
        active_columns: &active_columns,
        active_indexed_columns: &active.indexed_columns,
        current_primary_key: &catalog.primary_key.columns,
        current_columns: &current_columns,
        current_indexed_columns: &catalog.indexed_columns,
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
    crate::catalog::cache::invalidate_table(table_oid);
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

    let metadata =
        registration_metadata_for_refresh(table_oid_u32, active, catalog, primary_key_shape);
    let refresh = plan_schema_refresh(metadata, active.version, Uuid::new_v4())
        .map_err(|error| error.to_string())?;
    crate::spi::update(&refresh.deactivate, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?;
    execute_schema_registry_insert(&refresh.insert)?;
    Ok(())
}

#[cfg(feature = "pg")]
fn insert_completed_empty_migration_job(
    job_id: Uuid,
    table_oid: u32,
    table_type: &str,
    storage_id: Uuid,
    scope_column: Option<&str>,
    table: &koldstore_migrate::QualifiedTableName,
) -> Result<(), String> {
    use koldstore_migrate::jobs::plan_completed_empty_migration_job;
    use pgrx::datum::DatumWithOid;

    let table_name = table.quoted();
    let statement = plan_completed_empty_migration_job().map_err(|error| error.to_string())?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(pgrx::Uuid::from_bytes(*job_id.as_bytes())),
            DatumWithOid::from(pgrx::pg_sys::Oid::from(table_oid)),
            DatumWithOid::from(table_name.as_str()),
            DatumWithOid::from(table_type),
            DatumWithOid::from(pgrx::Uuid::from_bytes(*storage_id.as_bytes())),
            DatumWithOid::from(scope_column),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(feature = "pg")]
fn enqueue_migration_job(
    plan: &koldstore_migrate::ExistingTableMigrationPlan,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    pgrx::Spi::run_with_args(
        &plan.backfill_job.statement.sql,
        &[
            DatumWithOid::from(pgrx::Uuid::from_bytes(*plan.backfill_job.job_id.as_bytes())),
            DatumWithOid::from(pgrx::pg_sys::Oid::from(plan.backfill_job.table_oid)),
            DatumWithOid::from(pgrx::JsonB(plan.backfill_job.payload.clone())),
        ],
    )
    .map_err(|error| error.to_string())
}

#[cfg(feature = "pg")]
fn complete_migration_job(job_id: Uuid, table_oid: u32, processed_rows: i64) -> Result<(), String> {
    use koldstore_migrate::jobs::plan_complete_migration_backfill_job;
    use pgrx::datum::DatumWithOid;

    let statement = plan_complete_migration_backfill_job().map_err(|error| error.to_string())?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(pgrx::Uuid::from_bytes(*job_id.as_bytes())),
            DatumWithOid::from(pgrx::pg_sys::Oid::from(table_oid)),
            DatumWithOid::from(processed_rows),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(feature = "pg")]
fn run_existing_table_mirror_initialization_inline(
    plan: &koldstore_migrate::ExistingTableMigrationPlan,
    mirror_plan: &koldstore_migrate::ChangeLogMirrorPlan,
    primary_key_shape: &koldstore_common::PrimaryKeyShape,
) -> Result<i64, String> {
    let batch = koldstore_migrate::backfill::plan_mirror_initialization_batch(
        &plan.table,
        &mirror_plan.mirror_table,
        primary_key_shape.columns(),
        plan.ordering.clone(),
        plan.backfill_batch_size,
    )
    .map_err(|error| error.to_string())?;
    let mut processed_rows = 0_i64;
    loop {
        let candidate_rows = crate::spi::execute_prepared(
            &batch.statement,
            &[pgrx::datum::DatumWithOid::from(
                i64::try_from(batch.batch_size.get()).unwrap_or(i64::MAX),
            )],
            crate::spi::first_row::<i64>,
        )
        .map_err(|error| error.to_string())?
        .unwrap_or(0);
        if candidate_rows == 0 {
            break;
        }
        processed_rows = processed_rows.saturating_add(candidate_rows);
    }

    pgrx::Spi::run_with_args(
        &koldstore_migrate::register::plan_activate_managed_schema(plan.table_oid)
            .map_err(|error| error.to_string())?
            .sql,
        &[pgrx::datum::DatumWithOid::from(pgrx::pg_sys::Oid::from(
            plan.table_oid,
        ))],
    )
    .map_err(|error| error.to_string())?;
    crate::catalog::cache::invalidate_table(pgrx::pg_sys::Oid::from(plan.table_oid));
    crate::spi::invalidate_all_prepared_plans();
    Ok(processed_rows)
}

/// Unmanages a managed table through the SQL API.
///
/// SQL contract:
/// `koldstore.unmanage_table(table_name regclass, rehydrate boolean default null, drop_cold boolean default null)`.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "unmanage_table", schema = "koldstore", security_definer)]
pub fn unmanage_table_pg(
    table_name: pgrx::pg_sys::Oid,
    rehydrate: pgrx::default!(Option<bool>, "NULL"),
    drop_cold: pgrx::default!(Option<bool>, "NULL"),
) -> i64 {
    let options = DemigrateTableRequest {
        table_name: String::new(),
        rehydrate,
        drop_cold,
    }
    .options();
    unmanage_table_pg_impl(table_name, options)
        .unwrap_or_else(|error| pgrx::error!("unmanage table failed: {error}"))
}

#[cfg(feature = "pg")]
fn unmanage_table_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    options: DemigrateOptions,
) -> Result<i64, String> {
    use koldstore_migrate::rehydrate::{demigration_context, plan_demigration};

    let table_oid_u32 = table_oid.to_u32();
    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)
        .map_err(|error| error.to_string())?;
    let table = koldstore_migrate::QualifiedTableName::parse(&relation)
        .map_err(|error| error.to_string())?;
    let mirror_table = crate::catalog::resolve::mirror_relation_by_table_oid(table_oid)?;
    let context = demigration_context(table, table_oid_u32, mirror_table);
    let plan = plan_demigration(context, options).map_err(|error| error.to_string())?;

    execute_demigration_locks(&plan)?;
    let deactivated = execute_demigration_statements(&plan, table_oid)?;

    crate::catalog::cache::invalidate_table(table_oid);
    crate::spi::invalidate_all_prepared_plans();

    Ok(deactivated)
}

#[cfg(feature = "pg")]
fn execute_demigration_locks(
    plan: &koldstore_migrate::rehydrate::DemigrationPlan,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    for (index, statement) in plan.lock.statements.iter().enumerate() {
        if index == 0 {
            pgrx::Spi::run_with_args(
                &statement.sql,
                &[DatumWithOid::from(
                    plan.lock.lock_key.as_advisory_lock_key(),
                )],
            )
            .map_err(|error| error.to_string())?;
        } else {
            pgrx::Spi::run(&statement.sql).map_err(|error| error.to_string())?;
        }
    }

    Ok(())
}

#[cfg(feature = "pg")]
fn execute_demigration_statements(
    plan: &koldstore_migrate::rehydrate::DemigrationPlan,
    table_oid: pgrx::pg_sys::Oid,
) -> Result<i64, String> {
    use pgrx::datum::DatumWithOid;

    let statement_count = plan.statements.len();
    let mut deactivated = 0_i64;

    for (index, statement) in plan.statements.iter().enumerate() {
        if index + 2 == statement_count {
            deactivated = pgrx::Spi::get_one_with_args::<i64>(
                &statement.sql,
                &[DatumWithOid::from(table_oid)],
            )
            .map_err(|error| error.to_string())?
            .unwrap_or(0);
        } else if index + 1 == statement_count {
            pgrx::Spi::run_with_args(&statement.sql, &[DatumWithOid::from(table_oid)])
                .map_err(|error| error.to_string())?;
        } else {
            pgrx::Spi::run(&statement.sql).map_err(|error| error.to_string())?;
        }
    }

    Ok(deactivated)
}
