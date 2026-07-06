//! PostgreSQL migration and demigration SQL entrypoints.

#[cfg(feature = "pg")]
use koldstore_migrate::rehydrate::DemigrateOptions;
#[cfg(feature = "pg")]
use koldstore_migrate::{introspection, DemigrateTableRequest, MigrateTableRequest};
#[cfg(feature = "pg")]
use uuid::Uuid;

/// Migrates a heap table into pg-koldstore management from SQL.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "migrate_table", schema = "koldstore", security_definer)]
pub fn migrate_table_pg(
    table_name: pgrx::pg_sys::Oid,
    table_type: &str,
    storage_name: &str,
    flush_policy: Option<&str>,
    scope_column: Option<&str>,
) -> pgrx::composite_type!('static, "koldstore.managed_table_info") {
    migrate_table_pg_impl(
        table_name,
        table_type,
        storage_name,
        flush_policy,
        scope_column,
        None,
        None,
    )
}

/// Migrates a heap table and supplies an explicit oldest-to-newest order column.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "migrate_table", schema = "koldstore", security_definer)]
pub fn migrate_table_pg_with_order(
    table_name: pgrx::pg_sys::Oid,
    table_type: &str,
    storage_name: &str,
    flush_policy: Option<&str>,
    scope_column: Option<&str>,
    order_column: Option<&str>,
) -> pgrx::composite_type!('static, "koldstore.managed_table_info") {
    migrate_table_pg_impl(
        table_name,
        table_type,
        storage_name,
        flush_policy,
        scope_column,
        order_column,
        None,
    )
}

/// Migrates a heap table and supplies explicit oldest-to-newest order and compression options.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "migrate_table", schema = "koldstore", security_definer)]
pub fn migrate_table_pg_with_order_and_compression(
    table_name: pgrx::pg_sys::Oid,
    table_type: &str,
    storage_name: &str,
    flush_policy: Option<&str>,
    scope_column: Option<&str>,
    order_column: Option<&str>,
    compression: Option<&str>,
) -> pgrx::composite_type!('static, "koldstore.managed_table_info") {
    migrate_table_pg_impl(
        table_name,
        table_type,
        storage_name,
        flush_policy,
        scope_column,
        order_column,
        compression,
    )
}

#[cfg(feature = "pg")]
#[allow(clippy::too_many_arguments)]
fn migrate_table_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    table_type: &str,
    storage_name: &str,
    flush_policy: Option<&str>,
    scope_column: Option<&str>,
    order_column: Option<&str>,
    compression: Option<&str>,
) -> pgrx::composite_type!('static, "koldstore.managed_table_info") {
    let table_oid_u32 = table_oid.to_u32();
    let table_oid = pgrx::pg_sys::Oid::from(table_oid_u32);
    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let storage_id = crate::catalog::resolve::storage_id_by_name(storage_name)
        .unwrap_or_else(|error| pgrx::error!("storage lookup failed: {error}"))
        .unwrap_or_else(|| pgrx::error!("storage `{storage_name}` is not registered"));
    let catalog = migration_catalog(table_oid_u32)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let registry_catalog = catalog.clone();
    let primary_key_shape = primary_key_shape(table_oid_u32)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let options = migration_options(order_column, compression);
    let request = MigrateTableRequest {
        table_name: relation,
        table_type: table_type.to_string(),
        storage_name: storage_name.to_string(),
        flush_policy: flush_policy.map(ToString::to_string),
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
            flush_policy,
            primary_key: &registry_catalog.primary_key.columns,
            columns: &registry_catalog.columns,
            indexed_columns: &registry_catalog.indexed_columns,
            options: &request.options,
            active: true,
            migration_status: "active",
        })
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
        apply_user_scope_policy(
            &empty_plan.table,
            empty_plan.effective_scope_column.as_deref(),
        )
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
        return managed_table_info_tuple(
            table_oid_u32,
            table_type,
            storage_id,
            empty_plan.effective_scope_column.as_deref(),
        );
    }

    let plan = koldstore_migrate::plan_existing_table_migration(
        &request,
        koldstore_migrate::MigrationTableContext {
            table_oid: table_oid_u32,
            storage_id,
        },
        catalog,
        Uuid::new_v4(),
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
        flush_policy,
        primary_key: &registry_catalog.primary_key.columns,
        columns: &registry_catalog.columns,
        indexed_columns: &registry_catalog.indexed_columns,
        options: &request.options,
        active: false,
        migration_status: "mirror_initializing",
    })
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    run_existing_table_mirror_initialization_inline(&plan, &mirror_plan, &primary_key_shape)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    apply_user_scope_policy(&plan.table, plan.effective_scope_column.as_deref())
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));

    managed_table_info_tuple(
        table_oid_u32,
        table_type,
        storage_id,
        plan.effective_scope_column.as_deref(),
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

#[cfg(feature = "pg")]
fn managed_table_info_tuple(
    table_oid_u32: u32,
    table_type: &str,
    storage_id: Uuid,
    scope_column: Option<&str>,
) -> pgrx::composite_type!('static, "koldstore.managed_table_info") {
    let mut tuple =
        pgrx::heap_tuple::PgHeapTuple::new_composite_type("koldstore.managed_table_info")
            .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    tuple
        .set_by_name("table_oid", pgrx::pg_sys::Oid::from(table_oid_u32))
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    tuple
        .set_by_name("table_type", table_type)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    tuple
        .set_by_name("storage_id", pgrx::Uuid::from_bytes(*storage_id.as_bytes()))
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    tuple
        .set_by_name("schema_version", 1_i32)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    tuple
        .set_by_name("scope_column", scope_column)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    tuple
}

#[cfg(feature = "pg")]
pub(crate) fn migration_catalog(
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
    flush_policy: Option<&'a str>,
    primary_key: &'a [String],
    columns: &'a [koldstore_migrate::order::CatalogColumn],
    indexed_columns: &'a [String],
    options: &'a serde_json::Value,
    active: bool,
    migration_status: &'a str,
}

#[cfg(feature = "pg")]
fn register_schema_version(input: SchemaRegistrationInput<'_>) -> Result<(), String> {
    use koldstore_migrate::register::{
        plan_schema_registry_insert_with_id, schema_columns_from_catalog, RegistrationMetadata,
    };
    use pgrx::datum::DatumWithOid;

    let mut options = input.options.clone();
    if let serde_json::Value::Object(object) = &mut options {
        object.insert(
            "migration_status".to_string(),
            serde_json::Value::String(input.migration_status.to_string()),
        );
    } else {
        options = serde_json::json!({ "migration_status": input.migration_status });
    }
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
        flush_policy: input
            .flush_policy
            .map(str::trim)
            .filter(|policy| !policy.is_empty())
            .map(str::to_string),
        options,
    };
    let plan = plan_schema_registry_insert_with_id(&metadata, Uuid::new_v4())
        .map_err(|error| error.to_string())?;
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
    .map_err(|error| error.to_string())?;
    let table_oid = pgrx::pg_sys::Oid::from(input.table_oid);
    crate::catalog::cache::invalidate_table(table_oid);
    crate::spi::invalidate_all_prepared_plans();
    Ok(())
}

#[cfg(feature = "pg")]
fn run_existing_table_mirror_initialization_inline(
    plan: &koldstore_migrate::ExistingTableMigrationPlan,
    mirror_plan: &koldstore_migrate::ChangeLogMirrorPlan,
    primary_key_shape: &koldstore_common::PrimaryKeyShape,
) -> Result<(), String> {
    let batch = koldstore_migrate::backfill::plan_mirror_initialization_batch(
        &plan.table,
        &mirror_plan.mirror_table,
        primary_key_shape.columns(),
        plan.ordering.clone(),
        plan.backfill_batch_size,
    )
    .map_err(|error| error.to_string())?;
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
    Ok(())
}

/// Demigrates a managed table through the SQL API.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "demigrate_table", schema = "koldstore", security_definer)]
pub fn demigrate_table_pg(
    table_name: pgrx::pg_sys::Oid,
    rehydrate: Option<bool>,
    drop_cold: Option<bool>,
) -> i64 {
    let options = DemigrateTableRequest {
        table_name: String::new(),
        rehydrate,
        drop_cold,
    }
    .options();
    demigrate_table_pg_impl(table_name, options)
        .unwrap_or_else(|error| pgrx::error!("demigrate table failed: {error}"))
}

#[cfg(feature = "pg")]
fn demigrate_table_pg_impl(
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

#[cfg(feature = "pg")]
fn migration_options(order_column: Option<&str>, compression: Option<&str>) -> serde_json::Value {
    let mut options = serde_json::Map::new();
    if let Some(order_column) = order_column
        .map(str::trim)
        .filter(|column| !column.is_empty())
    {
        options.insert(
            "order_column".to_string(),
            serde_json::Value::String(order_column.to_string()),
        );
    }
    if let Some(compression) = compression.map(str::trim).filter(|codec| !codec.is_empty()) {
        options.insert(
            "compression".to_string(),
            serde_json::Value::String(compression.to_ascii_lowercase()),
        );
    }
    serde_json::Value::Object(options)
}

#[cfg(all(test, feature = "pg"))]
mod tests {
    use super::migration_options;

    #[test]
    fn migration_options_include_ordering_and_compression_when_provided() {
        let options = migration_options(Some("created_at"), Some("zstd"));

        assert_eq!(
            options,
            serde_json::json!({
                "order_column": "created_at",
                "compression": "zstd"
            })
        );
    }

    #[test]
    fn migration_options_skip_blank_values() {
        let options = migration_options(Some(" "), Some(""));

        assert_eq!(options, serde_json::json!({}));
    }
}
