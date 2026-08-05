//! PostgreSQL table-management flow, validation, and runtime setup.
#[cfg(feature = "pg")]
use super::introspection_spi::{manage_table_constraints_catalog, migration_catalog};
#[cfg(feature = "pg")]
use super::migration_jobs::{
    complete_migration_job, enqueue_migration_job, insert_completed_empty_migration_job,
    mark_migration_job_running, run_existing_table_mirror_initialization_inline,
};
#[cfg(feature = "pg")]
use super::schema_registry::register_schema_version;
#[cfg(feature = "pg")]
use super::schema_registry::SchemaRegistrationInput;
#[cfg(feature = "pg")]
use koldstore_common::{ManageTableOptions, MigrationStatus};
#[cfg(feature = "pg")]
use koldstore_migrate::{introspection, MigrateTableRequest};
#[cfg(feature = "pg")]
use uuid::Uuid;
#[cfg(feature = "pg")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn manage_table_pg_impl(
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
    auto_flush: bool,
    segment_order_column: Option<&str>,
) -> pgrx::Uuid {
    crate::preload::require_shared_preload();
    let min_max_rows_per_file = u64::try_from(crate::guc::min_max_rows_per_file())
        .unwrap_or(koldstore_common::DEFAULT_MIN_MAX_ROWS_PER_FILE);
    koldstore_migrate::manage_table::validate_manage_table_preflight(
        koldstore_migrate::manage_table::ManageTablePolicyInput {
            hot_row_limit,
            min_flush_rows,
            max_rows_per_file,
            target_file_size_mb,
            min_max_rows_per_file,
            auto_flush,
        },
        compression,
    )
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    // Validate logical decoding before taking the transaction-scoped job lock.
    crate::mirror::lifecycle::prepare_capture()
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let table_oid_u32 = table_oid.to_u32();
    let table_oid = pgrx::pg_sys::Oid::from(table_oid_u32);
    crate::sql::job_lock::lock_table_job(table_oid)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let storage_id = crate::catalog::resolve::storage_id_by_name(storage_name)
        .unwrap_or_else(|error| pgrx::error!("storage lookup failed: {error}"));
    let catalog = migration_catalog(table_oid_u32)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
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
            segment_order_column,
            compression,
            target_file_size_mb,
            hot_row_limit,
            min_flush_rows,
            max_rows_per_file,
            auto_flush,
            catalog.as_ref(),
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
        table_name: koldstore_common::TableName::parse(&relation)
            .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}")),
        table_type: table_type.to_string(),
        storage_name: storage_name.to_string(),
        scope_column: scope_column.map(ToString::to_string),
        options,
    };
    let empty_plan = koldstore_migrate::plan_empty_table_migration(
        &request,
        koldstore_migrate::MigrationTableContext {
            table_oid: koldstore_common::TableOid::from_raw(table_oid_u32),
            storage_id: storage_id.clone(),
        },
    )
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));

    let has_existing_rows = table_has_rows(&empty_plan.table)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let order_column_name = request
        .options
        .segment_order_column_id
        .and_then(|column_id| {
            catalog
                .columns
                .iter()
                .find(|column| column.column_id.get() == column_id)
                .map(|column| column.name.as_str())
        });
    let mirror_plan = koldstore_migrate::plan_change_log_mirror_with_order_column(
        &empty_plan.table,
        &primary_key_shape,
        order_column_name,
    )
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    if !has_existing_rows {
        for statement in mirror_plan.create_statements() {
            pgrx::Spi::run(&statement.sql)
                .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
        }
        register_schema_version(SchemaRegistrationInput {
            table_oid: koldstore_common::TableOid::from_raw(table_oid_u32),
            table_type,
            storage_id: storage_id.clone(),
            scope_column: empty_plan.effective_scope_column.as_deref(),
            mirror_relation: &mirror_plan.mirror_table,
            primary_key_shape: &primary_key_shape,
            initialization_state: koldstore_schema::MirrorInitializationState::Complete,
            primary_key: &catalog.primary_key.columns,
            columns: &catalog.columns,
            indexed_columns: &catalog.indexed_columns,
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
            storage_id.clone(),
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
        crate::mirror::lifecycle::activate_table(
            &empty_plan.table,
            &mirror_plan.mirror_table,
            &primary_key_shape,
            order_column_name,
        )
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
        ensure_async_mirror_worker_for_managed_options(&request.options);
        return crate::spi::uuid_to_pgrx(job_id);
    }

    let plan = koldstore_migrate::plan_existing_table_migration(
        &request,
        koldstore_migrate::MigrationTableContext {
            table_oid: koldstore_common::TableOid::from_raw(table_oid_u32),
            storage_id: storage_id.clone(),
        },
        (*catalog).clone(),
        job_id,
    )
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));

    for statement in mirror_plan.create_statements() {
        pgrx::Spi::run(&statement.sql)
            .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    }
    // Hold the apply lock for the whole publish → backfill → catch-up window so
    // the shared applied_lsn cannot advance past this table's undecoded WAL.
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32();
    crate::mirror::lifecycle::lock_apply(database_oid)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    crate::sql::flush::spi::lock_source_table_share_row_exclusive(table_oid)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    crate::mirror::lifecycle::activate_table(
        &plan.table,
        &mirror_plan.mirror_table,
        &primary_key_shape,
        order_column_name,
    )
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let activation_lsn =
        pgrx::Spi::get_one::<String>("SELECT pg_catalog.pg_current_wal_insert_lsn()::text")
            .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"))
            .unwrap_or_else(|| pgrx::error!("migrate table failed: missing activation LSN"));
    register_schema_version(SchemaRegistrationInput {
        table_oid: koldstore_common::TableOid::from_raw(table_oid_u32),
        table_type,
        storage_id,
        scope_column: plan.effective_scope_column.as_deref(),
        mirror_relation: &mirror_plan.mirror_table,
        primary_key_shape: &primary_key_shape,
        initialization_state: koldstore_schema::MirrorInitializationState::Backfilling,
        primary_key: &catalog.primary_key.columns,
        columns: &catalog.columns,
        indexed_columns: &catalog.indexed_columns,
        options: &request.options,
        active: false,
        migration_status: MigrationStatus::MirrorInitializing,
    })
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    pgrx::Spi::run_with_args(
        "UPDATE koldstore.schemas \
         SET activation_lsn = $2::pg_lsn, updated_at = pg_catalog.clock_timestamp() \
         WHERE table_oid = $1::oid AND active = false",
        &[
            pgrx::datum::DatumWithOid::from(table_oid),
            pgrx::datum::DatumWithOid::from(activation_lsn.as_str()),
        ],
    )
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    // Source lock was for the publication ADD only; concurrent DML now hits WAL.
    // Apply lock remains held through backfill + catch-up.
    enqueue_migration_job(&plan)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    mark_migration_job_running(job_id, table_oid_u32, 0)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let processed_rows = run_existing_table_mirror_initialization_inline(
        &plan,
        &mirror_plan,
        &primary_key_shape,
        job_id,
    )
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    pgrx::Spi::run_with_args(
        "UPDATE koldstore.schemas \
         SET initialization_state = 'catching_up', updated_at = pg_catalog.clock_timestamp() \
         WHERE table_oid = $1::oid AND NOT active",
        &[pgrx::datum::DatumWithOid::from(table_oid)],
    )
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let wal_apply_floor = pgrx::Spi::get_one_with_args::<i64>(
        &format!(
            "SELECT COALESCE(MAX(seq), 0) FROM {}",
            mirror_plan.mirror_table.quoted()
        ),
        &[],
    )
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"))
    .unwrap_or(0);
    // Catch up WAL under the apply lock so concurrent commits cannot escape.
    let _ = crate::mirror::apply::apply_bounded(crate::mirror::apply::BoundedApplyRequest {
        upper_bound: None,
        skip_through: None,
        acknowledge_durable_checkpoint: true,
        advance_slot_on_empty: true,
        target_prune_floor: Some((
            table_oid,
            crate::mirror::apply::PruneSeqFloor::new(wal_apply_floor),
        )),
        max_rows: Some(0),
        max_ms: Some(0),
    })
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    pgrx::Spi::run_with_args(
        &koldstore_migrate::register::plan_activate_managed_schema(
            koldstore_common::TableOid::from_raw(table_oid_u32),
        )
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"))
        .sql,
        &[pgrx::datum::DatumWithOid::from(table_oid)],
    )
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    crate::catalog::cache::invalidate_table(table_oid);
    crate::spi::invalidate_all_prepared_plans();
    apply_user_scope_policy(&plan.table, plan.effective_scope_column.as_deref())
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    complete_migration_job(job_id, table_oid_u32, processed_rows)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    refresh_managed_table_row_counters(table_oid_u32, &plan.table, &mirror_plan.mirror_table)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    ensure_async_mirror_worker_for_managed_options(&request.options);

    crate::spi::uuid_to_pgrx(job_id)
}

#[cfg(feature = "pg")]
fn ensure_async_mirror_worker_for_managed_options(options: &ManageTableOptions) {
    if options.auto_flush_enabled() && options.flush_enabled() {
        let _ = crate::worker::ensure_async_mirror_worker();
    }
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
pub(super) fn apply_user_scope_policy(
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
fn table_has_rows(table: &koldstore_migrate::QualifiedTableName) -> Result<bool, String> {
    let statement = koldstore_migrate::register::plan_table_has_rows(table)
        .map_err(|error| error.to_string())?;
    pgrx::Spi::get_one::<bool>(&statement.sql)
        .map(|value| value.unwrap_or(false))
        .map_err(|error| error.to_string())
}
fn table_is_already_managed(table_oid: pgrx::pg_sys::Oid) -> Result<bool, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_catalog::queries::plan_table_already_managed()
        .map_err(|error| error.to_string())?;
    pgrx::Spi::get_one_with_args::<bool>(&statement.sql, &[DatumWithOid::from(table_oid)])
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
    segment_order_column: Option<&'a str>,
    compression: Option<&'a str>,
    target_file_size_mb: Option<i64>,
    hot_row_limit: Option<i64>,
    min_flush_rows: i64,
    max_rows_per_file: i64,
    auto_flush: bool,
    catalog: &'a koldstore_migrate::ExistingTableCatalog,
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
                column.nullable,
                column.generated,
            )
        })
        .collect();
    let min_max_rows_per_file = u64::try_from(crate::guc::min_max_rows_per_file())
        .unwrap_or(koldstore_common::DEFAULT_MIN_MAX_ROWS_PER_FILE);
    let primary_key_names = catalog
        .primary_key
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let scope_column_input = scope_column
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .and_then(|name| catalog.columns.iter().find(|column| column.name == name))
        .map(|column| koldstore_migrate::manage_table::ScopeColumnInput {
            column_id: column.column_id.get(),
        });
    let segment_order_column = segment_order_column
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(|name| {
            let column = catalog
                .columns
                .iter()
                .find(|column| column.name == name)
                .unwrap_or_else(|| {
                    pgrx::error!(
                        "migrate table failed: segment order column `{name}` does not exist"
                    )
                });
            koldstore_migrate::manage_table::SegmentOrderColumnInput {
                column_id: column.column_id.get(),
                name: &column.name,
                type_oid: column.pg_type.type_oid(),
                nullable: column.nullable,
            }
        });

    koldstore_migrate::manage_table::ManageTableValidationContext {
        migration: MigrationValidationInput {
            table_type: table_type.to_string(),
            scope_column: scope_column.map(str::to_string),
            storage_exists,
            flush_enabled: hot_row_limit.is_some(),
            allow_fk_hot_only: false,
            columns,
            primary_key: primary_key_names.clone(),
            expression_primary_key: false,
            indexes: Vec::new(),
            check_constraints: Vec::new(),
            not_null_columns: primary_key_names,
            unique_constraints: constraints.unique_constraints,
            foreign_keys: constraints.foreign_keys,
        },
        already_managed,
        migration_order_by,
        scope_column: scope_column_input,
        segment_order_column,
        compression,
        policy: koldstore_migrate::manage_table::ManageTablePolicyInput {
            hot_row_limit,
            min_flush_rows,
            max_rows_per_file,
            target_file_size_mb,
            min_max_rows_per_file,
            auto_flush,
        },
    }
}

#[cfg(feature = "pg")]
pub(super) fn primary_key_shape(
    table_oid: u32,
) -> Result<koldstore_common::PrimaryKeyShape, String> {
    use pgrx::datum::DatumWithOid;

    let probe = koldstore_migrate::register::primary_key_shape_probe_plan(
        koldstore_common::TableOid::from_raw(table_oid),
    )
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
pub(super) fn set_table_auto_flush_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    enabled: bool,
) -> Result<bool, String> {
    use pgrx::datum::DatumWithOid;

    crate::sql::job_lock::lock_table_job(table_oid)?;
    let statement = koldstore_migrate::register::plan_set_table_auto_flush()
        .map_err(|error| error.to_string())?;
    let updated = pgrx::Spi::get_one_with_args::<bool>(
        &statement.sql,
        &[DatumWithOid::from(table_oid), DatumWithOid::from(enabled)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    if !updated {
        return Err("table is not an active managed table".to_string());
    }
    crate::catalog::cache::invalidate_table_globally(table_oid);
    if enabled {
        let _ = crate::worker::ensure_async_mirror_worker();
    }
    Ok(true)
}
