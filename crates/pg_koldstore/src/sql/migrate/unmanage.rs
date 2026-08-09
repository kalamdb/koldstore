//! Demigration and managed-table teardown execution.
#[cfg(feature = "pg")]
use koldstore_migrate::rehydrate::DemigrateOptions;
#[cfg(feature = "pg")]
pub(super) fn unmanage_table_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    options: DemigrateOptions,
) -> Result<i64, String> {
    use koldstore_migrate::rehydrate::{demigration_context, plan_demigration};

    let table_oid_u32 = table_oid.to_u32();
    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let timer = koldstore_common::TimedOp::start(
        koldstore_common::log::component::UNMANAGE,
        format!("table={relation}"),
    );
    let table = koldstore_migrate::QualifiedTableName::parse(&relation)
        .map_err(|error| error.to_string())?;
    let mirror_table = crate::catalog::resolve::mirror_relation_by_table_oid(table_oid)?;
    let context = demigration_context(
        table,
        koldstore_common::TableOid::from_raw(table_oid_u32),
        mirror_table,
    );
    let plan = plan_demigration(context, options).map_err(|error| error.to_string())?;

    execute_demigration_locks(&plan)?;
    let deactivated = execute_demigration_statements(&plan, table_oid)?;

    crate::catalog::cache::invalidate_table_globally(table_oid);
    crate::spi::invalidate_all_prepared_plans();

    timer.finish(format!(
        "unmanaged table={relation} schemas_deactivated={deactivated}"
    ));
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

    // Rehydrate issues `TRUNCATE TABLE ONLY <managed>` before catalog deactivation.
    // The ProcessUtility TRUNCATE guard must allow that internal path only.
    let _allow_truncate = crate::hooks::ddl::AllowManagedTruncateGuard::enter();

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
