//! Migration backfill job enqueue, progress, and completion helpers.
#[cfg(feature = "pg")]
use uuid::Uuid;
#[cfg(feature = "pg")]
pub(super) fn insert_completed_empty_migration_job(
    job_id: Uuid,
    table_oid: u32,
    table_type: &str,
    storage_id: koldstore_common::StorageId,
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
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(pgrx::pg_sys::Oid::from(table_oid)),
            DatumWithOid::from(table_name.as_str()),
            DatumWithOid::from(table_type),
            DatumWithOid::from(storage_id.as_str()),
            DatumWithOid::from(scope_column),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(feature = "pg")]
pub(super) fn enqueue_migration_job(
    plan: &koldstore_migrate::ExistingTableMigrationPlan,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    pgrx::Spi::run_with_args(
        &plan.backfill_job.statement.sql,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(plan.backfill_job.job_id)),
            DatumWithOid::from(pgrx::pg_sys::Oid::from(plan.backfill_job.table_oid.get())),
            DatumWithOid::from(pgrx::JsonB(plan.backfill_job.payload.clone())),
        ],
    )
    .map_err(|error| error.to_string())
}

#[cfg(feature = "pg")]
pub(super) fn mark_migration_job_running(
    job_id: Uuid,
    table_oid: u32,
    progress_total: i64,
) -> Result<(), String> {
    use koldstore_migrate::jobs::plan_mark_migration_backfill_running;
    use pgrx::datum::DatumWithOid;

    let statement = plan_mark_migration_backfill_running().map_err(|error| error.to_string())?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(pgrx::pg_sys::Oid::from(table_oid)),
            DatumWithOid::from(progress_total),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(feature = "pg")]
pub(super) fn update_migration_job_progress(
    job_id: Uuid,
    table_oid: u32,
    processed_rows: i64,
    progress_total: i64,
    batches_completed: i32,
) -> Result<(), String> {
    use koldstore_migrate::jobs::plan_update_migration_backfill_progress;
    use pgrx::datum::DatumWithOid;

    let statement = plan_update_migration_backfill_progress().map_err(|error| error.to_string())?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(pgrx::pg_sys::Oid::from(table_oid)),
            DatumWithOid::from(processed_rows),
            DatumWithOid::from(progress_total),
            DatumWithOid::from(batches_completed),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(feature = "pg")]
pub(super) fn complete_migration_job(
    job_id: Uuid,
    table_oid: u32,
    processed_rows: i64,
) -> Result<(), String> {
    use koldstore_migrate::jobs::plan_complete_migration_backfill_job;
    use pgrx::datum::DatumWithOid;

    let statement = plan_complete_migration_backfill_job().map_err(|error| error.to_string())?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(crate::spi::uuid_to_pgrx(job_id)),
            DatumWithOid::from(pgrx::pg_sys::Oid::from(table_oid)),
            DatumWithOid::from(processed_rows),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(feature = "pg")]
pub(super) fn run_existing_table_mirror_initialization_inline(
    plan: &koldstore_migrate::ExistingTableMigrationPlan,
    mirror_plan: &koldstore_migrate::ChangeLogMirrorPlan,
    primary_key_shape: &koldstore_common::PrimaryKeyShape,
    segment_order_column: Option<&str>,
    job_id: Uuid,
) -> Result<i64, String> {
    let batch = koldstore_migrate::backfill::plan_mirror_initialization_batch_with_segment_order(
        &plan.table,
        &mirror_plan.mirror_table,
        primary_key_shape.columns(),
        plan.ordering.clone(),
        plan.backfill_batch_size,
        segment_order_column,
    )
    .map_err(|error| error.to_string())?;
    let mut processed_rows = 0_i64;
    let mut batches_completed = 0_i32;
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
        batches_completed = batches_completed.saturating_add(1);
        update_migration_job_progress(
            job_id,
            plan.table_oid.get(),
            processed_rows,
            processed_rows,
            batches_completed,
        )?;
    }

    crate::catalog::cache::invalidate_table(pgrx::pg_sys::Oid::from(plan.table_oid.get()));
    crate::spi::invalidate_all_prepared_plans();
    Ok(processed_rows)
}
