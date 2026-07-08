//! Flush orchestration SPI adapter for `koldstore.flush_table`.

use koldstore_catalog::decode::{FlushStorageContext, RelationContext};
use koldstore_catalog::ManagedTableSnapshot;
use koldstore_flush::{
    manifest_paths, max_rows_per_file_from_policy, validate_flush_row_selection, FlushStats,
    TableFlushBatchOutcome, TableFlushPreparedContext,
};

use super::jobs::{
    ensure_flush_job, mark_flush_job_completed, mark_flush_job_failed, mark_flush_job_running,
};
use super::segments::{
    manifest_from_active_cold_segments, next_flush_batch_number, prune_flushed_hot_rows,
    upsert_manifest_row, write_flush_segment,
};
use super::stats::{flush_stats_for_rows, resolve_flush_stats};
use super::write::{chunk_flush_write_input, flush_write_input, FlushWriteInput};

pub(super) struct FlushPreparedContext {
    job_id: uuid::Uuid,
    force: bool,
    relation: RelationContext,
    storage: FlushStorageContext,
    snapshot: ManagedTableSnapshot,
    catalog_columns: Vec<koldstore_migrate::order::CatalogColumn>,
    max_rows_per_file: usize,
}

impl FlushPreparedContext {
    fn as_table_flush_context(&self) -> TableFlushPreparedContext {
        TableFlushPreparedContext {
            job_id: self.job_id,
            force: self.force,
            namespace: self.relation.namespace.clone(),
            table_name: self.relation.name.clone(),
            base_path: self.storage.base_path.clone(),
            schema_version: self.storage.schema_version,
            compression: self.storage.compression.clone(),
            primary_key_columns: self.snapshot.primary_key_columns.clone(),
            max_rows_per_file: self.max_rows_per_file,
        }
    }
}

pub(super) fn prepare_flush_context(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<FlushPreparedContext, String> {
    crate::sql::job_lock_pg::lock_table_job(table_oid)?;
    let (job_id, force) = ensure_flush_job(table_oid)?;
    mark_flush_job_running(job_id, table_oid)?;
    let relation = crate::catalog::resolve::relation_context(table_oid)?;
    let storage = crate::catalog::resolve::active_flush_storage_context(table_oid)?;
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let catalog = crate::sql::migrate_pg::migration_catalog(table_oid.to_u32())?;
    let max_rows_per_file = max_rows_per_file_from_policy(
        super::stats::active_flush_policy(table_oid)?.and_then(|policy| policy.max_rows_per_file),
    );
    Ok(FlushPreparedContext {
        job_id,
        force,
        relation,
        storage,
        snapshot,
        catalog_columns: catalog.columns,
        max_rows_per_file,
    })
}

pub(super) fn build_flush_write_input(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
    stats: &FlushStats,
) -> Result<FlushWriteInput, String> {
    crate::merge_scan::pg::with_custom_scan_disabled(|| {
        flush_write_input(
            table_oid,
            ctx.storage.schema_version as u32,
            &ctx.snapshot.primary_key_columns,
            &ctx.catalog_columns,
            stats.max_seq,
        )
    })
}

pub(super) fn write_flush_batches(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
    write_input: &FlushWriteInput,
) -> Result<TableFlushBatchOutcome, String> {
    let table_ctx = ctx.as_table_flush_context();
    let (manifest_path, absolute_manifest_path) = manifest_paths(
        &table_ctx.namespace,
        &table_ctx.table_name,
        &table_ctx.base_path,
    );
    let mut batch_number = next_flush_batch_number(table_oid)?;
    let mut total_rows_flushed = 0_i64;
    let mut last_max_seq = 0_i64;
    let mut last_max_commit_seq = 0_i64;

    for chunk in chunk_flush_write_input(write_input, ctx.max_rows_per_file) {
        let chunk_stats = flush_stats_for_rows(&chunk.rows)?;
        write_flush_segment(
            table_oid,
            &ctx.relation,
            &ctx.storage,
            &ctx.snapshot,
            &write_input.columns,
            &chunk,
            batch_number,
            &chunk_stats,
        )?;
        total_rows_flushed = total_rows_flushed.saturating_add(chunk_stats.row_count);
        last_max_seq = chunk_stats.max_seq;
        last_max_commit_seq = chunk_stats.max_commit_seq;
        batch_number = batch_number.saturating_add(1);
    }
    prune_flushed_hot_rows(
        table_oid,
        &ctx.snapshot.primary_key_columns,
        &write_input.cleanup_rows,
    )?;
    let manifest = manifest_from_active_cold_segments(
        table_oid,
        &ctx.relation,
        &ctx.snapshot,
        ctx.storage.schema_version,
    )?;

    Ok(TableFlushBatchOutcome {
        total_rows_flushed,
        last_max_seq,
        last_max_commit_seq,
        manifest,
        manifest_path,
        absolute_manifest_path,
    })
}

pub(super) fn finalize_flush(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
    outcome: &TableFlushBatchOutcome,
) -> Result<(), String> {
    if let Some(parent) = outcome.absolute_manifest_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    std::fs::write(
        &outcome.absolute_manifest_path,
        serde_json::to_vec_pretty(&outcome.manifest).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    upsert_manifest_row(
        table_oid,
        &outcome.manifest_path,
        outcome.manifest.segments.len() as i32,
        outcome.manifest.max_seq,
        outcome.manifest.max_commit_seq,
    )?;
    mark_flush_job_completed(
        ctx.job_id,
        table_oid,
        outcome.total_rows_flushed,
        outcome.last_max_seq,
        outcome.last_max_commit_seq,
    )?;
    crate::catalog::cache::invalidate_table(table_oid);
    Ok(())
}

pub(super) fn flush_table_pg_impl(table_oid: pgrx::pg_sys::Oid) -> Result<pgrx::Uuid, String> {
    let mut ctx = prepare_flush_context(table_oid)?;
    let job_uuid = pgrx::Uuid::from_bytes(*ctx.job_id.as_bytes());
    match crate::sql::migrate_pg::refresh_active_schema_if_changed(table_oid) {
        Ok(true) => {
            ctx = prepare_flush_context(table_oid).inspect_err(|error| {
                let _ = mark_flush_job_failed(ctx.job_id, table_oid, error);
            })?;
        }
        Ok(false) => {}
        Err(error) => {
            mark_flush_job_failed(ctx.job_id, table_oid, &error)?;
            return Ok(job_uuid);
        }
    }
    let result = flush_prepared_table(table_oid, &ctx);
    match result {
        Ok(()) => Ok(job_uuid),
        Err(error) => {
            mark_flush_job_failed(ctx.job_id, table_oid, &error)?;
            Ok(job_uuid)
        }
    }
}

fn flush_prepared_table(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
) -> Result<(), String> {
    let stats = resolve_flush_stats(table_oid, ctx.force)?;
    if stats.row_count == 0 {
        mark_flush_job_completed(ctx.job_id, table_oid, 0, 0, 0)?;
        return Ok(());
    }

    let write_input = build_flush_write_input(table_oid, ctx, &stats)?;
    validate_flush_row_selection(stats.row_count, write_input.rows.len())?;

    let outcome = write_flush_batches(table_oid, ctx, &write_input)?;
    finalize_flush(table_oid, ctx, &outcome)
}
