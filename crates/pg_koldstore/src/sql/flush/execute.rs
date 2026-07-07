//! Flush orchestration: prepare, batch write, and finalize.

use koldstore_catalog::ManagedTableSnapshot;
use koldstore_catalog::decode::{FlushStorageContext, RelationContext};

use super::jobs::{ensure_flush_job, mark_flush_job_completed, mark_flush_job_running};
use super::segments::{next_flush_batch_number, upsert_manifest_row, write_flush_segment};
use super::stats::{active_flush_policy, resolve_flush_stats};
use super::write::{chunk_flush_write_input, flush_write_input, FlushWriteInput};

pub(super) struct FlushPreparedContext {
    pub job_id: uuid::Uuid,
    pub force: bool,
    pub relation: RelationContext,
    pub storage: FlushStorageContext,
    pub snapshot: ManagedTableSnapshot,
    pub catalog_columns: Vec<koldstore_migrate::order::CatalogColumn>,
    pub max_rows_per_file: usize,
}

pub(super) struct FlushBatchOutcome {
    pub total_rows_flushed: i64,
    pub last_max_seq: i64,
    pub last_max_commit_seq: i64,
    pub manifest: koldstore_manifest::Manifest,
    pub manifest_path: String,
    pub absolute_manifest_path: std::path::PathBuf,
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
    let max_rows_per_file = active_flush_policy(table_oid)?
        .and_then(|policy| policy.max_rows_per_file)
        .map(|value| usize::try_from(value).unwrap_or(usize::MAX))
        .unwrap_or(usize::MAX);
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
    stats: &super::stats::FlushStats,
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
) -> Result<FlushBatchOutcome, String> {
    let prefix = format!("{}/{}", ctx.relation.namespace, ctx.relation.name);
    let manifest_path = format!("{prefix}/manifest.json");
    let absolute_manifest_path = std::path::Path::new(&ctx.storage.base_path).join(&manifest_path);
    let mut manifest = if absolute_manifest_path.exists() {
        serde_json::from_str::<koldstore_manifest::Manifest>(
            &std::fs::read_to_string(&absolute_manifest_path).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?
    } else {
        koldstore_manifest::Manifest::new_shared(
            ctx.relation.namespace.clone(),
            ctx.relation.name.clone(),
            ctx.storage.schema_version as u32,
        )
    };
    let mut batch_number = next_flush_batch_number(table_oid)?;
    let mut total_rows_flushed = 0_i64;
    let mut last_max_seq = 0_i64;
    let mut last_max_commit_seq = 0_i64;

    for chunk in chunk_flush_write_input(write_input, ctx.max_rows_per_file) {
        let chunk_stats = super::stats::flush_stats_for_rows(&chunk.rows)?;
        let segment = write_flush_segment(
            table_oid,
            &ctx.relation,
            &ctx.storage,
            &ctx.snapshot,
            &write_input.columns,
            &chunk,
            batch_number,
            &chunk_stats,
        )?;
        manifest.append_segment(segment.0);
        total_rows_flushed = total_rows_flushed.saturating_add(chunk_stats.row_count);
        last_max_seq = chunk_stats.max_seq;
        last_max_commit_seq = chunk_stats.max_commit_seq;
        batch_number = batch_number.saturating_add(1);
    }

    Ok(FlushBatchOutcome {
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
    outcome: &FlushBatchOutcome,
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
    let ctx = prepare_flush_context(table_oid)?;
    let stats = resolve_flush_stats(table_oid, ctx.force)?;
    if stats.row_count == 0 {
        mark_flush_job_completed(ctx.job_id, table_oid, 0, 0, 0)?;
        return Ok(pgrx::Uuid::from_bytes(*ctx.job_id.as_bytes()));
    }

    let write_input = build_flush_write_input(table_oid, &ctx, &stats)?;
    if i64::try_from(write_input.rows.len()).map_err(|error| error.to_string())? != stats.row_count
    {
        return Err(format!(
            "flush row selection mismatch: stats reported {} rows but writer built {} rows",
            stats.row_count,
            write_input.rows.len()
        ));
    }

    let outcome = write_flush_batches(table_oid, &ctx, &write_input)?;
    finalize_flush(table_oid, &ctx, &outcome)?;
    Ok(pgrx::Uuid::from_bytes(*ctx.job_id.as_bytes()))
}
