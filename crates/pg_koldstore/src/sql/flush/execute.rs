//! Flush orchestration for `koldstore.flush_table`.
//!
//! Owns PostgreSQL-specific job locking and SPI wiring. Flush workflow logic
//! lives in `koldstore-flush`.

use std::sync::Arc;

use koldstore_catalog::decode::{FlushStorageContext, RelationContext};
use koldstore_catalog::ManagedTableSnapshot;
use koldstore_common::{dedupe_nonblank, QualifiedTableName};
use koldstore_flush::{
    manifest_paths, max_rows_per_file_from_policy, plan_apply_flush_row_count_deltas,
    stream_flush_chunks, validate_flush_row_selection, write_flush_segment_with_client, FlushStats,
    FlushWriteChunk, ResolvedFlushSelection, StreamEncodeInput, TableFlushBatchOutcome,
    TableFlushPreparedContext, WrittenFlushSegment,
};
use koldstore_manifest::{
    build_manifest_segment_from_catalog_row, try_load_manifest_with_client,
    write_manifest_with_client,
};
use koldstore_storage::open_client_from_catalog_fields;

use super::jobs::{
    ensure_flush_job, mark_flush_job_completed, mark_flush_job_failed, mark_flush_job_running,
};
use super::mirror_fetch::fetch_mirror_batch;
use super::spi::{
    active_cold_segment_count, manifest_from_active_cold_segments, next_flush_batch_number,
    persist_flush_segments_batch, prune_flushed_hot_rows, resolve_flush_stats, upsert_manifest_row,
};

pub(super) struct FlushPreparedContext {
    job_id: uuid::Uuid,
    force: bool,
    relation: RelationContext,
    storage: FlushStorageContext,
    snapshot: Arc<ManagedTableSnapshot>,
    catalog_columns: Vec<koldstore_migrate::order::CatalogColumn>,
    indexed_columns: Vec<String>,
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
    let indexed_columns = dedupe_nonblank(
        snapshot
            .primary_key_columns
            .iter()
            .map(String::as_str)
            .chain(catalog.indexed_columns.iter().map(String::as_str)),
    );
    let min_floor = u64::try_from(crate::guc::min_max_rows_per_file())
        .unwrap_or(koldstore_common::DEFAULT_MIN_MAX_ROWS_PER_FILE);
    let configured =
        super::spi::active_flush_policy(table_oid)?.and_then(|policy| policy.max_rows_per_file);
    if let Some(value) = configured {
        let hint = format!(
            "lower the floor for testing with SET {} = <value>",
            crate::settings::MIN_MAX_ROWS_PER_FILE_GUC
        );
        koldstore_common::validate_max_rows_per_file(value, min_floor, Some(&hint))?;
    }
    let max_rows_per_file = max_rows_per_file_from_policy(configured, min_floor)?;
    Ok(FlushPreparedContext {
        job_id,
        force,
        relation,
        storage,
        snapshot,
        catalog_columns: catalog.columns,
        indexed_columns,
        max_rows_per_file,
    })
}

pub(super) fn stream_write_flush_batches(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
    selection: &ResolvedFlushSelection,
) -> Result<TableFlushBatchOutcome, String> {
    let stats = &selection.stats;
    let table_ctx = ctx.as_table_flush_context();
    let (manifest_path, absolute_manifest_path) = manifest_paths(
        &table_ctx.namespace,
        &table_ctx.table_name,
        &table_ctx.base_path,
    );
    let schema_version =
        u32::try_from(ctx.storage.schema_version).map_err(|error| error.to_string())?;
    let client = open_client_from_catalog_fields(
        &ctx.storage.storage_type,
        &ctx.storage.base_path,
        &ctx.storage.credentials,
        &ctx.storage.config,
    )
    .map_err(|error| error.to_string())?;
    let mut manifest =
        try_load_manifest_with_client(&client, &manifest_path)?.unwrap_or_else(|| {
            koldstore_manifest::Manifest::new_shared(
                table_ctx.namespace.clone(),
                table_ctx.table_name.clone(),
                schema_version,
            )
        });
    let mut batch_number = next_flush_batch_number(table_oid)?;
    let mut total_rows_flushed = 0_i64;
    let mut last_max_seq = 0_i64;
    let mut last_max_commit_seq = 0_i64;
    let mut written_segments: Vec<WrittenFlushSegment> = Vec::new();

    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table = QualifiedTableName::parse(&relation).map_err(|error| error.to_string())?;
    let mirror = QualifiedTableName::from_table_name(&ctx.snapshot.mirror_relation);
    let encode_input = StreamEncodeInput {
        table,
        mirror,
        primary_key_columns: ctx.snapshot.primary_key_columns.clone(),
        base_column_names: ctx
            .catalog_columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        parquet_columns: ctx
            .catalog_columns
            .iter()
            .map(|column| {
                koldstore_parquet::PgColumn::new(column.name.clone(), column.pg_type, true)
            })
            .collect(),
        indexed_columns: ctx.indexed_columns.clone(),
        schema_version,
        max_seq: stats.max_seq,
        max_rows_per_file: ctx.max_rows_per_file,
        mirror_ops: selection.mirror_ops.clone(),
    };
    let catalog_columns = ctx.catalog_columns.clone();

    let stream_outcome = crate::merge_scan::pg::with_custom_scan_disabled(|| {
        stream_flush_chunks(
            &encode_input,
            |statement, max_seq, after_seq| {
                fetch_mirror_batch(&catalog_columns, statement, max_seq, after_seq)
            },
            |chunk| {
                write_streamed_chunk(
                    &client,
                    ctx,
                    &mut batch_number,
                    &mut total_rows_flushed,
                    &mut last_max_seq,
                    &mut last_max_commit_seq,
                    &mut written_segments,
                    chunk,
                )
            },
        )
    })?;

    validate_flush_row_selection(stats.row_count, stream_outcome.rows_written)?;
    let pending_manifest_segments = written_segments
        .iter()
        .map(|written| {
            build_manifest_segment_from_catalog_row(
                &ctx.relation.namespace,
                &ctx.relation.name,
                &ctx.snapshot.primary_key_columns,
                &written.catalog_row,
            )
            .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let _ = manifest.append_segment_batch(pending_manifest_segments);

    persist_flush_segments_batch(table_oid, &written_segments)?;

    let catalog_segments = active_cold_segment_count(table_oid)?;
    if manifest.segments.len() as i64 != catalog_segments {
        manifest = manifest_from_active_cold_segments(
            table_oid,
            &ctx.relation,
            &ctx.snapshot,
            ctx.storage.schema_version,
        )?;
    }

    Ok(TableFlushBatchOutcome {
        total_rows_flushed,
        last_max_seq,
        last_max_commit_seq,
        mirror_ops: selection.mirror_ops.clone(),
        prune_max_seq: stream_outcome.max_seq,
        manifest,
        manifest_path,
        absolute_manifest_path,
    })
}

#[allow(clippy::too_many_arguments)]
fn write_streamed_chunk(
    client: &koldstore_storage::ObjectStoreClient,
    ctx: &FlushPreparedContext,
    batch_number: &mut i32,
    total_rows_flushed: &mut i64,
    last_max_seq: &mut i64,
    last_max_commit_seq: &mut i64,
    written_segments: &mut Vec<WrittenFlushSegment>,
    chunk: FlushWriteChunk,
) -> Result<(), String> {
    let chunk_stats = FlushStats::from_cold_batch(&chunk.cold_batch)?;
    let written = write_flush_segment_with_client(
        client,
        &ctx.relation.namespace,
        &ctx.relation.name,
        &ctx.storage.compression,
        &ctx.snapshot.primary_key_columns,
        &ctx.indexed_columns,
        ctx.storage.schema_version,
        *batch_number,
        &chunk,
        &chunk_stats,
    )?;
    *total_rows_flushed = total_rows_flushed.saturating_add(chunk_stats.row_count);
    *last_max_seq = chunk_stats.max_seq;
    *last_max_commit_seq = chunk_stats.max_commit_seq;
    *batch_number = batch_number.saturating_add(1);
    written_segments.push(written);
    Ok(())
}

fn apply_flush_row_count_deltas(
    table_oid: pgrx::pg_sys::Oid,
    mirror_pruned: i64,
    hot_pruned: i64,
    cold_rows_added: i64,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    let statement = plan_apply_flush_row_count_deltas().map_err(|error| error.to_string())?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(mirror_pruned),
            DatumWithOid::from(hot_pruned),
            DatumWithOid::from(cold_rows_added),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

pub(super) fn finalize_flush(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
    outcome: &TableFlushBatchOutcome,
) -> Result<(), String> {
    let client = open_client_from_catalog_fields(
        &ctx.storage.storage_type,
        &ctx.storage.base_path,
        &ctx.storage.credentials,
        &ctx.storage.config,
    )
    .map_err(|error| error.to_string())?;
    write_manifest_with_client(&client, &outcome.manifest_path, &outcome.manifest)?;
    upsert_manifest_row(
        table_oid,
        &outcome.manifest_path,
        outcome.manifest.segments.len() as i32,
        outcome.manifest.max_seq,
        outcome.manifest.max_commit_seq,
    )?;
    let (mirror_pruned, hot_pruned) = prune_flushed_hot_rows(
        table_oid,
        &ctx.snapshot.primary_key_columns,
        outcome.prune_max_seq,
        outcome.mirror_ops.as_deref(),
    )?;
    apply_flush_row_count_deltas(
        table_oid,
        mirror_pruned,
        hot_pruned,
        outcome.total_rows_flushed,
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
    let selection = resolve_flush_stats(table_oid, ctx.force)?;
    if selection.stats.row_count == 0 {
        mark_flush_job_completed(ctx.job_id, table_oid, 0, 0, 0)?;
        return Ok(());
    }

    let outcome = stream_write_flush_batches(table_oid, ctx, &selection)?;
    finalize_flush(table_oid, ctx, &outcome)
}
