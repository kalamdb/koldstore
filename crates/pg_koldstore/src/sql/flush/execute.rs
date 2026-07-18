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
    activate_flush_segments, capture_durable_wal_fence, lock_source_table_share_row_exclusive,
    manifest_from_publishable_cold_segments, manifest_generation, next_flush_batch_number,
    persist_flush_segment, prune_flushed_hot_rows, publishable_cold_segment_count,
    resolve_flush_stats,
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
    target_file_size_bytes: Option<u64>,
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
            target_file_size_bytes: self.target_file_size_bytes,
        }
    }
}

fn claim_flush_job(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> Result<(uuid::Uuid, bool), String> {
    crate::sql::job_lock_pg::lock_table_job(table_oid)?;
    let (job_id, force) = ensure_flush_job(table_oid, force)?;
    mark_flush_job_running(job_id, table_oid)?;
    crate::failpoints::hit("after_claim")?;
    Ok((job_id, force))
}

fn load_flush_prepared_context(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
    job_id: uuid::Uuid,
) -> Result<FlushPreparedContext, String> {
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
    let policy = super::spi::active_flush_policy(table_oid)?;
    let configured = policy.and_then(|policy| policy.max_rows_per_file);
    if let Some(value) = configured {
        let hint = format!(
            "lower the floor for testing with SET {} = <value>",
            crate::settings::MIN_MAX_ROWS_PER_FILE_GUC
        );
        koldstore_common::validate_max_rows_per_file(value, min_floor, Some(&hint))?;
    }
    let max_rows_per_file = max_rows_per_file_from_policy(configured, min_floor)?;
    let target_file_size_bytes = policy
        .and_then(|policy| policy.target_file_size_mb)
        .map(|megabytes| {
            megabytes
                .checked_mul(1024 * 1024)
                .ok_or_else(|| format!("target_file_size_mb {megabytes} is too large"))
        })
        .transpose()?;
    Ok(FlushPreparedContext {
        job_id,
        force,
        relation,
        storage,
        snapshot,
        catalog_columns: catalog.columns,
        indexed_columns,
        max_rows_per_file,
        target_file_size_bytes,
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
        target_file_size_bytes: ctx.target_file_size_bytes,
        compression: ctx.storage.compression.clone(),
        row_group_size: koldstore_parquet::WriterOptions::default().row_group_size,
        mirror_ops: selection.mirror_ops.clone(),
    };
    let catalog_columns = ctx.catalog_columns.clone();
    // Failpoint after pending catalog inserts lives in write_streamed_chunk.

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
                    table_oid,
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
    let pending_segment_ids: Vec<uuid::Uuid> = written_segments
        .iter()
        .map(|written| written.segment_id)
        .collect();
    let pending_manifest_segments = written_segments
        .iter()
        .map(|written| {
            build_manifest_segment_from_catalog_row(
                &ctx.relation.namespace,
                &ctx.relation.name,
                &ctx.snapshot.primary_key_columns,
                written.catalog_row.clone(),
            )
            .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let _ = manifest.append_segment_batch(pending_manifest_segments);

    // Reconcile derived manifest against pending+active catalog truth when counts drift.
    let catalog_segments = publishable_cold_segment_count(table_oid)?;
    if manifest.segments.len() as i64 != catalog_segments {
        manifest = manifest_from_publishable_cold_segments(
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
        pending_segment_ids,
    })
}

#[allow(clippy::too_many_arguments)]
fn write_streamed_chunk(
    client: &koldstore_storage::ObjectStoreClient,
    ctx: &FlushPreparedContext,
    table_oid: pgrx::pg_sys::Oid,
    batch_number: &mut i32,
    total_rows_flushed: &mut i64,
    last_max_seq: &mut i64,
    last_max_commit_seq: &mut i64,
    written_segments: &mut Vec<WrittenFlushSegment>,
    chunk: FlushWriteChunk,
) -> Result<(), String> {
    crate::failpoints::hit("during_parquet_write")?;
    let chunk_stats = FlushStats::from_write_chunk(&chunk)?;
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
    crate::failpoints::hit("after_temp_object")?;
    crate::failpoints::hit("after_checksum_metadata")?;
    persist_flush_segment(table_oid, &written)?;
    crate::failpoints::hit("after_pending_segment")?;
    *total_rows_flushed = total_rows_flushed.saturating_add(chunk_stats.row_count);
    *last_max_seq = chunk_stats.max_seq;
    *last_max_commit_seq = chunk_stats.max_commit_seq;
    pgrx::log!(
        "koldstore flush: wrote+cataloged segment batch={} rows={} total_rows={}",
        *batch_number,
        chunk_stats.row_count,
        *total_rows_flushed
    );
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
    phase0_applied: Option<crate::async_mirror::apply::AppliedWalBoundary>,
) -> Result<(), String> {
    // Phase 5.5: finite catch-up after upload, before catalog publication and
    // before SHARE ROW EXCLUSIVE. The transaction-scoped apply lock from phase 0
    // is still held (releasing it mid-flush deadlocks with worker peeks waiting
    // on this open XID); this pass drains accumulated WAL so the relation-lock
    // window covers only (Lp, F1].
    let skip_through = run_async_prelock_catchup(table_oid, outcome.prune_max_seq, phase0_applied)?;

    let client = open_client_from_catalog_fields(
        &ctx.storage.storage_type,
        &ctx.storage.base_path,
        &ctx.storage.credentials,
        &ctx.storage.config,
    )
    .map_err(|error| error.to_string())?;
    crate::failpoints::hit("before_manifest_publish")?;
    pgrx::log!(
        "koldstore flush: writing manifest path={} segments={} rows={}",
        outcome.manifest_path,
        outcome.manifest.segments.len(),
        outcome.total_rows_flushed
    );
    write_manifest_with_client(&client, &outcome.manifest_path, &outcome.manifest)?;
    crate::failpoints::hit("before_activate")?;
    let expected_generation = manifest_generation(table_oid)?;
    let _new_generation = activate_flush_segments(
        table_oid,
        expected_generation,
        &outcome.manifest_path,
        outcome.manifest.segments.len() as i32,
        outcome.manifest.max_seq,
        outcome.manifest.max_commit_seq,
        &outcome.pending_segment_ids,
    )?;
    crate::failpoints::hit("after_manifest_publish")?;
    run_async_prune_fence(table_oid, outcome.prune_max_seq, skip_through)?;
    crate::failpoints::hit("before_hot_cleanup")?;
    pgrx::log!(
        "koldstore flush: pruning hot/mirror rows through seq={}",
        outcome.prune_max_seq
    );
    crate::failpoints::hit("during_hot_cleanup")?;
    let (mirror_pruned, hot_pruned) = prune_flushed_hot_rows(
        table_oid,
        &ctx.snapshot.primary_key_columns,
        outcome.prune_max_seq,
        outcome.mirror_ops.as_deref(),
    )?;
    pgrx::log!(
        "koldstore flush: pruned mirror_rows={} hot_rows={}",
        mirror_pruned,
        hot_pruned
    );
    apply_flush_row_count_deltas(
        table_oid,
        mirror_pruned,
        hot_pruned,
        outcome.total_rows_flushed,
    )?;
    crate::failpoints::hit("after_cleanup_before_job_complete")?;
    mark_flush_job_completed(
        ctx.job_id,
        table_oid,
        outcome.total_rows_flushed,
        outcome.last_max_seq,
        outcome.last_max_commit_seq,
    )?;
    crate::failpoints::hit("after_job_complete_before_temp_cleanup")?;
    crate::catalog::cache::invalidate_table_globally(table_oid);
    Ok(())
}

/// Phase-5.5: finite pre-lock catch-up after object upload.
///
/// Returns the skip boundary (`Lp`) for phase 6.
fn run_async_prelock_catchup(
    table_oid: pgrx::pg_sys::Oid,
    prune_max_seq: i64,
    phase0_applied: Option<crate::async_mirror::apply::AppliedWalBoundary>,
) -> Result<Option<crate::async_mirror::apply::AppliedWalBoundary>, String> {
    use crate::async_mirror::apply::{apply_bounded, BoundedApplyRequest, PruneSeqFloor};
    use koldstore_common::MirrorCaptureMode;

    if super::spi::active_mirror_capture_mode(table_oid)? != MirrorCaptureMode::Async {
        return Ok(phase0_applied);
    }
    if prune_max_seq <= 0 {
        return Ok(phase0_applied);
    }

    let max_passes = crate::guc::flush_prelock_max_passes();
    let max_ms = crate::guc::flush_prelock_max_ms();
    let started = std::time::Instant::now();
    let mut skip_through = phase0_applied;

    for pass in 1..=max_passes {
        if started.elapsed().as_millis() as i64 >= max_ms {
            return Err(format!(
                "async flush pre-lock catch-up exceeded {max_ms}ms budget before relation lock"
            ));
        }
        let fence = capture_durable_wal_fence()?;
        let remaining_ms = (max_ms - started.elapsed().as_millis() as i64).max(1);
        pgrx::log!(
            "koldstore flush: pre-lock catch-up pass={pass}/{max_passes} upto_lsn={} skip_through={:?} floor={}",
            koldstore_common::format_pg_lsn(fence.get()),
            skip_through.map(|lsn| koldstore_common::format_pg_lsn(lsn.get())),
            prune_max_seq
        );
        let outcome = apply_bounded(BoundedApplyRequest {
            upper_bound: Some(fence),
            skip_through,
            acknowledge_durable_checkpoint: false,
            target_prune_floor: Some((table_oid, PruneSeqFloor::new(prune_max_seq))),
            max_rows: Some(0),
            max_ms: Some(remaining_ms),
        })?;
        skip_through = outcome.last_applied.or(skip_through);
        pgrx::log!(
            "koldstore flush: pre-lock catch-up pass={pass} row_changes={} budget_exhausted={}",
            outcome.row_changes,
            outcome.budget_exhausted
        );
        if outcome.row_changes == 0 && !outcome.budget_exhausted {
            break;
        }
        if pass == max_passes && outcome.budget_exhausted {
            return Err(format!(
                "async flush pre-lock catch-up exhausted {max_passes} passes with WAL remaining"
            ));
        }
    }
    Ok(skip_through)
}

/// Phase-6 async prune fence: block source writers, catch mirror up through a
/// durable WAL upper bound, then allow `prune_flushed_hot_rows` to run safely.
fn run_async_prune_fence(
    table_oid: pgrx::pg_sys::Oid,
    prune_max_seq: i64,
    skip_through: Option<crate::async_mirror::apply::AppliedWalBoundary>,
) -> Result<(), String> {
    use crate::async_mirror::apply::{apply_bounded, BoundedApplyRequest, PruneSeqFloor};
    use koldstore_common::MirrorCaptureMode;

    if super::spi::active_mirror_capture_mode(table_oid)? != MirrorCaptureMode::Async {
        return Ok(());
    }
    if prune_max_seq <= 0 {
        return Ok(());
    }

    lock_source_table_share_row_exclusive(table_oid)?;
    let fence = capture_durable_wal_fence()?;
    pgrx::log!(
        "koldstore flush: async prune fence upto_lsn={} skip_through={:?} floor={}",
        koldstore_common::format_pg_lsn(fence.get()),
        skip_through.map(|lsn| koldstore_common::format_pg_lsn(lsn.get())),
        prune_max_seq
    );
    let outcome = apply_bounded(BoundedApplyRequest {
        upper_bound: Some(fence),
        skip_through,
        acknowledge_durable_checkpoint: false,
        target_prune_floor: Some((table_oid, PruneSeqFloor::new(prune_max_seq))),
        max_rows: Some(0),
        max_ms: Some(0),
    })?;
    pgrx::log!(
        "koldstore flush: async prune fence applied row_changes={} last_applied={:?}",
        outcome.row_changes,
        outcome
            .last_applied
            .map(|lsn| koldstore_common::format_pg_lsn(lsn.get()))
    );
    Ok(())
}

pub(crate) fn flush_table_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> Result<pgrx::Uuid, String> {
    // Flush selects authoritative latest-state rows, so async capture must be
    // fenced before row selection. Strict databases return immediately because
    // they have no async slot. Retain L0 for the post-publish prune fence.
    let phase0 = crate::async_mirror::apply::apply_bounded(
        crate::async_mirror::apply::BoundedApplyRequest::available_unlimited(),
    )?;
    let (job_id, force) = claim_flush_job(table_oid, force)?;
    let job_uuid = crate::spi::uuid_to_pgrx(job_id);
    match flush_after_claim(table_oid, force, job_id, phase0.last_applied) {
        Ok(()) => Ok(job_uuid),
        Err(error) => {
            mark_flush_job_failed(job_id, table_oid, &error)?;
            // Segments/manifest/hot cleanup may already have committed work in
            // this transaction; drop stale merge-scan caches even on failure.
            crate::catalog::cache::invalidate_table_globally(table_oid);
            Ok(job_uuid)
        }
    }
}

fn flush_after_claim(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
    job_id: uuid::Uuid,
    phase0_applied: Option<crate::async_mirror::apply::AppliedWalBoundary>,
) -> Result<(), String> {
    let mut ctx = load_flush_prepared_context(table_oid, force, job_id)?;
    match crate::sql::migrate_pg::refresh_active_schema_if_changed(table_oid) {
        Ok(true) => {
            ctx = load_flush_prepared_context(table_oid, force, job_id)?;
        }
        Ok(false) => {}
        Err(error) => return Err(error),
    }
    flush_prepared_table(table_oid, &ctx, phase0_applied)
}

fn flush_prepared_table(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
    phase0_applied: Option<crate::async_mirror::apply::AppliedWalBoundary>,
) -> Result<(), String> {
    let selection = resolve_flush_stats(table_oid, ctx.force)?;
    crate::failpoints::hit("after_select_rows")?;
    if selection.stats.row_count == 0 {
        mark_flush_job_completed(ctx.job_id, table_oid, 0, 0, 0)?;
        crate::catalog::cache::invalidate_table_globally(table_oid);
        return Ok(());
    }

    pgrx::log!(
        "koldstore flush: starting table={} rows={} max_seq={} force={}",
        ctx.relation.name,
        selection.stats.row_count,
        selection.stats.max_seq,
        ctx.force
    );
    let outcome = stream_write_flush_batches(table_oid, ctx, &selection)?;
    finalize_flush(table_oid, ctx, &outcome, phase0_applied)
}
