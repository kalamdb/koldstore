//! Flush orchestration for `koldstore.flush_table`.
//!
//! Owns PostgreSQL-specific job locking and SPI wiring. Flush workflow logic
//! lives in `koldstore-flush`.
//!
//! ## Commit styles
//!
//! - [`FlushCommitStyle::Nested`]: claim + work stay inside the caller's
//!   transaction (inline `flush_table` / `#[pg_test]`). No mid-flush commits.
//! - [`FlushCommitStyle::Short`]: queue flush executors. Each SPI catalog
//!   boundary opens/commits via [`crate::worker::txn`]; object upload runs
//!   outside any PostgreSQL transaction.

use std::sync::Arc;

use koldstore_catalog::decode::{FlushStorageContext, RelationContext};
use koldstore_catalog::ManagedTableSnapshot;
use koldstore_common::{ColumnRef, FlushPolicy, QualifiedTableName, SeqId};
use koldstore_flush::{
    flush_mirror_fetch_limit, flush_phase, max_rows_per_file_from_policy,
    plan_apply_flush_row_count_deltas, should_continue_flush_catchup, should_start_catchup_pass,
    stream_flush_chunks, validate_flush_row_selection, write_flush_segment_with_client, FlushStats,
    FlushWriteChunk, ResolvedFlushSelection, StreamEncodeInput, TableFlushBatchOutcome,
};
use koldstore_manifest::write_manifest_with_client;
use koldstore_storage::{manifest_object_key, render_regular_table_prefix, PathTemplate};

use super::jobs::{
    ensure_flush_job, flush_cancel_requested, mark_flush_job_cancelled, mark_flush_job_completed,
    mark_flush_job_completed_after_cancel, mark_flush_job_failed, mark_flush_job_running,
    update_flush_job_progress, FlushJobProgressUpdate,
};
use super::mirror_fetch::fetch_mirror_batch;
use super::spi::{
    activate_flush_segments, capture_durable_wal_fence, lock_source_table_share_row_exclusive,
    manifest_from_publishable_cold_segments, manifest_generation, mirror_catchup_watermark,
    next_flush_batch_number, persist_flush_segment, prune_flushed_hot_rows, resolve_flush_stats,
};

/// How flush catalog SPI interacts with PostgreSQL transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlushCommitStyle {
    /// Already inside the caller's transaction (inline / `pg_test`).
    Nested,
    /// Queue executor: each SPI boundary opens and commits its own short txn.
    Short,
}

impl FlushCommitStyle {
    /// Runs `body` either nested in the caller txn or in a short BGWorker txn.
    fn run_spi<R>(self, body: impl FnOnce() -> Result<R, String>) -> Result<R, String> {
        match self {
            Self::Nested => body(),
            Self::Short => crate::worker::txn::run(body),
        }
    }
}

pub(super) struct FlushPreparedContext {
    job_id: uuid::Uuid,
    attempt_token: uuid::Uuid,
    force: bool,
    relation: RelationContext,
    storage: FlushStorageContext,
    snapshot: Arc<ManagedTableSnapshot>,
    catalog: Arc<koldstore_migrate::ExistingTableCatalog>,
    indexed_columns: Vec<ColumnRef>,
    bloom_filter_columns: Vec<String>,
    max_rows_per_file: usize,
    target_file_size_bytes: Option<u64>,
    parquet_row_group_size: usize,
    parquet_data_page_row_count_limit: Option<usize>,
    parquet_bloom_filter_false_positive_rate: Option<f64>,
}

/// Acquires the session table-job lock without waiting.
///
/// Manual `flush_table` must not block behind auto-flush — those can hold the
/// table lock for a long time with no client-visible progress. Scheduler ticks
/// already skip via try-lock; SQL callers get a clear error instead.
fn try_acquire_flush_table_lock(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<crate::sql::job_lock::TableJobLockGuard, String> {
    crate::sql::job_lock::TableJobLockGuard::try_lock(table_oid)?
        .ok_or_else(|| flush_already_in_progress_message(table_oid))
}

fn claim_flush_job(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> Result<(uuid::Uuid, uuid::Uuid, bool, i64, Option<SeqId>), String> {
    // Caller already holds the session table-job lock — do not re-lock (that
    // would bump the session lock count and require a matching extra unlock).
    let (job_id, force) = ensure_flush_job(table_oid, force)?;
    // Fixed watermark + progress estimate at claim (newer rows stay hot).
    let progress_total = super::spi::flush_progress_total_estimate(table_oid, force)?;
    let target_seq =
        super::spi::mirror_catchup_watermark(table_oid)?.and_then(|seq| SeqId::new(seq).ok());
    let attempt_token = mark_flush_job_running(job_id, table_oid, progress_total, target_seq)?;
    crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterClaim)?;
    Ok((job_id, attempt_token, force, progress_total, target_seq))
}

fn load_flush_prepared_context(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
    job_id: uuid::Uuid,
    attempt_token: uuid::Uuid,
) -> Result<FlushPreparedContext, String> {
    let relation = crate::catalog::resolve::relation_context(table_oid)?;
    let storage = crate::catalog::resolve::active_flush_storage_context(table_oid)?;
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let catalog = crate::sql::migrate::migration_catalog(table_oid.to_u32())?;
    let mut seen_column_ids = std::collections::BTreeSet::new();
    let mut indexed_columns = catalog
        .columns
        .iter()
        .filter(|column| column.is_primary_key)
        .map(|column| ColumnRef::new(column.column_id, column.name.clone()))
        .chain(catalog.indexed_columns.iter().cloned())
        .filter(|column| seen_column_ids.insert(column.column_id))
        .collect::<Vec<_>>();
    if let Some(order_column_id) = snapshot.segment_order_column_id {
        if let Some(column) = catalog
            .columns
            .iter()
            .find(|column| column.column_id == order_column_id)
        {
            if seen_column_ids.insert(column.column_id) {
                indexed_columns.push(ColumnRef::new(column.column_id, column.name.clone()));
            }
        }
    }
    let options = super::spi::active_manage_options(table_oid)?.unwrap_or_default();
    let cold_metadata = super::spi::active_cold_metadata(table_oid)?;
    let policy = options.flush_policy();
    let configured = policy.as_ref().map(FlushPolicy::max_rows_per_file);
    // Trust the catalog value: `manage_table` already validated against the
    // floor GUC in the managing session. Re-checking against the *current*
    // backend's `min_max_rows_per_file` breaks queue flush executors (and any
    // peer) that never inherited that session SET — they would reject a
    // persisted `max_rows_per_file=5` with the default floor of 1000.
    let max_rows_per_file = max_rows_per_file_from_policy(configured, 1)?;
    let target_file_size_bytes = options
        .target_file_size_mb
        .map(|megabytes| {
            megabytes
                .checked_mul(1024 * 1024)
                .ok_or_else(|| format!("target_file_size_mb {megabytes} is too large"))
        })
        .transpose()?;
    let parquet_row_group_size = options
        .parquet_row_group_size
        .map(usize::try_from)
        .transpose()
        .map_err(|error| format!("parquet_row_group_size is too large: {error}"))?
        .unwrap_or_else(|| koldstore_parquet::WriterOptions::default().row_group_size);
    let parquet_data_page_row_count_limit = options
        .parquet_data_page_row_count_limit
        .map(usize::try_from)
        .transpose()
        .map_err(|error| format!("parquet_data_page_row_count_limit is too large: {error}"))?;
    let parquet_bloom_filter_false_positive_rate = options
        .parquet_bloom_filter_fpp
        .map(koldstore_common::ParquetBloomFilterFpp::get);
    if let Some(cold) = cold_metadata.as_ref() {
        if !cold.stats_columns.is_empty() {
            // Start from catalog stats, then force PK + order column so Exact-PK
            // / migration_order_by segment-index probes never lose their bounds
            // when operators only listed secondary pruning columns.
            indexed_columns = cold.stats_columns.clone();
            for column in catalog
                .columns
                .iter()
                .filter(|column| column.is_primary_key)
            {
                if !indexed_columns
                    .iter()
                    .any(|existing| existing.column_id == column.column_id)
                {
                    indexed_columns.push(ColumnRef::new(column.column_id, column.name.clone()));
                }
            }
            if let Some(order_column_id) = snapshot.segment_order_column_id {
                if let Some(column) = catalog
                    .columns
                    .iter()
                    .find(|column| column.column_id == order_column_id)
                {
                    if !indexed_columns
                        .iter()
                        .any(|existing| existing.column_id == column.column_id)
                    {
                        indexed_columns.push(ColumnRef::new(column.column_id, column.name.clone()));
                    }
                }
            }
        }
    }
    let bloom_filter_columns = cold_metadata
        .as_ref()
        .map(|cold| {
            cold.bloom_filter_columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
        })
        .filter(|columns| !columns.is_empty())
        .unwrap_or_else(|| {
            catalog
                .columns
                .iter()
                .filter(|column| column.is_primary_key)
                .map(|column| column.name.clone())
                .collect()
        });
    Ok(FlushPreparedContext {
        job_id,
        attempt_token,
        force,
        relation,
        storage,
        snapshot,
        catalog,
        indexed_columns,
        bloom_filter_columns,
        max_rows_per_file,
        target_file_size_bytes,
        parquet_row_group_size,
        parquet_data_page_row_count_limit,
        parquet_bloom_filter_false_positive_rate,
    })
}

/// Lightweight pending-segment identity retained after catalog insert.
///
/// Avoids holding full [`koldstore_flush::WrittenFlushSegment`] (packed metadata
/// + catalog row) across the rest of the pass.
#[derive(Debug, Clone, Copy)]
struct PendingFlushSegmentRef {
    segment_id: uuid::Uuid,
    byte_size: i64,
}

/// Pass upload/persist outcome before manifest build + finalize.
struct StreamedFlushPass {
    total_rows_flushed: i64,
    last_max_seq: i64,
    bytes_written: i64,
    mirror_ops: Option<Vec<i16>>,
    prune_max_seq: i64,
    pending_segment_ids: Vec<uuid::Uuid>,
    manifest_path: String,
}

fn stream_write_flush_batches(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
    selection: &ResolvedFlushSelection,
    client: &koldstore_storage::ObjectStoreClient,
    commit_style: FlushCommitStyle,
    started: std::time::Instant,
) -> Result<StreamedFlushPass, String> {
    let stats = &selection.stats;
    let table_prefix = render_regular_table_prefix(
        &PathTemplate::new(&ctx.storage.regular_path_tmpl),
        &ctx.relation.namespace,
        &ctx.relation.name,
    )?;
    let manifest_path = manifest_object_key(&table_prefix);
    let schema_version =
        u32::try_from(ctx.storage.schema_version).map_err(|error| error.to_string())?;
    let mut batch_number = commit_style.run_spi(|| next_flush_batch_number(table_oid))?;
    let pass_id = uuid::Uuid::new_v4();
    let mut total_rows_flushed = 0_i64;
    let mut last_max_seq = 0_i64;
    let mut pending_segments: Vec<PendingFlushSegmentRef> = Vec::new();

    // Use the already-loaded relation context — do not SPI here. Short mode is
    // between catalog commits (no open txn); `qualified_relation_name` would
    // Assert(IsTransactionState) in the flush executor.
    let table =
        QualifiedTableName::parse(&format!("{}.{}", ctx.relation.namespace, ctx.relation.name))
            .map_err(|error| error.to_string())?;
    let mirror = QualifiedTableName::from_table_name(&ctx.snapshot.mirror_relation);
    let primary_key_columns: Vec<String> = ctx
        .snapshot
        .primary_key_names()
        .map(str::to_string)
        .collect();
    let primary_key_param_types = if ctx.snapshot.segment_order_column_id.is_some() {
        primary_key_columns
            .iter()
            .map(|pk_name| {
                let column = ctx
                    .catalog
                    .columns
                    .iter()
                    .find(|column| column.name == *pk_name)
                    .ok_or_else(|| {
                        format!("primary-key column `{pk_name}` missing from catalog")
                    })?;
                super::mirror_fetch::flush_keyset_param_type(column.pg_type)
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };
    let encode_input = StreamEncodeInput {
        table,
        mirror,
        primary_key_columns: primary_key_columns.clone(),
        primary_key_param_types,
        base_column_names: ctx
            .catalog
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        parquet_columns: ctx
            .catalog
            .columns
            .iter()
            // Delete markers retain every PK value but intentionally leave
            // non-PK payload columns null, even when the source column is
            // declared NOT NULL. Keeping only PK columns non-null also makes
            // their footer bounds a required publication invariant.
            .map(|column| {
                koldstore_parquet::PgColumn::new(
                    column.name.clone(),
                    column.pg_type,
                    !column.is_primary_key,
                )
            })
            .collect(),
        indexed_columns: ctx.indexed_columns.clone(),
        bloom_filter_columns: ctx.bloom_filter_columns.clone(),
        schema_version,
        max_seq: stats.max_seq,
        max_rows_per_file: ctx.max_rows_per_file,
        fetch_batch_size: flush_mirror_fetch_limit(ctx.max_rows_per_file),
        target_file_size_bytes: ctx.target_file_size_bytes,
        compression: ctx.storage.compression.clone(),
        row_group_size: ctx.parquet_row_group_size,
        data_page_row_count_limit: ctx.parquet_data_page_row_count_limit,
        bloom_filter_false_positive_rate: ctx.parquet_bloom_filter_false_positive_rate,
        mirror_ops: selection.mirror_ops.clone(),
        sort_by_order_key: ctx.snapshot.segment_order_column_id.is_some(),
        order_key_column: ctx.snapshot.segment_order_column_id.and_then(|column_id| {
            ctx.catalog
                .columns
                .iter()
                .find(|column| column.column_id == column_id)
                .map(|column| column.name.clone())
        }),
    };
    let catalog_columns = &ctx.catalog.columns;
    let fetch_batch_size = encode_input.fetch_batch_size;
    // Failpoint after pending catalog inserts lives in write_streamed_chunk.

    let stream_outcome = crate::merge_scan::pg::with_custom_scan_disabled(|| {
        stream_flush_chunks(
            &encode_input,
            |statement, max_seq, cursor| {
                // Short: each mirror fetch is its own catalog transaction.
                commit_style.run_spi(|| {
                    fetch_mirror_batch(
                        catalog_columns,
                        &primary_key_columns,
                        statement,
                        max_seq,
                        cursor,
                        fetch_batch_size,
                        ctx.snapshot.segment_order_column_id.is_some(),
                    )
                })
            },
            |chunk| {
                write_streamed_chunk(
                    client,
                    ctx,
                    pass_id,
                    &table_prefix,
                    table_oid,
                    commit_style,
                    &mut batch_number,
                    &mut total_rows_flushed,
                    &mut last_max_seq,
                    &mut pending_segments,
                    chunk,
                    started,
                )
            },
        )
    })?;

    validate_flush_row_selection(stats.row_count, stream_outcome.rows_written)?;
    let pending_segment_ids: Vec<uuid::Uuid> = pending_segments
        .iter()
        .map(|pending| pending.segment_id)
        .collect();
    let bytes_written = pending_segments
        .iter()
        .map(|pending| pending.byte_size)
        .fold(0_i64, i64::saturating_add);

    Ok(StreamedFlushPass {
        total_rows_flushed,
        last_max_seq,
        bytes_written,
        mirror_ops: selection.mirror_ops.clone(),
        prune_max_seq: stream_outcome.max_seq,
        pending_segment_ids,
        manifest_path,
    })
}

/// Builds publication metadata and runs finalize under one SPI boundary.
///
/// Short mode: one short txn for catalog manifest + finalize (manifest object
/// I/O stays inside finalize as today). Nested mode: same work in the caller txn.
fn build_manifest_and_finalize(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
    streamed: StreamedFlushPass,
    client: &koldstore_storage::ObjectStoreClient,
    commit_style: FlushCommitStyle,
) -> Result<TableFlushBatchOutcome, String> {
    let StreamedFlushPass {
        total_rows_flushed,
        last_max_seq,
        bytes_written,
        mirror_ops,
        prune_max_seq,
        pending_segment_ids,
        manifest_path,
    } = streamed;
    commit_style.run_spi(|| {
        // PERFORMANCE: catalog is the source of truth for publishable segments.
        let manifest = manifest_from_publishable_cold_segments(
            table_oid,
            &ctx.relation,
            &ctx.snapshot,
            ctx.storage.schema_version,
        )?;
        // Move pass-owned vectors/strings directly into finalize. A pass can
        // contain many pending segment ids or mirror-op markers; cloning them
        // immediately before the slot-locked finalize doubles peak memory for no
        // correctness benefit.
        let outcome = TableFlushBatchOutcome {
            total_rows_flushed,
            last_max_seq,
            bytes_written,
            mirror_ops,
            prune_max_seq,
            manifest,
            manifest_path,
            pending_segment_ids,
        };
        finalize_flush(table_oid, ctx, &outcome, client)?;
        Ok(outcome)
    })
}

#[allow(clippy::too_many_arguments)]
fn write_streamed_chunk(
    client: &koldstore_storage::ObjectStoreClient,
    ctx: &FlushPreparedContext,
    pass_id: uuid::Uuid,
    table_prefix: &str,
    table_oid: pgrx::pg_sys::Oid,
    commit_style: FlushCommitStyle,
    batch_number: &mut i32,
    total_rows_flushed: &mut i64,
    last_max_seq: &mut i64,
    pending_segments: &mut Vec<PendingFlushSegmentRef>,
    chunk: FlushWriteChunk,
    started: std::time::Instant,
) -> Result<(), String> {
    // Per-chunk budget check: a single pass over a very large force flush can
    // stream many chunks before the outer per-pass check in
    // `flush_prepared_table` runs again, so re-check here too or a huge
    // one-pass flush could outlive `flush_job_max_runtime_seconds` entirely.
    ensure_flush_runtime_budget(started)?;
    // Wait/error failpoints need SPI; keep them inside a short txn when Short.
    commit_style.run_spi(|| {
        crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::DuringParquetWrite)
    })?;
    let chunk_stats = FlushStats::from_write_chunk(&chunk)?;
    // Object upload is intentionally outside any PostgreSQL transaction (Short).
    let written = write_flush_segment_with_client(
        client,
        table_prefix,
        ctx.storage.schema_version,
        *batch_number,
        &chunk,
        &chunk_stats,
    )?;
    // Free compressed Parquet bytes before catalog SPI / next segment encode.
    drop(chunk);
    let byte_size = written.catalog_row.byte_size;
    let segment_id = written.segment_id;
    let batch = *batch_number;
    let rows = chunk_stats.row_count;
    // Post-upload failpoints (may Wait) in their own SPI boundary so a barrier
    // park does not hold the pending-segment insert transaction open.
    commit_style.run_spi(|| {
        crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterTempObject)?;
        crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterChecksumMetadata)?;
        Ok(())
    })?;
    // Persist pending segment in its own short txn immediately after upload.
    commit_style.run_spi(|| {
        persist_flush_segment(
            table_oid,
            super::spi::FlushSegmentWriterIdentity {
                job_id: ctx.job_id,
                attempt_token: ctx.attempt_token,
                pass_id,
                physically_sorted_sort_order_id: ctx
                    .snapshot
                    .segment_order_column_id
                    .map(|column_id| i32::from(column_id.get()))
                    .unwrap_or(0),
            },
            &written,
        )?;
        crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterPendingSegment)?;
        Ok(())
    })?;
    // Drop packed metadata + catalog row; retain only ids + byte sum.
    drop(written);
    *total_rows_flushed = total_rows_flushed.saturating_add(rows);
    *last_max_seq = chunk_stats.max_seq;
    pgrx::log!(
        "koldstore flush: wrote+cataloged segment batch={batch} rows={rows} bytes={byte_size} total_rows={}",
        *total_rows_flushed
    );
    *batch_number = batch_number.saturating_add(1);
    pending_segments.push(PendingFlushSegmentRef {
        segment_id,
        byte_size,
    });
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
    client: &koldstore_storage::ObjectStoreClient,
) -> Result<(), String> {
    // One critical section under try-lock slot ownership: prelock catch-up,
    // manifest write, activate, source fence, prune. Encode/upload already
    // finished without the slot lock.
    with_slot_lock_retry(|| {
        let skip_through = run_async_prelock_catchup(table_oid, outcome.prune_max_seq)?;

        crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::BeforeManifestPublish)?;
        pgrx::log!(
            "koldstore flush: writing manifest path={} segments={} rows={}",
            outcome.manifest_path,
            outcome.manifest.segments.len(),
            outcome.total_rows_flushed
        );
        write_manifest_with_client(client, &outcome.manifest_path, &outcome.manifest)?;
        crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::BeforeActivate)?;
        let expected_generation = manifest_generation(table_oid)?;
        activate_flush_segments(
            table_oid,
            expected_generation,
            outcome.manifest.segments.len() as i32,
            outcome.manifest.max_seq,
            &outcome.pending_segment_ids,
        )?;
        // Broadcast before hot prune so peer backends drop a stale "no cold"
        // cache. Otherwise queue Short-commit readers can observe pruned hot
        // while still planning as hot-only (count/LIMIT → 0). Relcache
        // invalidation requires an open txn (this finalize SPI boundary).
        crate::catalog::cache::invalidate_table_globally(table_oid);
        crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterManifestPublish)?;
        run_async_prune_fence(table_oid, outcome.prune_max_seq, skip_through)?;
        crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::BeforeHotCleanup)?;
        pgrx::log!(
            "koldstore flush: pruning hot/mirror rows through seq={}",
            outcome.prune_max_seq
        );
        crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::DuringHotCleanup)?;
        let primary_key_names = ctx
            .snapshot
            .primary_key_names()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let (mirror_pruned, hot_pruned) = prune_flushed_hot_rows(
            table_oid,
            &primary_key_names,
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
        crate::failpoints::hit_typed(
            crate::failpoints::FlushFailpoint::AfterCleanupBeforeJobComplete,
        )?;
        crate::failpoints::hit_typed(
            crate::failpoints::FlushFailpoint::AfterJobCompleteBeforeTempCleanup,
        )?;
        Ok(())
    })
}

/// Tries the slot lock with bounded backoff; never blocks indefinitely.
///
/// Callers run finalize fence work while the lock is held. Nested apply uses
/// [`apply_bounded_locked`] so we do not depend on re-entrant blocking lock.
fn with_slot_lock_retry<T>(body: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    use crate::mirror::lifecycle::try_lock_slot;

    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32();
    // Bound wait so finalize never blocks forever on a stuck applier, but allow
    // several seconds under parallel E2E / busy apply (former ~0.8s budget flaked).
    const MAX_ATTEMPTS: u32 = 200;
    const SLEEP_MS: u64 = 50;
    for attempt in 1..=MAX_ATTEMPTS {
        crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::BeforeSlotLock)?;
        if try_lock_slot(database_oid)? {
            crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterSlotLock)?;
            return body();
        }
        if attempt == MAX_ATTEMPTS {
            break;
        }
        pgrx::log!("koldstore flush: slot lock busy (attempt {attempt}/{MAX_ATTEMPTS}); retrying");
        std::thread::sleep(std::time::Duration::from_millis(SLEEP_MS));
    }
    Err("flush finalize could not acquire slot lock before deadline".to_string())
}

/// Drops flush-scoped caches and requests allocator page release.
///
/// Calls are safe at pass boundaries: allocator trimming is coalesced in the
/// memory module, so a multi-pass job does not walk glibc arenas on every pass.
/// Relcache broadcast requires an open transaction; between Short SPI commits
/// only the backend-local caches are cleared.
pub(crate) fn release_flush_memory(table_oid: pgrx::pg_sys::Oid) {
    crate::catalog::cache::invalidate_table_globally(table_oid);
    crate::memory::release_process_heap();
}

/// Brief slot-locked apply so selection sees committed WAL rows.
///
/// Queue / [`FlushCommitStyle::Short`] only: each SPI boundary commits, so the
/// transaction-scoped apply lock is released before encode/upload.
///
/// Inline / [`FlushCommitStyle::Nested`] skips this path — taking the xact apply
/// lock in the outer `flush_table` statement would hold it through upload and
/// stall the async applier. Callers (and E2E fixtures) must drain WAL first
/// (`fence_async_mirror` / prior apply). Finalize still catch-up applies under
/// its own slot-lock critical section.
fn catch_up_mirror_before_select(commit_style: FlushCommitStyle) -> Result<(), String> {
    match commit_style {
        FlushCommitStyle::Nested => Ok(()),
        FlushCommitStyle::Short => with_slot_lock_retry(|| {
            use crate::mirror::apply::{apply_bounded_locked, BoundedApplyRequest};
            let outcome = apply_bounded_locked(BoundedApplyRequest::available())?;
            if outcome.row_changes > 0 {
                pgrx::log!(
                    "koldstore flush: pre-select mirror catch-up row_changes={}",
                    outcome.row_changes
                );
            }
            Ok(())
        }),
    }
}

/// Phase-5.5: finite pre-lock catch-up after object upload.
///
/// Caller must hold the slot lock (see [`with_slot_lock_retry`]). Returns the
/// skip boundary for phase 6. Starts from no prior apply boundary (encode runs
/// without a phase-0 fence under short-txn flush).
fn run_async_prelock_catchup(
    table_oid: pgrx::pg_sys::Oid,
    prune_max_seq: i64,
) -> Result<Option<crate::mirror::apply::AppliedWalBoundary>, String> {
    use crate::mirror::apply::{apply_bounded_locked, BoundedApplyRequest, PruneSeqFloor};

    if prune_max_seq <= 0 {
        return Ok(None);
    }

    let max_passes = crate::guc::flush_prelock_max_passes();
    let max_ms = crate::guc::flush_prelock_max_ms();
    let started = std::time::Instant::now();
    let mut skip_through: Option<crate::mirror::apply::AppliedWalBoundary> = None;

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
        let outcome = apply_bounded_locked(BoundedApplyRequest {
            upper_bound: Some(fence),
            skip_through,
            acknowledge_durable_checkpoint: false,
            advance_slot_on_empty: false,
            target_prune_floor: Some((table_oid.to_u32(), PruneSeqFloor::new(prune_max_seq))),
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

/// Phase-6 async prune fence: slot lock already held; take short source lock,
/// catch mirror up through a durable WAL upper bound, then prune safely.
fn run_async_prune_fence(
    table_oid: pgrx::pg_sys::Oid,
    prune_max_seq: i64,
    skip_through: Option<crate::mirror::apply::AppliedWalBoundary>,
) -> Result<(), String> {
    use crate::mirror::apply::{apply_bounded_locked, BoundedApplyRequest, PruneSeqFloor};

    if prune_max_seq <= 0 {
        return Ok(());
    }

    // Lock order: session table job (held) → slot (held by caller) → source table.
    crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::BeforeSourceLock)?;
    lock_source_table_share_row_exclusive(table_oid)?;
    crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterSourceLock)?;
    let fence = capture_durable_wal_fence()?;
    pgrx::log!(
        "koldstore flush: async prune fence upto_lsn={} skip_through={:?} floor={}",
        koldstore_common::format_pg_lsn(fence.get()),
        skip_through.map(|lsn| koldstore_common::format_pg_lsn(lsn.get())),
        prune_max_seq
    );
    let outcome = apply_bounded_locked(BoundedApplyRequest {
        upper_bound: Some(fence),
        skip_through,
        acknowledge_durable_checkpoint: false,
        advance_slot_on_empty: false,
        target_prune_floor: Some((table_oid.to_u32(), PruneSeqFloor::new(prune_max_seq))),
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

/// Operator-facing `flush_table` result (JSON). Queue mode returns as soon as the
/// job is durable; completion / storage errors appear later in `koldstore.jobs`
/// and PostgreSQL WARNING logs.
#[derive(Debug, Clone)]
pub(crate) struct FlushTableResponse {
    pub ok: bool,
    pub job_id: Option<pgrx::Uuid>,
    pub status: &'static str,
    pub force: bool,
    pub execution: &'static str,
    pub reason: Option<String>,
    pub error: Option<String>,
    pub rows_flushed: Option<i64>,
    pub estimated_rows: Option<i64>,
}

impl FlushTableResponse {
    pub(crate) fn to_jsonb(&self) -> pgrx::JsonB {
        pgrx::JsonB(serde_json::json!({
            "ok": self.ok,
            "job_id": self.job_id.map(|id| id.to_string()),
            "status": self.status,
            "force": self.force,
            "execution": self.execution,
            "reason": self.reason,
            "error": self.error,
            "rows_flushed": self.rows_flushed,
            "estimated_rows": self.estimated_rows,
        }))
    }

    fn not_due(force: bool, execution: &'static str) -> Self {
        Self {
            ok: true,
            job_id: None,
            status: "not_due",
            force,
            execution,
            reason: Some(
                "no flush work due (policy selection empty, or excess below max_rows_per_file)"
                    .to_string(),
            ),
            error: None,
            rows_flushed: None,
            estimated_rows: Some(0),
        }
    }
}

pub(crate) fn flush_table_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> Result<FlushTableResponse, String> {
    let execution = match crate::guc::flush_execution_mode() {
        crate::settings::FlushExecutionMode::Inline => "inline",
        crate::settings::FlushExecutionMode::Queue => "queue",
    };
    match crate::guc::flush_execution_mode() {
        crate::settings::FlushExecutionMode::Inline => {
            // Try-lock *before* enqueue. Nested inline holds an open transaction
            // across claim→work, so the jobs row stays locked; enqueue-first would
            // block forever on the unique active-flush index / row instead of
            // failing fast with "flush already in progress".
            let table_lock = try_acquire_flush_table_lock(table_oid)?;
            let Some(job_uuid) =
                crate::sql::flush::jobs::enqueue_flush_job_if_due(table_oid, force)
                    .map_err(|error| error.to_string())?
            else {
                drop(table_lock);
                return Ok(FlushTableResponse::not_due(force, execution));
            };
            match flush_table_with_session_lock(table_oid, force, table_lock) {
                Ok(_) => {
                    let summary = load_flush_job_summary(job_uuid)?;
                    Ok(FlushTableResponse {
                        ok: summary.status != "error",
                        job_id: Some(job_uuid),
                        status: if summary.status == "error" {
                            "error"
                        } else {
                            "completed"
                        },
                        force,
                        execution,
                        reason: None,
                        error: summary.error_trace,
                        rows_flushed: Some(summary.rows_flushed),
                        estimated_rows: Some(summary.progress_total),
                    })
                }
                Err(error) => {
                    pgrx::warning!(
                        "koldstore flush_table: inline flush failed table_oid={} job={} error={error}",
                        table_oid.to_u32(),
                        job_uuid
                    );
                    Ok(FlushTableResponse {
                        ok: false,
                        job_id: Some(job_uuid),
                        status: "error",
                        force,
                        execution,
                        reason: None,
                        error: Some(error),
                        rows_flushed: None,
                        estimated_rows: None,
                    })
                }
            }
        }
        crate::settings::FlushExecutionMode::Queue => {
            // Probe ownership without blocking on Nested jobs-row / unique-index
            // waits. If another backend holds the session table lock, either
            // return the committed active job UUID or fail fast.
            if !crate::sql::job_lock::try_lock_table_job(table_oid)? {
                if let Some(existing) =
                    crate::sql::flush::jobs::lookup_active_flush_job_uuid(table_oid)
                        .map_err(|error| error.to_string())?
                {
                    return Ok(FlushTableResponse {
                        ok: true,
                        job_id: Some(existing),
                        status: "already_running",
                        force,
                        execution,
                        reason: Some(
                            "another backend already holds the flush lock; returning active job"
                                .to_string(),
                        ),
                        error: None,
                        rows_flushed: None,
                        estimated_rows: None,
                    });
                }
                return Err(flush_already_in_progress_message(table_oid));
            }
            // Successful try-lock means no live executor owns this table. Reclaim
            // any durable orphan `running` row before releasing the lock; queue
            // enqueue alone only force-upgrades / inserts and would leave the
            // orphan `running` forever (e2e wait-budget hang).
            crate::sql::flush::jobs::reclaim_orphan_running_under_lock(table_oid)
                .map_err(|error| error.to_string())?;
            // Queue callers must not keep the session lock; one-shot executors
            // claim it for the real attempt.
            crate::sql::job_lock::unlock_table_job(table_oid)?;
            let Some(job_uuid) =
                crate::sql::flush::jobs::enqueue_flush_job_if_due(table_oid, force)
                    .map_err(|error| error.to_string())?
            else {
                return Ok(FlushTableResponse::not_due(force, execution));
            };
            // Foreground backends never register or wait for workers. Enqueue
            // marks FLUSH_QUEUE_DIRTY transaction-locally and COMMIT publishes a
            // generation to the static supervisor. If registration is under
            // pressure the durable job remains pending and supervisor backoff
            // retries without delaying this client transaction.
            let estimate = super::spi::flush_progress_total_estimate(table_oid, force).ok();
            Ok(FlushTableResponse {
                ok: true,
                job_id: Some(job_uuid),
                status: "queued",
                force,
                execution,
                reason: Some(
                    "job durably enqueued; supervisor dispatches a one-shot executor after commit"
                        .to_string(),
                ),
                error: None,
                rows_flushed: None,
                estimated_rows: estimate,
            })
        }
    }
}

struct FlushJobSummary {
    status: String,
    rows_flushed: i64,
    progress_total: i64,
    error_trace: Option<String>,
}

fn load_flush_job_summary(job_id: pgrx::Uuid) -> Result<FlushJobSummary, String> {
    use pgrx::datum::DatumWithOid;

    pgrx::Spi::connect(|client| {
        let table = client
            .select(
                "SELECT status, rows_flushed, progress_total, error_trace \
                 FROM koldstore.jobs WHERE id = $1::uuid LIMIT 1",
                Some(1),
                &[DatumWithOid::from(job_id)],
            )
            .map_err(|error| error.to_string())?;
        let first = table.first();
        let status = first
            .get::<String>(1)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("flush job {job_id} not found after flush"))?;
        let rows_flushed = first
            .get::<i64>(2)
            .map_err(|error| error.to_string())?
            .unwrap_or(0);
        let progress_total = first
            .get::<i64>(3)
            .map_err(|error| error.to_string())?
            .unwrap_or(0);
        let error_trace = first
            .get::<String>(4)
            .map_err(|error| error.to_string())?
            .filter(|value| !value.is_empty());
        Ok(FlushJobSummary {
            status,
            rows_flushed,
            progress_total,
            error_trace,
        })
    })
}

fn flush_already_in_progress_message(table_oid: pgrx::pg_sys::Oid) -> String {
    let table = crate::catalog::resolve::qualified_relation_name(table_oid)
        .unwrap_or_else(|_| format!("oid={}", table_oid.to_u32()));
    format!(
        "flush already in progress for {table}; retry after it completes \
         (background auto-flush may run right after server start)"
    )
}

/// Runs flush while already holding the session table-job lock (executor / inline).
///
/// Does **not** hold the slot/apply lock during Parquet encode or object upload.
/// Slot lock is acquired only inside finalize fence paths via try-lock + retry.
///
/// Uses [`FlushCommitStyle::Nested`]: claim + work stay in the caller's
/// transaction (required for `#[pg_test]` / inline mode).
pub(crate) fn flush_table_with_session_lock(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
    table_lock: crate::sql::job_lock::TableJobLockGuard,
) -> Result<pgrx::Uuid, String> {
    let _table_lock = table_lock;
    let table = crate::catalog::resolve::qualified_relation_name(table_oid)
        .unwrap_or_else(|_| format!("oid={}", table_oid.to_u32()));
    let claimed = claim_flush_job_for_executor(table_oid, force)?;
    run_flush_after_claim(table_oid, &table, claimed, FlushCommitStyle::Nested)
}

/// Claimed flush job identity returned by [`claim_flush_job`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct ClaimedFlushJob {
    pub job_id: uuid::Uuid,
    pub attempt_token: uuid::Uuid,
    pub force: bool,
    pub progress_total: i64,
    /// Fixed job watermark; `None` when unset (no mirror rows at claim).
    pub target_seq: Option<SeqId>,
}

/// Claims (or resumes) the durable flush job under an already-held session lock.
pub(crate) fn claim_flush_job_for_executor(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> Result<ClaimedFlushJob, String> {
    let (job_id, attempt_token, force, progress_total, target_seq) =
        claim_flush_job(table_oid, force)?;
    Ok(ClaimedFlushJob {
        job_id,
        attempt_token,
        force,
        progress_total,
        target_seq,
    })
}

/// Continues a previously claimed flush (separate transaction from claim).
///
/// Uses [`FlushCommitStyle::Short`]: catalog SPI boundaries open/commit via
/// [`crate::worker::txn`]; object upload runs outside any PostgreSQL txn.
pub(crate) fn run_claimed_flush_with_session_lock(
    table_oid: pgrx::pg_sys::Oid,
    table_lock: crate::sql::job_lock::TableJobLockGuard,
    claimed: ClaimedFlushJob,
) -> Result<pgrx::Uuid, String> {
    let _table_lock = table_lock;
    // Claim already committed; open a short txn for catalog SPI (relcache asserts
    // if SPI runs with no active transaction in a BGWorker).
    let table = FlushCommitStyle::Short
        .run_spi(|| crate::catalog::resolve::qualified_relation_name(table_oid))
        .unwrap_or_else(|_| format!("oid={}", table_oid.to_u32()));
    run_flush_after_claim(table_oid, &table, claimed, FlushCommitStyle::Short)
}

fn run_flush_after_claim(
    table_oid: pgrx::pg_sys::Oid,
    table: &str,
    claimed: ClaimedFlushJob,
    commit_style: FlushCommitStyle,
) -> Result<pgrx::Uuid, String> {
    let ClaimedFlushJob {
        job_id,
        attempt_token,
        force,
        progress_total,
        target_seq,
    } = claimed;
    let job_uuid = crate::spi::uuid_to_pgrx(job_id);
    let started = std::time::Instant::now();
    pgrx::log!(
        "koldstore flush: started table={table} job={job_id} attempt={attempt_token} force={force} estimated_rows={progress_total} target_seq={:?}",
        target_seq.map(SeqId::get)
    );
    match flush_after_claim(table_oid, &claimed, started, table, commit_style) {
        Ok(()) => Ok(job_uuid),
        Err(error) => {
            // WARNING so Docker Desktop / default log viewers surface the
            // failure (LOG is easy to miss). Job row also keeps error_trace.
            pgrx::warning!(
                "koldstore flush: FAILED table={table} job={job_id} attempt={attempt_token} duration={} error={error}",
                format_flush_duration(started)
            );
            // Queue executors retry with backoff; inline/pg_test stays terminal.
            let retryable = matches!(commit_style, FlushCommitStyle::Short);
            commit_style.run_spi(|| {
                mark_flush_job_failed(job_id, table_oid, attempt_token, &error, retryable)
                    .map_err(|err| err.to_string())?;
                // Broadcast under the same short txn; CacheInvalidate asserts
                // IsTransactionState (queue executor is otherwise idle).
                crate::catalog::cache::invalidate_table_globally(table_oid);
                Ok(())
            })?;
            Ok(job_uuid)
        }
    }
}

fn flush_after_claim(
    table_oid: pgrx::pg_sys::Oid,
    claimed: &ClaimedFlushJob,
    started: std::time::Instant,
    table: &str,
    commit_style: FlushCommitStyle,
) -> Result<(), String> {
    let ClaimedFlushJob {
        job_id,
        attempt_token,
        force,
        progress_total,
        target_seq,
    } = *claimed;
    let mut ctx = commit_style
        .run_spi(|| load_flush_prepared_context(table_oid, force, job_id, attempt_token))?;
    if commit_style.run_spi(|| crate::sql::migrate::refresh_active_schema_if_changed(table_oid))? {
        ctx = commit_style
            .run_spi(|| load_flush_prepared_context(table_oid, force, job_id, attempt_token))?;
    }
    flush_prepared_table(
        table_oid,
        &ctx,
        progress_total,
        target_seq,
        started,
        table,
        commit_style,
    )
}

/// Cap catch-up passes inside one flush job (each pass ≤ `max_rows_per_flush`).
///
/// 64 × 10_000 default rows covers a 640k hot backlog in a single scheduler tick
/// / `flush_table` call without leaving hundreds of tiny completed job rows.
const MAX_CATCHUP_PASSES_PER_JOB: u32 = 64;

fn format_flush_duration(started: std::time::Instant) -> String {
    let elapsed = started.elapsed();
    if elapsed.as_secs() >= 1 {
        format!("{:.3}s", elapsed.as_secs_f64())
    } else {
        format!("{}ms", elapsed.as_millis())
    }
}

fn format_flush_bytes(bytes: i64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let bytes = u64::try_from(bytes.max(0)).unwrap_or(0);
    let bytes_f = bytes as f64;
    if bytes_f >= GB {
        format!("{bytes} ({:.1} GB)", bytes_f / GB)
    } else if bytes_f >= MB {
        format!("{bytes} ({:.1} MB)", bytes_f / MB)
    } else if bytes_f >= KB {
        format!("{bytes} ({:.1} kB)", bytes_f / KB)
    } else {
        format!("{bytes} bytes")
    }
}

/// Fails the attempt when wall-clock budget is exceeded (`0` disables).
fn ensure_flush_runtime_budget(started: std::time::Instant) -> Result<(), String> {
    let max_secs = crate::guc::flush_job_max_runtime_seconds();
    if max_secs <= 0 {
        return Ok(());
    }
    let elapsed = started.elapsed().as_secs();
    let budget = u64::try_from(max_secs).unwrap_or(u64::MAX);
    if elapsed >= budget {
        return Err(format!(
            "flush job exceeded koldstore.flush_job_max_runtime_seconds ({max_secs}); \
             elapsed={elapsed}s"
        ));
    }
    Ok(())
}

fn flush_prepared_table(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
    progress_total: i64,
    target_seq: Option<SeqId>,
    started: std::time::Instant,
    table: &str,
    commit_style: FlushCommitStyle,
) -> Result<(), String> {
    let mut total_rows_flushed = 0_i64;
    let mut total_batches = 0_i32;
    let mut total_bytes_written = 0_i64;
    let mut last_max_seq: Option<SeqId> = None;
    let mut passes = 0_u32;
    // Drain committed WAL into the mirror before fixing/reading the watermark so
    // enqueue-and-run flush sees rows that landed after the last apply tick.
    commit_style.run_spi(|| catch_up_mirror_before_select(commit_style))?;
    // Fixed watermark from claim — newer rows remain hot for a later job.
    let catchup_upto_seq = match target_seq {
        Some(seq) => Some(seq.get()),
        None => commit_style.run_spi(|| mirror_catchup_watermark(table_oid))?,
    };

    // One client per job: reused across passes for segment publish + manifest write.
    let client = crate::object_store::open_managed_object_store_client(
        &ctx.storage.storage_type,
        &ctx.storage.base_path,
        &ctx.storage.credentials,
        &ctx.storage.config,
    )
    .map_err(|error| error.to_string())?;

    let report_progress = |phase: &str,
                           rows_flushed: i64,
                           batches_completed: i32,
                           checkpoint_seq: Option<SeqId>|
     -> Result<(), String> {
        update_flush_job_progress(
            ctx.job_id,
            table_oid,
            FlushJobProgressUpdate {
                attempt_token: ctx.attempt_token,
                rows_flushed,
                batches_completed,
                checkpoint_seq,
                phase,
                progress_total,
            },
        )
        .map_err(Into::into)
    };

    loop {
        ensure_flush_runtime_budget(started)?;
        if commit_style
            .run_spi(|| flush_cancel_requested(ctx.job_id, table_oid).map_err(Into::into))?
        {
            return finish_flush_after_cancel(
                ctx.job_id,
                ctx.attempt_token,
                table_oid,
                table,
                started,
                total_rows_flushed,
                total_batches,
                total_bytes_written,
                last_max_seq,
                passes,
                commit_style,
            );
        }
        commit_style.run_spi(|| {
            report_progress(
                flush_phase::SELECTING,
                total_rows_flushed,
                total_batches,
                last_max_seq,
            )
        })?;
        // Keep selection current with committed WAL; release slot lock before encode.
        commit_style.run_spi(|| catch_up_mirror_before_select(commit_style))?;
        let selection = commit_style.run_spi(|| resolve_flush_stats(table_oid, ctx.force))?;
        commit_style.run_spi(|| {
            crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterSelectRows)
        })?;
        // Re-check after the select barrier so peer cancel/DROP can stop work
        // before object writes begin.
        ensure_flush_runtime_budget(started)?;
        if commit_style
            .run_spi(|| flush_cancel_requested(ctx.job_id, table_oid).map_err(Into::into))?
        {
            release_flush_memory(table_oid);
            return finish_flush_after_cancel(
                ctx.job_id,
                ctx.attempt_token,
                table_oid,
                table,
                started,
                total_rows_flushed,
                total_batches,
                total_bytes_written,
                last_max_seq,
                passes,
                commit_style,
            );
        }
        if !should_start_catchup_pass(
            catchup_upto_seq,
            selection.stats.row_count,
            selection.stats.min_seq,
        ) {
            break;
        }
        passes = passes.saturating_add(1);
        pgrx::log!(
            "koldstore flush: starting table={table} pass={passes} rows={} max_seq={} force={} catchup_upto={:?}",
            selection.stats.row_count,
            selection.stats.max_seq,
            ctx.force,
            catchup_upto_seq
        );
        commit_style.run_spi(|| {
            report_progress(
                flush_phase::WRITING,
                total_rows_flushed,
                total_batches,
                last_max_seq,
            )
        })?;
        // Pass: select already done → upload/persist batches (commits between
        // SPI phases when Short) → manifest+finalize short txn → continue.
        let streamed =
            stream_write_flush_batches(table_oid, ctx, &selection, &client, commit_style, started)?;
        let pass_batches =
            i32::try_from(streamed.pending_segment_ids.len()).map_err(|error| error.to_string())?;
        // Cooperative cancel / runtime budget before publish: do not activate this pass.
        ensure_flush_runtime_budget(started)?;
        if commit_style
            .run_spi(|| flush_cancel_requested(ctx.job_id, table_oid).map_err(Into::into))?
        {
            drop(streamed);
            release_flush_memory(table_oid);
            return finish_flush_after_cancel(
                ctx.job_id,
                ctx.attempt_token,
                table_oid,
                table,
                started,
                total_rows_flushed,
                total_batches,
                total_bytes_written,
                last_max_seq,
                passes,
                commit_style,
            );
        }
        commit_style.run_spi(|| {
            report_progress(
                flush_phase::ACTIVATING,
                total_rows_flushed,
                total_batches,
                last_max_seq,
            )
        })?;
        let outcome = build_manifest_and_finalize(table_oid, ctx, streamed, &client, commit_style)?;

        total_rows_flushed = total_rows_flushed.saturating_add(outcome.total_rows_flushed);
        total_batches = total_batches.saturating_add(pass_batches);
        total_bytes_written = total_bytes_written.saturating_add(outcome.bytes_written);
        last_max_seq = SeqId::new(outcome.last_max_seq).ok().or(last_max_seq);

        // Drop pass-owned buffers before the next selection / encode pass.
        drop(outcome);
        release_flush_memory(table_oid);

        commit_style.run_spi(|| {
            report_progress(
                flush_phase::WRITING,
                total_rows_flushed,
                total_batches,
                last_max_seq,
            )
        })?;

        // Policy and force passes are both row-capped; keep draining the pinned
        // start-of-job watermark (not rows applied during this flush).
        let more_passes = passes < MAX_CATCHUP_PASSES_PER_JOB
            && should_continue_flush_catchup(
                catchup_upto_seq,
                last_max_seq.map(SeqId::get).unwrap_or(0),
            );
        if !more_passes {
            break;
        }
        commit_style.run_spi(|| {
            crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterPassProgress)
        })?;
    }

    commit_style.run_spi(|| {
        mark_flush_job_completed(
            ctx.job_id,
            table_oid,
            ctx.attempt_token,
            total_rows_flushed,
            last_max_seq,
            total_batches,
        )
        .map_err(Into::into)
    })?;
    pgrx::log!(
        "koldstore flush: completed table={table} job={} attempt={} duration={} rows={} segments={} bytes={} pass={} max_seq={} force={}",
        ctx.job_id,
        ctx.attempt_token,
        format_flush_duration(started),
        total_rows_flushed,
        total_batches,
        format_flush_bytes(total_bytes_written),
        passes,
        last_max_seq.map(SeqId::get).unwrap_or(0),
        ctx.force
    );
    release_flush_memory(table_oid);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn finish_flush_after_cancel(
    job_id: uuid::Uuid,
    attempt_token: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
    table: &str,
    started: std::time::Instant,
    total_rows_flushed: i64,
    total_batches: i32,
    total_bytes_written: i64,
    last_max_seq: Option<SeqId>,
    passes: u32,
    commit_style: FlushCommitStyle,
) -> Result<(), String> {
    if total_rows_flushed > 0 {
        // Publish already committed in an earlier pass of this statement: do not
        // pretend cold data was unpublished.
        commit_style.run_spi(|| {
            mark_flush_job_completed_after_cancel(
                job_id,
                table_oid,
                attempt_token,
                total_rows_flushed,
                last_max_seq,
                total_batches,
            )
            .map_err(Into::into)
        })?;
        pgrx::log!(
            "koldstore flush: cancelled-after-progress table={table} job={job_id} attempt={attempt_token} duration={} rows={} segments={} bytes={} pass={} max_seq={}",
            format_flush_duration(started),
            total_rows_flushed,
            total_batches,
            format_flush_bytes(total_bytes_written),
            passes,
            last_max_seq.map(SeqId::get).unwrap_or(0)
        );
    } else {
        commit_style.run_spi(|| {
            mark_flush_job_cancelled(job_id, table_oid, attempt_token).map_err(Into::into)
        })?;
        pgrx::log!(
            "koldstore flush: cancelled table={table} job={job_id} attempt={attempt_token} duration={}",
            format_flush_duration(started)
        );
    }
    release_flush_memory(table_oid);
    Ok(())
}
