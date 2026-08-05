//! Flush orchestration for `koldstore.flush_table`.
//!
//! Owns PostgreSQL-specific job locking and SPI wiring. Flush workflow logic
//! lives in `koldstore-flush`.

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

pub(super) struct FlushPreparedContext {
    job_id: uuid::Uuid,
    attempt_token: uuid::Uuid,
    force: bool,
    relation: RelationContext,
    storage: FlushStorageContext,
    snapshot: Arc<ManagedTableSnapshot>,
    catalog: Arc<koldstore_migrate::ExistingTableCatalog>,
    indexed_columns: Vec<ColumnRef>,
    max_rows_per_file: usize,
    target_file_size_bytes: Option<u64>,
}

/// Acquires the session table-job lock without waiting.
///
/// Manual `flush_table` must not block behind auto-flush — those can hold the
/// table lock for a long time with no client-visible progress. Scheduler ticks
/// already skip via try-lock; SQL callers get a clear error instead.
fn try_acquire_flush_table_lock(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<crate::sql::job_lock::TableJobLockGuard, String> {
    let table = crate::catalog::resolve::qualified_relation_name(table_oid)
        .unwrap_or_else(|_| format!("oid={}", table_oid.to_u32()));
    crate::sql::job_lock::TableJobLockGuard::try_lock(table_oid)?.ok_or_else(|| {
        format!(
            "flush already in progress for {table}; retry after it completes \
             (background auto-flush may run right after server start)"
        )
    })
}

fn claim_flush_job(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> Result<(uuid::Uuid, uuid::Uuid, bool, i64, Option<SeqId>), String> {
    // Caller already holds the session table-job lock — do not re-lock (that
    // would bump the session lock count and require a matching extra unlock).
    let (job_id, force) = ensure_flush_job(table_oid, force)?;
    // Fixed watermark + progress estimate at claim (newer rows stay hot).
    let progress_total = super::spi::mirror_catchup_row_estimate(table_oid)?;
    let target_seq = super::spi::mirror_catchup_watermark(table_oid)?
        .and_then(|seq| SeqId::new(seq).ok());
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
    let min_floor = u64::try_from(crate::guc::min_max_rows_per_file())
        .unwrap_or(koldstore_common::DEFAULT_MIN_MAX_ROWS_PER_FILE);
    let options = super::spi::active_manage_options(table_oid)?.unwrap_or_default();
    let policy = options.flush_policy();
    let configured = policy.as_ref().map(FlushPolicy::max_rows_per_file);
    if let Some(value) = configured {
        let hint = format!(
            "lower the floor for testing with SET {} = <value>",
            crate::settings::MIN_MAX_ROWS_PER_FILE_GUC
        );
        koldstore_common::validate_max_rows_per_file(value, min_floor, Some(&hint))?;
    }
    let max_rows_per_file = max_rows_per_file_from_policy(configured, min_floor)?;
    let target_file_size_bytes = options
        .target_file_size_mb
        .map(|megabytes| {
            megabytes
                .checked_mul(1024 * 1024)
                .ok_or_else(|| format!("target_file_size_mb {megabytes} is too large"))
        })
        .transpose()?;
    Ok(FlushPreparedContext {
        job_id,
        attempt_token,
        force,
        relation,
        storage,
        snapshot,
        catalog,
        indexed_columns,
        max_rows_per_file,
        target_file_size_bytes,
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

fn stream_write_flush_batches(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
    selection: &ResolvedFlushSelection,
    client: &koldstore_storage::ObjectStoreClient,
) -> Result<TableFlushBatchOutcome, String> {
    let stats = &selection.stats;
    let table_prefix = render_regular_table_prefix(
        &PathTemplate::new(&ctx.storage.regular_path_tmpl),
        &ctx.relation.namespace,
        &ctx.relation.name,
    )?;
    let manifest_path = manifest_object_key(&table_prefix);
    let schema_version =
        u32::try_from(ctx.storage.schema_version).map_err(|error| error.to_string())?;
    let mut batch_number = next_flush_batch_number(table_oid)?;
    let pass_id = uuid::Uuid::new_v4();
    let mut total_rows_flushed = 0_i64;
    let mut last_max_seq = 0_i64;
    let mut pending_segments: Vec<PendingFlushSegmentRef> = Vec::new();

    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table = QualifiedTableName::parse(&relation).map_err(|error| error.to_string())?;
    let mirror = QualifiedTableName::from_table_name(&ctx.snapshot.mirror_relation);
    let encode_input = StreamEncodeInput {
        table,
        mirror,
        primary_key_columns: ctx
            .snapshot
            .primary_key_names()
            .map(str::to_string)
            .collect(),
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
        schema_version,
        max_seq: stats.max_seq,
        max_rows_per_file: ctx.max_rows_per_file,
        fetch_batch_size: flush_mirror_fetch_limit(ctx.max_rows_per_file),
        target_file_size_bytes: ctx.target_file_size_bytes,
        compression: ctx.storage.compression.clone(),
        row_group_size: koldstore_parquet::WriterOptions::default().row_group_size,
        mirror_ops: selection.mirror_ops.clone(),
        sort_by_order_key: ctx.snapshot.segment_order_column_id.is_some(),
    };
    let catalog_columns = &ctx.catalog.columns;
    let fetch_batch_size = encode_input.fetch_batch_size;
    // Failpoint after pending catalog inserts lives in write_streamed_chunk.

    let stream_outcome = crate::merge_scan::pg::with_custom_scan_disabled(|| {
        stream_flush_chunks(
            &encode_input,
            |statement, max_seq, after_seq| {
                fetch_mirror_batch(
                    catalog_columns,
                    statement,
                    max_seq,
                    after_seq,
                    fetch_batch_size,
                    ctx.snapshot.segment_order_column_id.is_some(),
                )
            },
            |chunk| {
                write_streamed_chunk(
                    client,
                    ctx,
                    pass_id,
                    &table_prefix,
                    table_oid,
                    &mut batch_number,
                    &mut total_rows_flushed,
                    &mut last_max_seq,
                    &mut pending_segments,
                    chunk,
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
    // PERFORMANCE: catalog is the source of truth for publishable segments.
    // Build the derived manifest once after pending inserts — skip object-store
    // reload + in-memory append that duplicated large JSON each pass.
    let manifest = manifest_from_publishable_cold_segments(
        table_oid,
        &ctx.relation,
        &ctx.snapshot,
        ctx.storage.schema_version,
    )?;

    Ok(TableFlushBatchOutcome {
        total_rows_flushed,
        last_max_seq,
        bytes_written,
        mirror_ops: selection.mirror_ops.clone(),
        prune_max_seq: stream_outcome.max_seq,
        manifest,
        manifest_path,
        pending_segment_ids,
    })
}

#[allow(clippy::too_many_arguments)]
fn write_streamed_chunk(
    client: &koldstore_storage::ObjectStoreClient,
    ctx: &FlushPreparedContext,
    pass_id: uuid::Uuid,
    table_prefix: &str,
    table_oid: pgrx::pg_sys::Oid,
    batch_number: &mut i32,
    total_rows_flushed: &mut i64,
    last_max_seq: &mut i64,
    pending_segments: &mut Vec<PendingFlushSegmentRef>,
    chunk: FlushWriteChunk,
) -> Result<(), String> {
    crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::DuringParquetWrite)?;
    let chunk_stats = FlushStats::from_write_chunk(&chunk)?;
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
    crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterTempObject)?;
    crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterChecksumMetadata)?;
    let byte_size = written.catalog_row.byte_size;
    let segment_id = written.segment_id;
    persist_flush_segment(
        table_oid,
        super::spi::FlushSegmentWriterIdentity {
            job_id: ctx.job_id,
            attempt_token: ctx.attempt_token,
            pass_id,
        },
        &written,
    )?;
    // Drop packed metadata + catalog row; retain only ids + byte sum.
    drop(written);
    crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterPendingSegment)?;
    *total_rows_flushed = total_rows_flushed.saturating_add(chunk_stats.row_count);
    *last_max_seq = chunk_stats.max_seq;
    pgrx::log!(
        "koldstore flush: wrote+cataloged segment batch={} rows={} bytes={} total_rows={}",
        *batch_number,
        chunk_stats.row_count,
        byte_size,
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
    phase0_applied: Option<crate::mirror::apply::AppliedWalBoundary>,
) -> Result<(), String> {
    // One critical section under try-lock slot ownership: prelock catch-up,
    // manifest write, activate, source fence, prune. Encode/upload already
    // finished without the slot lock.
    with_slot_lock_retry(|| {
        let skip_through =
            run_async_prelock_catchup(table_oid, outcome.prune_max_seq, phase0_applied)?;

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
        crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterCleanupBeforeJobComplete)?;
        crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterJobCompleteBeforeTempCleanup)?;
        Ok(())
    })
}

/// Tries the slot lock with bounded backoff; never blocks indefinitely.
///
/// Callers run finalize fence work while the lock is held. Nested apply uses
/// [`apply_bounded_locked`] so we do not depend on re-entrant blocking lock.
fn with_slot_lock_retry<T>(body: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    use crate::mirror::lifecycle::try_lock_apply;

    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32();
    const MAX_ATTEMPTS: u32 = 32;
    const SLEEP_MS: u64 = 25;
    for attempt in 1..=MAX_ATTEMPTS {
        crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::BeforeSlotLock)?;
        if try_lock_apply(database_oid)? {
            crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterSlotLock)?;
            return body();
        }
        if attempt == MAX_ATTEMPTS {
            break;
        }
        pgrx::log!(
            "koldstore flush: slot lock busy (attempt {attempt}/{MAX_ATTEMPTS}); retrying"
        );
        std::thread::sleep(std::time::Duration::from_millis(SLEEP_MS));
    }
    Err("flush finalize could not acquire slot lock before deadline".to_string())
}

/// Drops flush-scoped caches and asks the allocator to return free pages.
///
/// Call after each pass and again when the job finishes so large Parquet /
/// manifest allocations do not stay pinned in the backend RSS.
pub(crate) fn release_flush_memory(table_oid: pgrx::pg_sys::Oid) {
    crate::catalog::cache::invalidate_table_globally(table_oid);
    crate::memory::mark_heap_trim_pending();
    crate::memory::release_process_heap();
}

/// Phase-5.5: finite pre-lock catch-up after object upload.
///
/// Caller must hold the slot lock (see [`with_slot_lock_retry`]). Returns the
/// skip boundary for phase 6.
fn run_async_prelock_catchup(
    table_oid: pgrx::pg_sys::Oid,
    prune_max_seq: i64,
    phase0_applied: Option<crate::mirror::apply::AppliedWalBoundary>,
) -> Result<Option<crate::mirror::apply::AppliedWalBoundary>, String> {
    use crate::mirror::apply::{apply_bounded_locked, BoundedApplyRequest, PruneSeqFloor};

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
        let outcome = apply_bounded_locked(BoundedApplyRequest {
            upper_bound: Some(fence),
            skip_through,
            acknowledge_durable_checkpoint: false,
            advance_slot_on_empty: false,
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
    // Durable queue first: return the active job UUID even when an executor is
    // already running (or when queue mode only spawns work).
    let job_uuid = crate::sql::flush::jobs::enqueue_or_lookup_flush_job(table_oid, force)
        .map_err(|error| error.to_string())?;

    match crate::guc::flush_execution_mode() {
        crate::settings::FlushExecutionMode::Inline => {
            let table_lock = try_acquire_flush_table_lock(table_oid)?;
            flush_table_with_session_lock(table_oid, force, table_lock)?;
        }
        crate::settings::FlushExecutionMode::Queue => {
            // Best-effort: executor connects after this transaction commits.
            if let Err(error) = crate::worker::spawn_flush_executor_if_needed() {
                pgrx::warning!("koldstore flush_table: could not spawn flush executor: {error}");
            }
        }
    }
    Ok(job_uuid)
}

/// Runs flush while already holding the session table-job lock (executor / inline).
///
/// Does **not** hold the slot/apply lock during Parquet encode or object upload.
/// Slot lock is acquired only inside finalize fence paths via try-lock + retry.
pub(crate) fn flush_table_with_session_lock(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
    table_lock: crate::sql::job_lock::TableJobLockGuard,
) -> Result<pgrx::Uuid, String> {
    let _table_lock = table_lock;
    let table = crate::catalog::resolve::qualified_relation_name(table_oid)
        .unwrap_or_else(|_| format!("oid={}", table_oid.to_u32()));
    let claimed = claim_flush_job_for_executor(table_oid, force)?;
    run_flush_after_claim(table_oid, &table, claimed)
}

/// Claimed flush job identity returned by [`claim_flush_job`].
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
pub(crate) fn run_claimed_flush_with_session_lock(
    table_oid: pgrx::pg_sys::Oid,
    table_lock: crate::sql::job_lock::TableJobLockGuard,
    claimed: ClaimedFlushJob,
) -> Result<pgrx::Uuid, String> {
    let _table_lock = table_lock;
    let table = crate::catalog::resolve::qualified_relation_name(table_oid)
        .unwrap_or_else(|_| format!("oid={}", table_oid.to_u32()));
    run_flush_after_claim(table_oid, &table, claimed)
}

fn run_flush_after_claim(
    table_oid: pgrx::pg_sys::Oid,
    table: &str,
    claimed: ClaimedFlushJob,
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
    match flush_after_claim(
        table_oid,
        force,
        job_id,
        attempt_token,
        progress_total,
        target_seq,
        None, // no phase-0 apply; finalize fences under try_lock_slot
        started,
        table,
    ) {
        Ok(()) => Ok(job_uuid),
        Err(error) => {
            pgrx::log!(
                "koldstore flush: failed table={table} job={job_id} attempt={attempt_token} duration={} error={error}",
                format_flush_duration(started)
            );
            mark_flush_job_failed(job_id, table_oid, attempt_token, &error)?;
            crate::catalog::cache::invalidate_table_globally(table_oid);
            Ok(job_uuid)
        }
    }
}

fn flush_after_claim(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
    job_id: uuid::Uuid,
    attempt_token: uuid::Uuid,
    progress_total: i64,
    target_seq: Option<SeqId>,
    phase0_applied: Option<crate::mirror::apply::AppliedWalBoundary>,
    started: std::time::Instant,
    table: &str,
) -> Result<(), String> {
    let mut ctx = load_flush_prepared_context(table_oid, force, job_id, attempt_token)?;
    match crate::sql::migrate::refresh_active_schema_if_changed(table_oid) {
        Ok(true) => {
            ctx = load_flush_prepared_context(table_oid, force, job_id, attempt_token)?;
        }
        Ok(false) => {}
        Err(error) => return Err(error),
    }
    flush_prepared_table(
        table_oid,
        &ctx,
        progress_total,
        target_seq,
        phase0_applied,
        started,
        table,
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

fn flush_prepared_table(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
    progress_total: i64,
    target_seq: Option<SeqId>,
    phase0_applied: Option<crate::mirror::apply::AppliedWalBoundary>,
    started: std::time::Instant,
    table: &str,
) -> Result<(), String> {
    let mut total_rows_flushed = 0_i64;
    let mut total_batches = 0_i32;
    let mut total_bytes_written = 0_i64;
    let mut last_max_seq: Option<SeqId> = None;
    let mut skip_through = phase0_applied;
    let mut passes = 0_u32;
    // Fixed watermark from claim — newer rows remain hot for a later job.
    let catchup_upto_seq = match target_seq {
        Some(seq) => Some(seq.get()),
        None => mirror_catchup_watermark(table_oid)?,
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
        if flush_cancel_requested(ctx.job_id, table_oid)? {
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
            );
        }
        report_progress(
            flush_phase::SELECTING,
            total_rows_flushed,
            total_batches,
            last_max_seq,
        )?;
        let selection = resolve_flush_stats(table_oid, ctx.force)?;
        crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterSelectRows)?;
        // Re-check after the select barrier so peer cancel/DROP can stop work
        // before object writes begin.
        if flush_cancel_requested(ctx.job_id, table_oid)? {
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
        report_progress(
            flush_phase::WRITING,
            total_rows_flushed,
            total_batches,
            last_max_seq,
        )?;
        let outcome = stream_write_flush_batches(table_oid, ctx, &selection, &client)?;
        let pass_batches =
            i32::try_from(outcome.pending_segment_ids.len()).map_err(|error| error.to_string())?;
        // Cooperative cancel before publish: do not activate this pass.
        if flush_cancel_requested(ctx.job_id, table_oid)? {
            drop(outcome);
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
            );
        }
        report_progress(
            flush_phase::ACTIVATING,
            total_rows_flushed,
            total_batches,
            last_max_seq,
        )?;
        finalize_flush(table_oid, ctx, &outcome, &client, skip_through)?;
        // Later passes have no async phase-0 boundary to skip through.
        skip_through = None;

        total_rows_flushed = total_rows_flushed.saturating_add(outcome.total_rows_flushed);
        total_batches = total_batches.saturating_add(pass_batches);
        total_bytes_written = total_bytes_written.saturating_add(outcome.bytes_written);
        last_max_seq = SeqId::new(outcome.last_max_seq).ok().or(last_max_seq);

        // Drop pass-owned buffers before the next selection / encode pass.
        drop(outcome);
        release_flush_memory(table_oid);

        report_progress(
            flush_phase::WRITING,
            total_rows_flushed,
            total_batches,
            last_max_seq,
        )?;

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
        crate::failpoints::hit_typed(crate::failpoints::FlushFailpoint::AfterPassProgress)?;
    }

    mark_flush_job_completed(
        ctx.job_id,
        table_oid,
        ctx.attempt_token,
        total_rows_flushed,
        last_max_seq,
        total_batches,
    )?;
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
) -> Result<(), String> {
    if total_rows_flushed > 0 {
        // Publish already committed in an earlier pass of this statement: do not
        // pretend cold data was unpublished.
        mark_flush_job_completed_after_cancel(
            job_id,
            table_oid,
            attempt_token,
            total_rows_flushed,
            last_max_seq,
            total_batches,
        )?;
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
        mark_flush_job_cancelled(job_id, table_oid, attempt_token)?;
        pgrx::log!(
            "koldstore flush: cancelled table={table} job={job_id} attempt={attempt_token} duration={}",
            format_flush_duration(started)
        );
    }
    release_flush_memory(table_oid);
    Ok(())
}
