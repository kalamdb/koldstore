//! PostgreSQL SPI adapters for flush: stats, catalog writes, and cleanup.

use koldstore_catalog::{decode::RelationContext, ManagedTableSnapshot};
use koldstore_common::QualifiedTableName;
use koldstore_flush::policy::FlushPolicy;
use koldstore_flush::{
    cleanup::plan_seq_range_cleanup, manifest_from_catalog_rows, plan_activate_flush_segments,
    plan_flush_segments_batch_insert, policy_flush_row_count, CatalogManifestSegmentRow,
    FlushStats, ResolvedFlushSelection, WrittenFlushSegment,
};
use koldstore_mirror::{
    mirror_to_sql, plan_mirror_oldest_rows_max_seq, plan_mirror_op_stats, plan_mirror_stats,
    MirrorRelation, MirrorSeqStats,
};

pub(super) fn resolve_flush_stats(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> Result<ResolvedFlushSelection, String> {
    use koldstore_common::MirrorOperation;
    use koldstore_flush::{resolve_force_flush_selection, resolve_policy_flush_selection};

    if force {
        let all = mirror_flush_stats(table_oid)?;
        let delete_code = MirrorOperation::Delete.code();
        let delete_stats = mirror_op_stats(table_oid, delete_code)?;
        return Ok(resolve_force_flush_selection(all, delete_stats));
    }

    // PERFORMANCE: Prefer O(1) manifest counters over COUNT(*) on the mirror.
    let pending = mirror_pending_row_count(table_oid)?;
    let policy = active_flush_policy(table_oid)?;
    let cutoff = if pending == 0 {
        None
    } else if let Some(ref policy) = policy {
        let flush_count = policy_flush_row_count(pending, policy);
        if flush_count == 0 {
            None
        } else {
            Some(mirror_oldest_rows_cutoff(table_oid, flush_count)?)
        }
    } else {
        None
    };
    let full_mirror = if policy.is_none() && pending > 0 {
        mirror_flush_stats(table_oid)?
    } else {
        FlushStats::empty()
    };
    Ok(resolve_policy_flush_selection(
        pending,
        policy.as_ref(),
        cutoff,
        full_mirror,
    ))
}

pub(super) fn active_flush_policy(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<Option<FlushPolicy>, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_catalog::queries::plan_active_flush_policy_options()
        .map_err(|error| error.to_string())?;
    let options =
        crate::spi::select_one::<pgrx::JsonB>(&statement, &[DatumWithOid::from(table_oid)])
            .map_err(|error| error.to_string())?;
    let Some(options) = options else {
        return Ok(None);
    };
    Ok(koldstore_flush::policy::flush_policy_from_options(
        &options.0,
    ))
}

/// Returns the managed table's mirror capture mode (defaults to strict).
pub(super) fn active_mirror_capture_mode(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<koldstore_common::MirrorCaptureMode, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_catalog::queries::plan_active_flush_policy_options()
        .map_err(|error| error.to_string())?;
    let options =
        crate::spi::select_one::<pgrx::JsonB>(&statement, &[DatumWithOid::from(table_oid)])
            .map_err(|error| error.to_string())?;
    let Some(options) = options else {
        return Ok(koldstore_common::MirrorCaptureMode::Strict);
    };
    Ok(koldstore_common::ManageTableOptions::from_value(&options.0).mirror_capture_mode())
}

/// Blocks concurrent source DML for the async prune fence.
///
/// Uses `SHARE ROW EXCLUSIVE` so in-flight writers finish, new writers wait,
/// and ordinary `SELECT` continues. Sets a local `lock_timeout` so an idle
/// blocker fails the flush before prune rather than waiting forever.
pub(super) fn lock_source_table_share_row_exclusive(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<(), String> {
    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table = QualifiedTableName::parse(&relation).map_err(|error| error.to_string())?;
    let quoted = table.quoted();
    pgrx::Spi::connect_mut(|client| -> Result<(), String> {
        client
            .update("SET LOCAL lock_timeout = '30s'", None, &[])
            .map_err(|error| error.to_string())?;
        client
            .update(
                &format!("LOCK TABLE ONLY {quoted} IN SHARE ROW EXCLUSIVE MODE"),
                None,
                &[],
            )
            .map_err(|error| format!("async flush prune fence could not lock {quoted}: {error}"))?;
        Ok(())
    })
}

/// Captures the end of inserted WAL and forces it durable on disk.
///
/// Required so logical decoding with `upto_lsn = F1` can see commits that used
/// `synchronous_commit = off`.
///
/// Uses `XLogFlush` directly rather than SPI-polling `pg_current_wal_flush_lsn`
/// with `pg_sleep`: during `flush_table` the apply advisory lock blocks the
/// async worker, so that poll can sit for the full ~10s budget per flush.
///
/// The fence LSN must be the end of inserted WAL ([`inserted_wal_end_lsn`]), not
/// a raw [`GetXLogInsertRecPtr`]: at page boundaries the latter points past the
/// next page header and `XLogFlush` fails with "xlog flush request … is not
/// satisfied".
pub(super) fn capture_durable_wal_fence() -> Result<crate::async_mirror::apply::WalFenceLsn, String>
{
    let fence = inserted_wal_end_lsn();
    unsafe { pgrx::pg_sys::XLogFlush(fence) };
    Ok(crate::async_mirror::apply::WalFenceLsn::new(fence))
}

/// Latest inserted WAL end pointer that is safe to pass to [`XLogFlush`].
///
/// Prefer `GetXLogInsertEndRecPtr` when the running PostgreSQL exports it.
/// PG 16.13 does not; emulate the page-boundary correction instead.
fn inserted_wal_end_lsn() -> pgrx::pg_sys::XLogRecPtr {
    #[cfg(not(feature = "pg16"))]
    {
        unsafe { pgrx::pg_sys::GetXLogInsertEndRecPtr() }
    }
    #[cfg(feature = "pg16")]
    {
        // Same correction as GetXLogInsertEndRecPtr / XLogBytePosToEndRecPtr:
        // at a page boundary GetXLogInsertRecPtr sits just after the page header
        // (e.g. …/018 or …/028) while no WAL exists there yet.
        let insert = unsafe { pgrx::pg_sys::GetXLogInsertRecPtr() };
        let page_off = insert % u64::from(pgrx::pg_sys::XLOG_BLCKSZ);
        let short_phd = std::mem::size_of::<pgrx::pg_sys::XLogPageHeaderData>() as u64;
        let long_phd = std::mem::size_of::<pgrx::pg_sys::XLogLongPageHeaderData>() as u64;
        if page_off == short_phd || page_off == long_phd {
            insert - page_off
        } else {
            insert
        }
    }
}

pub(super) fn next_flush_batch_number(table_oid: pgrx::pg_sys::Oid) -> Result<i32, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_catalog::queries::plan_next_flush_batch_number()
        .map_err(|error| error.to_string())?;
    crate::spi::select_one::<i32>(&statement, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "batch number lookup returned no rows".to_string())
}

pub(super) fn publishable_cold_segment_count(table_oid: pgrx::pg_sys::Oid) -> Result<i64, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_catalog::queries::plan_publishable_cold_segment_count()
        .map_err(|error| error.to_string())?;
    crate::spi::select_one::<i64>(&statement, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "cold segment count lookup returned no rows".to_string())
}

pub(super) fn manifest_from_publishable_cold_segments(
    table_oid: pgrx::pg_sys::Oid,
    relation: &RelationContext,
    snapshot: &ManagedTableSnapshot,
    schema_version: i32,
) -> Result<koldstore_manifest::Manifest, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_catalog::queries::plan_publishable_cold_segments_for_manifest_json()
        .map_err(|error| error.to_string())?;
    let json = crate::spi::select_one::<String>(&statement, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?
        .unwrap_or_else(|| "[]".to_string());
    let rows: Vec<CatalogManifestSegmentRow> =
        serde_json::from_str(&json).map_err(|error| error.to_string())?;
    manifest_from_catalog_rows(
        &relation.namespace,
        &relation.name,
        u32::try_from(schema_version).map_err(|error| error.to_string())?,
        &snapshot.primary_key_columns,
        rows,
    )
    .map_err(|error| error.to_string())
}

pub(super) fn manifest_generation(table_oid: pgrx::pg_sys::Oid) -> Result<i64, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_catalog::queries::plan_manifest_generation()
        .map_err(|error| error.to_string())?;
    Ok(
        crate::spi::select_one::<i64>(&statement, &[DatumWithOid::from(table_oid)])
            .map_err(|error| error.to_string())?
            .unwrap_or(0),
    )
}

/// Catalogs every segment written by one `flush_table` call.
///
/// Segment rows + normalized `cold_segment_stats` go in one SPI round trip.
/// Exact per-PK catalog hints are intentionally not written: prune with
/// `cold_segment_stats` / Parquet stats so catalog size stays O(segments).
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared or SPI
/// execution fails.
pub(super) fn persist_flush_segments_batch(
    table_oid: pgrx::pg_sys::Oid,
    segments: &[WrittenFlushSegment],
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    if segments.is_empty() {
        return Ok(());
    }

    let mut segment_ids = Vec::with_capacity(segments.len());
    let mut object_paths = Vec::with_capacity(segments.len());
    let mut batch_numbers = Vec::with_capacity(segments.len());
    let mut min_seqs = Vec::with_capacity(segments.len());
    let mut max_seqs = Vec::with_capacity(segments.len());
    let mut min_commit_seqs = Vec::with_capacity(segments.len());
    let mut max_commit_seqs = Vec::with_capacity(segments.len());
    let mut row_counts = Vec::with_capacity(segments.len());
    let mut byte_sizes = Vec::with_capacity(segments.len());
    let mut schema_versions = Vec::with_capacity(segments.len());
    let mut column_stats = Vec::with_capacity(segments.len());
    let mut checksums = Vec::with_capacity(segments.len());
    let mut object_etags = Vec::with_capacity(segments.len());
    for segment in segments {
        let row = &segment.catalog_row;
        let segment_id = crate::spi::uuid_to_pgrx(segment.segment_id);
        segment_ids.push(segment_id);
        object_paths.push(row.object_path.clone());
        batch_numbers.push(row.batch_number);
        min_seqs.push(row.min_seq);
        max_seqs.push(row.max_seq);
        min_commit_seqs.push(row.min_commit_seq);
        max_commit_seqs.push(row.max_commit_seq);
        row_counts.push(row.row_count);
        byte_sizes.push(row.byte_size);
        schema_versions.push(row.schema_version);
        column_stats.push(pgrx::JsonB(row.column_stats.clone()));
        checksums.push(segment.checksum.clone());
        object_etags.push(segment.object_etag.clone().unwrap_or_default());
    }

    let statement = plan_flush_segments_batch_insert().map_err(|error| error.to_string())?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(segment_ids),
            DatumWithOid::from(object_paths),
            DatumWithOid::from(batch_numbers),
            DatumWithOid::from(min_seqs),
            DatumWithOid::from(max_seqs),
            DatumWithOid::from(min_commit_seqs),
            DatumWithOid::from(max_commit_seqs),
            DatumWithOid::from(row_counts),
            DatumWithOid::from(byte_sizes),
            DatumWithOid::from(schema_versions),
            DatumWithOid::from(column_stats),
            DatumWithOid::from(checksums),
            DatumWithOid::from(object_etags),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// Catalogs one written segment immediately (segment row + column stats).
///
/// Prefer this during streaming flush so catalog work tracks Parquet publish.
pub(super) fn persist_flush_segment(
    table_oid: pgrx::pg_sys::Oid,
    segment: &WrittenFlushSegment,
) -> Result<(), String> {
    persist_flush_segments_batch(table_oid, std::slice::from_ref(segment))
}

/// Activates pending flush segments and CAS-bumps `manifest.generation`.
///
/// Catalog-only: does not re-read object bodies. Returns the new generation.
///
/// # Errors
///
/// Returns an error when CAS misses (generation conflict) or SPI fails.
pub(super) fn activate_flush_segments(
    table_oid: pgrx::pg_sys::Oid,
    expected_generation: i64,
    manifest_path: &str,
    segment_count: i32,
    max_seq: i64,
    max_commit_seq: i64,
    pending_segment_ids: &[uuid::Uuid],
) -> Result<i64, String> {
    use pgrx::datum::DatumWithOid;

    let new_generation = expected_generation
        .checked_add(1)
        .ok_or_else(|| "manifest generation overflow".to_string())?;
    let segment_ids: Vec<pgrx::Uuid> = pending_segment_ids
        .iter()
        .copied()
        .map(crate::spi::uuid_to_pgrx)
        .collect();
    let statement = plan_activate_flush_segments().map_err(|error| error.to_string())?;
    let activated = crate::spi::update_one::<i64>(
        &statement,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(expected_generation),
            DatumWithOid::from(new_generation),
            DatumWithOid::from(manifest_path),
            DatumWithOid::from(segment_count),
            DatumWithOid::from(max_seq),
            DatumWithOid::from(max_commit_seq),
            DatumWithOid::from(segment_ids),
        ],
    )
    .map_err(|error| error.to_string())?;
    match activated {
        Some(generation) => Ok(generation),
        None => Err(format!(
            "manifest generation CAS failed: expected {expected_generation}"
        )),
    }
}

pub(super) fn prune_flushed_hot_rows(
    table_oid: pgrx::pg_sys::Oid,
    primary_key_columns: &[String],
    max_seq: i64,
    mirror_ops: Option<&[i16]>,
) -> Result<(i64, i64), String> {
    if max_seq <= 0 {
        return Ok((0, 0));
    }

    // PERFORMANCE: Contiguous oldest-by-seq flushes prune with one seq-range
    // DELETE instead of materializing every PK into JSON and chunking
    // jsonb_to_recordset deletes.
    let plan = prepare_seq_range_cleanup(table_oid, primary_key_columns, mirror_ops)?;
    execute_seq_range_cleanup(&plan, max_seq)
}

fn prepare_seq_range_cleanup(
    table_oid: pgrx::pg_sys::Oid,
    primary_key_columns: &[String],
    mirror_ops: Option<&[i16]>,
) -> Result<koldstore_flush::CleanSchemaCleanupPlan, String> {
    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table = QualifiedTableName::parse(&relation).map_err(|error| error.to_string())?;
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = QualifiedTableName::from_table_name(&snapshot.mirror_relation);
    plan_seq_range_cleanup(&table, &mirror, primary_key_columns, mirror_ops)
        .map_err(|error| error.to_string())
}

fn execute_seq_range_cleanup(
    plan: &koldstore_flush::CleanSchemaCleanupPlan,
    max_seq: i64,
) -> Result<(i64, i64), String> {
    use pgrx::datum::DatumWithOid;

    let cleanup_arg = [DatumWithOid::from(max_seq)];
    crate::merge_scan::pg::with_custom_scan_disabled(|| {
        pgrx::Spi::connect_mut(|client| -> Result<(i64, i64), String> {
            client
                .update("SET LOCAL session_replication_role = replica", None, &[])
                .map_err(|error| error.to_string())?;
            // Stamp only cleanup WAL with PostgreSQL's non-replicated origin.
            // Restoring the backend global immediately avoids the session-origin
            // lifecycle and error-recursion hazards of SQL origin setup/reset.
            //
            // `DoNotReplicateId` is `#define DoNotReplicateId PG_UINT16_MAX` in
            // replication/origin.h. Use `u16::MAX` directly: Windows pgrx
            // bindgen does not always export that macro into `pg_sys`.
            let previous_origin = unsafe { pgrx::pg_sys::replorigin_session_origin };
            unsafe {
                pgrx::pg_sys::replorigin_session_origin = u16::MAX;
            }
            let cleanup_result = client.update(&plan.statement.sql, None, &cleanup_arg);
            unsafe {
                pgrx::pg_sys::replorigin_session_origin = previous_origin;
            }
            let tuples = cleanup_result.map_err(|error| error.to_string())?;
            if tuples.is_empty() {
                return Ok((0_i64, 0_i64));
            }
            let row = tuples.first();
            let mirror_pruned = row
                .get_by_name::<i64, &str>("mirror_pruned")
                .map_err(|error| error.to_string())?
                .unwrap_or(0);
            let hot_pruned = row
                .get_by_name::<i64, &str>("hot_pruned")
                .map_err(|error| error.to_string())?
                .unwrap_or(0);
            Ok((mirror_pruned, hot_pruned))
        })
    })
}

fn mirror_pending_row_count(table_oid: pgrx::pg_sys::Oid) -> Result<i64, String> {
    match super::counters::read_table_row_counters(table_oid) {
        Ok(counters) => {
            // Async flush fences via `apply_available` in this same transaction.
            // Apply records counter deltas in backend memory until pre-commit, so
            // include them or flush can falsely see a zero pending mirror.
            let (_, mirror_delta) = crate::row_counter_cache::pending_deltas(table_oid);
            Ok(counters
                .mirror_row_count
                .saturating_add(mirror_delta)
                .max(0))
        }
        Err(_) => Ok(mirror_flush_stats(table_oid)?.row_count),
    }
}

fn mirror_oldest_rows_cutoff(
    table_oid: pgrx::pg_sys::Oid,
    limit: i64,
) -> Result<(i64, i64), String> {
    use pgrx::datum::DatumWithOid;

    if limit <= 0 {
        return Ok((0, 0));
    }
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = MirrorRelation::new(snapshot.mirror_relation.clone());
    let statement = mirror_to_sql(plan_mirror_oldest_rows_max_seq(&mirror))
        .map_err(|error| error.to_string())?;
    if let Some(max_seq) = crate::spi::execute_prepared(
        &statement,
        &[DatumWithOid::from(limit)],
        crate::spi::first_row::<i64>,
    )
    .map_err(|error| error.to_string())?
    {
        return Ok((limit, max_seq));
    }

    // Counters can briefly overshoot after concurrent DML; fall back to a live
    // aggregate so flush still selects the oldest available rows.
    let live = mirror_flush_stats(table_oid)?;
    let capped = limit.min(live.row_count);
    if capped <= 0 {
        return Ok((0, 0));
    }
    if capped == live.row_count {
        return Ok((capped, live.max_seq));
    }
    let max_seq = crate::spi::execute_prepared(
        &statement,
        &[DatumWithOid::from(capped)],
        crate::spi::first_row::<i64>,
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "mirror oldest-rows max-seq lookup returned no rows".to_string())?;
    Ok((capped, max_seq))
}

fn mirror_flush_stats(table_oid: pgrx::pg_sys::Oid) -> Result<FlushStats, String> {
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = MirrorRelation::new(snapshot.mirror_relation.clone());
    let stats = mirror_to_sql(plan_mirror_stats(&mirror)).map_err(|error| error.to_string())?;
    let json = crate::spi::execute_prepared(&stats, &[], crate::spi::first_row::<String>)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "flush stats lookup returned no rows".to_string())?;
    let stats: MirrorSeqStats = serde_json::from_str(&json).map_err(|error| error.to_string())?;
    Ok(stats.into())
}

fn mirror_op_stats(table_oid: pgrx::pg_sys::Oid, op: i16) -> Result<FlushStats, String> {
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = MirrorRelation::new(snapshot.mirror_relation.clone());
    let stats =
        mirror_to_sql(plan_mirror_op_stats(&mirror, op)).map_err(|error| error.to_string())?;
    let json = crate::spi::execute_prepared(&stats, &[], crate::spi::first_row::<String>)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "mirror op stats lookup returned no rows".to_string())?;
    let stats: MirrorSeqStats = serde_json::from_str(&json).map_err(|error| error.to_string())?;
    Ok(stats.into())
}
