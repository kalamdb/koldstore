//! PostgreSQL SPI adapters for flush: stats, catalog writes, and cleanup.

use koldstore_catalog::{decode::RelationContext, CatalogManifestSegmentRow, ManagedTableSnapshot};
use koldstore_common::QualifiedTableName;
use koldstore_flush::policy::FlushPolicy;
use koldstore_flush::{
    cleanup::plan_seq_range_cleanup, plan_activate_flush_segments,
    plan_flush_segments_batch_insert, policy_flush_row_count, FlushStats, ResolvedFlushSelection,
    WrittenFlushSegment,
};
use koldstore_manifest::manifest_from_catalog_rows;
use koldstore_mirror::{
    plan_mirror_force_flush_stats, plan_mirror_oldest_rows_max_seq, plan_mirror_stats,
    MirrorRelation, MirrorSeqStats,
};

pub(crate) fn resolve_flush_stats(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> Result<ResolvedFlushSelection, String> {
    use koldstore_flush::{
        apply_force_flush_pass_cap, resolve_force_flush_selection, resolve_policy_flush_selection,
        FORCE_FLUSH_PASS_ROW_CAP,
    };

    if force {
        let (all, delete_stats) = mirror_force_flush_stats(table_oid)?;
        let selection = resolve_force_flush_selection(all, delete_stats);
        // Cap large force mirrors into passes so encode/publish peak stays bounded.
        if selection.mirror_ops.is_none() && selection.stats.row_count > FORCE_FLUSH_PASS_ROW_CAP {
            let cutoff = mirror_oldest_rows_cutoff(table_oid, FORCE_FLUSH_PASS_ROW_CAP)?;
            return Ok(apply_force_flush_pass_cap(
                selection,
                FORCE_FLUSH_PASS_ROW_CAP,
                Some(cutoff),
            ));
        }
        return Ok(selection);
    }

    // PERFORMANCE: Prefer O(1) manifest counters over COUNT(*) on the mirror.
    let pending = mirror_pending_row_count(table_oid)?;
    let policy = active_flush_policy(table_oid)?;
    let cutoff = if pending == 0 {
        None
    } else if let Some(ref policy) = policy {
        match policy {
            FlushPolicy::RowLimit { .. } => {
                let flush_count = policy_flush_row_count(pending, policy);
                (flush_count > 0)
                    .then(|| mirror_oldest_rows_cutoff(table_oid, flush_count))
                    .transpose()?
            }
            FlushPolicy::OlderThan {
                age,
                min_flush_rows,
                max_rows_per_file,
                max_rows_per_flush,
            } => older_than_cutoff(
                table_oid,
                *age,
                *min_flush_rows,
                *max_rows_per_file,
                *max_rows_per_flush,
            )?,
            FlushPolicy::Filter { .. } => {
                return Err("filter flush policy is not supported yet".into())
            }
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
    Ok(active_manage_options(table_oid)?.and_then(|options| options.flush_policy()))
}

pub(crate) fn active_manage_options(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<Option<koldstore_common::ManageTableOptions>, String> {
    let Some(options) = active_options_json(table_oid)? else {
        return Ok(None);
    };
    Ok(Some(koldstore_common::ManageTableOptions::try_from_value(
        &options,
    )?))
}

pub(super) fn active_cold_metadata(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<Option<koldstore_migrate::register::ColdMetadataConfig>, String> {
    let Some(options) = active_options_json(table_oid)? else {
        return Ok(None);
    };
    let Some(value) = options.get("cold_metadata") else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    serde_json::from_value(value.clone())
        .map(Some)
        .map_err(|error| error.to_string())
}

fn active_options_json(table_oid: pgrx::pg_sys::Oid) -> Result<Option<serde_json::Value>, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_catalog::queries::plan_active_flush_policy_options()
        .map_err(|error| error.to_string())?;
    let options =
        crate::spi::select_one::<pgrx::JsonB>(&statement, &[DatumWithOid::from(table_oid)])
            .map_err(|error| error.to_string())?;
    Ok(options.map(|options| options.0))
}

fn older_than_cutoff(
    table_oid: pgrx::pg_sys::Oid,
    age: koldstore_common::MoveAfter,
    min_flush_rows: u64,
    max_rows_per_file: u64,
    max_rows_per_flush: u64,
) -> Result<Option<(i64, i64)>, String> {
    use pgrx::datum::DatumWithOid;
    let cutoff_ms = pgrx::Spi::get_one_with_args::<f64>(
        "SELECT extract(epoch FROM (statement_timestamp() - make_interval(months => $1::int, days => $2::int, secs => $3::double precision))) * 1000",
        &[
            DatumWithOid::from(age.months),
            DatumWithOid::from(age.days),
            DatumWithOid::from(age.microseconds as f64 / 1_000_000.0),
        ],
    ).map_err(|error| error.to_string())?.ok_or_else(|| "failed to compute move_after cutoff".to_string())?;
    let Some(cutoff_seq) = koldstore_common::minimum_id_at_unix_millis(cutoff_ms.floor() as i64)
    else {
        return Ok(None);
    };
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = MirrorRelation::new(snapshot.mirror_relation.clone()).quoted();
    let statement = koldstore_flush::plan_older_than_eligible_mirror_rows(&mirror)
        .map_err(|error| error.to_string())?;
    let (count, max_seq) = pgrx::Spi::connect(|client| {
        let row = client
            .select(
                &statement.sql,
                None,
                &[
                    DatumWithOid::from(cutoff_seq),
                    DatumWithOid::from(max_rows_per_flush as i64),
                ],
            )?
            .first();
        Ok::<_, pgrx::spi::SpiError>((row.get::<i64>(1)?.unwrap_or(0), row.get::<i64>(2)?))
    })
    .map_err(|error| error.to_string())?;
    if count < min_flush_rows as i64 {
        return Ok(None);
    }
    if !koldstore_flush::selected_rows_meet_file_minimum(count.max(0) as u64, max_rows_per_file) {
        return Ok(None);
    }
    Ok(max_seq.map(|seq| (count, seq)))
}

/// Blocks concurrent source DML for the async prune fence.
///
/// Uses `SHARE ROW EXCLUSIVE` so in-flight writers finish, new writers wait,
/// and ordinary `SELECT` continues. Sets a local `lock_timeout` so an idle
/// blocker fails the flush before prune rather than waiting forever.
pub(crate) fn lock_source_table_share_row_exclusive(
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
/// Delegates to [`crate::mirror::apply::capture_durable_wal_fence`] so flush
/// prune fences and `wait_for_async_mirror` share one LSN capture path.
pub(super) fn capture_durable_wal_fence() -> Result<crate::mirror::apply::WalFenceLsn, String> {
    crate::mirror::apply::capture_durable_wal_fence()
}

pub(super) fn next_flush_batch_number(table_oid: pgrx::pg_sys::Oid) -> Result<i32, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_catalog::queries::plan_next_flush_batch_number()
        .map_err(|error| error.to_string())?;
    crate::spi::select_one::<i32>(&statement, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "batch number lookup returned no rows".to_string())
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

/// Catalog identity for a flush writer attempt/pass.
///
/// UUIDs identify job / attempt / pass. Per-segment ordinals remain `i32` in
/// the batch insert path (catalog column `segment_ordinal`) until a shared
/// ordinal newtype exists.
#[derive(Debug, Clone, Copy)]
pub(super) struct FlushSegmentWriterIdentity {
    pub job_id: uuid::Uuid,
    pub attempt_token: uuid::Uuid,
    pub pass_id: uuid::Uuid,
    /// `cold_segment_order_index.sort_order_id` that matches Parquet physical sort,
    /// or `0` when segments are only seq-sorted (default flush).
    pub physically_sorted_sort_order_id: i32,
}

/// Catalogs every segment written by one `flush_table` call.
///
/// Segment rows + packed `cold_segment_index` bounds go in one SPI insert.
/// Exact per-PK catalog hints are intentionally not written: prune with
/// `cold_segment_index` / Parquet stats so catalog size stays O(segments).
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared or SPI
/// execution fails.
pub(super) fn persist_flush_segments_batch(
    table_oid: pgrx::pg_sys::Oid,
    writer: FlushSegmentWriterIdentity,
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
    let mut row_counts = Vec::with_capacity(segments.len());
    let mut byte_sizes = Vec::with_capacity(segments.len());
    let mut schema_versions = Vec::with_capacity(segments.len());
    let mut checksums = Vec::with_capacity(segments.len());
    let mut object_etags = Vec::with_capacity(segments.len());
    let mut segment_row_group_counts = Vec::with_capacity(segments.len());
    let mut segment_row_group_offsets = Vec::with_capacity(segments.len());
    let mut row_group_row_counts = Vec::new();
    let mut row_group_min_seqs = Vec::new();
    let mut row_group_max_seqs = Vec::new();
    let mut index_segment_ids = Vec::new();
    let mut index_column_ids = Vec::new();
    let mut index_type_oids = Vec::new();
    let mut index_codec_versions = Vec::new();
    let mut index_min_values: Vec<Option<Vec<u8>>> = Vec::new();
    let mut index_max_values: Vec<Option<Vec<u8>>> = Vec::new();
    let mut index_row_group_counts = Vec::new();
    let mut index_row_group_offsets = Vec::new();
    let mut row_group_min_values: Vec<Option<Vec<u8>>> = Vec::new();
    let mut row_group_max_values: Vec<Option<Vec<u8>>> = Vec::new();
    let mut row_group_null_counts: Vec<Option<i64>> = Vec::new();
    for segment in segments {
        let row = &segment.catalog_row;
        let packed = &segment.packed_metadata;
        let row_group_count = i32::try_from(packed.row_group_count)
            .map_err(|_| "row-group count exceeds PostgreSQL integer range".to_string())?;
        if row_group_count <= 0
            || packed.row_group_row_counts.len() != packed.row_group_count
            || packed.row_group_min_seqs.len() != packed.row_group_count
            || packed.row_group_max_seqs.len() != packed.row_group_count
        {
            return Err(format!(
                "segment {} has malformed packed row-group metadata",
                segment.segment_id
            ));
        }
        let segment_id = crate::spi::uuid_to_pgrx(segment.segment_id);
        segment_ids.push(segment_id);
        object_paths.push(row.path.clone());
        batch_numbers.push(row.batch_number);
        min_seqs.push(row.min_seq);
        max_seqs.push(row.max_seq);
        row_counts.push(row.row_count);
        byte_sizes.push(row.byte_size);
        schema_versions.push(row.schema_version);
        checksums.push(segment.checksum.clone());
        object_etags.push(segment.object_etag.clone().unwrap_or_default());
        segment_row_group_counts.push(row_group_count);
        segment_row_group_offsets.push(i32::try_from(row_group_row_counts.len()).map_err(
            |_| "flattened segment row-group metadata exceeds PostgreSQL integer range".to_string(),
        )?);
        row_group_row_counts.extend_from_slice(&packed.row_group_row_counts);
        row_group_min_seqs.extend_from_slice(&packed.row_group_min_seqs);
        row_group_max_seqs.extend_from_slice(&packed.row_group_max_seqs);

        for bound in &packed.column_indexes {
            if bound.row_group_min_values.len() != packed.row_group_count
                || bound.row_group_max_values.len() != packed.row_group_count
                || bound.row_group_null_counts.len() != packed.row_group_count
            {
                return Err(format!(
                    "segment {} column {} has malformed packed row-group metadata",
                    segment.segment_id, bound.column_id
                ));
            }
            index_segment_ids.push(segment_id);
            index_column_ids.push(bound.column_id.get());
            index_type_oids.push(pgrx::pg_sys::Oid::from(bound.type_oid));
            index_codec_versions.push(bound.codec_version);
            index_min_values.push(bound.min_value.clone());
            index_max_values.push(bound.max_value.clone());
            index_row_group_counts.push(row_group_count);
            index_row_group_offsets.push(i32::try_from(row_group_min_values.len()).map_err(
                |_| {
                    "flattened column row-group metadata exceeds PostgreSQL integer range"
                        .to_string()
                },
            )?);
            row_group_min_values.extend(bound.row_group_min_values.iter().cloned());
            row_group_max_values.extend(bound.row_group_max_values.iter().cloned());
            row_group_null_counts.extend_from_slice(&bound.row_group_null_counts);
        }
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
            DatumWithOid::from(row_counts),
            DatumWithOid::from(byte_sizes),
            DatumWithOid::from(schema_versions),
            DatumWithOid::from(checksums),
            DatumWithOid::from(object_etags),
            DatumWithOid::from(segment_row_group_counts),
            DatumWithOid::from(segment_row_group_offsets),
            DatumWithOid::from(row_group_row_counts),
            DatumWithOid::from(row_group_min_seqs),
            DatumWithOid::from(row_group_max_seqs),
            DatumWithOid::from(index_segment_ids),
            DatumWithOid::from(index_column_ids),
            DatumWithOid::from(index_type_oids),
            DatumWithOid::from(index_codec_versions),
            DatumWithOid::from(index_min_values),
            DatumWithOid::from(index_max_values),
            DatumWithOid::from(index_row_group_counts),
            DatumWithOid::from(index_row_group_offsets),
            DatumWithOid::from(row_group_min_values),
            DatumWithOid::from(row_group_max_values),
            DatumWithOid::from(row_group_null_counts),
            DatumWithOid::from(crate::spi::uuid_to_pgrx(writer.job_id)),
            DatumWithOid::from(crate::spi::uuid_to_pgrx(writer.attempt_token)),
            DatumWithOid::from(crate::spi::uuid_to_pgrx(writer.pass_id)),
            DatumWithOid::from(writer.physically_sorted_sort_order_id),
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
    writer: FlushSegmentWriterIdentity,
    segment: &WrittenFlushSegment,
) -> Result<(), String> {
    persist_flush_segments_batch(table_oid, writer, std::slice::from_ref(segment))
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
    segment_count: i32,
    max_seq: i64,
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
            DatumWithOid::from(segment_count),
            DatumWithOid::from(max_seq),
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
            // Keep the database-scoped origin set through COMMIT. pgoutput
            // emits ORIGIN from the commit record's origin; restoring before
            // commit leaves PG15 prune DELETEs without an ORIGIN message.
            arm_flush_replication_origin()?;
            let tuples = client
                .update(&plan.statement.sql, None, &cleanup_arg)
                .map_err(|error| error.to_string())?;
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

std::thread_local! {
    /// Prior `replorigin_session_origin` to restore after the flush xact ends.
    static FLUSH_ORIGIN_RESTORE: std::cell::Cell<Option<pgrx::pg_sys::RepOriginId>> =
        const { std::cell::Cell::new(None) };
    /// When set, the PG15 named-origin path must `replorigin_session_reset`.
    static FLUSH_ORIGIN_NEEDS_SESSION_RESET: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Registers the xact callback that clears the flush origin after commit/abort.
pub(crate) fn register_flush_origin_xact_callback() {
    unsafe {
        pgrx::pg_sys::RegisterXactCallback(Some(flush_origin_xact_callback), std::ptr::null_mut());
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn flush_origin_xact_callback(
    event: pgrx::pg_sys::XactEvent::Type,
    _arg: *mut std::ffi::c_void,
) {
    // Restore only after the commit/abort WAL is written. Pre-commit would
    // clear the origin before the commit record is stamped, which is exactly
    // the PG15 ORIGIN-message failure mode we are fixing.
    match event {
        pgrx::pg_sys::XactEvent::XACT_EVENT_COMMIT
        | pgrx::pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT
        | pgrx::pg_sys::XactEvent::XACT_EVENT_ABORT
        | pgrx::pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT => {
            FLUSH_ORIGIN_RESTORE.with(|slot| {
                if let Some(previous) = slot.take() {
                    let needs_reset =
                        FLUSH_ORIGIN_NEEDS_SESSION_RESET.with(|flag| flag.replace(false));
                    unsafe {
                        // Only reset when this backend armed via session_setup.
                        // Calling reset without a live session state can Assert/FATAL
                        // and take down the backend mid-abort (including async apply).
                        if needs_reset {
                            pgrx::pg_sys::replorigin_session_reset();
                        }
                        pgrx::pg_sys::replorigin_session_origin = previous;
                    }
                } else {
                    // Keep the flag from leaking across xacts if restore was cleared.
                    FLUSH_ORIGIN_NEEDS_SESSION_RESET.with(|flag| flag.set(false));
                }
            });
        }
        _ => {}
    }
}

/// Stamps prune WAL so async apply does not re-ingest flush deletes.
///
/// - PG16+: set `DoNotReplicateId` through commit. Peek `origin=none` filters it
///   and no exclusive replication-origin session is required, so parallel flushes
///   in one database do not contend.
/// - PG15: named database-scoped origin via `replorigin_session_setup`, serialized
///   by an advisory xact lock. pgoutput emits ORIGIN; apply skips that name.
fn arm_flush_replication_origin() -> Result<(), String> {
    FLUSH_ORIGIN_RESTORE.with(|slot| {
        if slot.get().is_some() {
            return Ok(());
        }
        #[cfg(feature = "pg15")]
        {
            arm_named_flush_origin_pg15(slot)
        }
        #[cfg(not(feature = "pg15"))]
        {
            arm_do_not_replicate_origin(slot)
        }
    })
}

/// Returns whether this transaction's managed-table writes are internal prune WAL.
pub(crate) fn flush_replication_origin_is_armed() -> bool {
    FLUSH_ORIGIN_RESTORE.with(|slot| slot.get().is_some())
}

/// PG16+ path: stamp `DoNotReplicateId` without `replorigin_session_setup`.
#[cfg(not(feature = "pg15"))]
fn arm_do_not_replicate_origin(
    slot: &std::cell::Cell<Option<pgrx::pg_sys::RepOriginId>>,
) -> Result<(), String> {
    // `DoNotReplicateId` is `#define DoNotReplicateId PG_UINT16_MAX`. Use
    // `u16::MAX` directly: Windows pgrx bindgen does not always export that
    // macro. Commit special-cases it so `replorigin_session_advance` is skipped
    // and no session setup is required.
    unsafe {
        let previous = pgrx::pg_sys::replorigin_session_origin;
        pgrx::pg_sys::replorigin_session_origin = u16::MAX;
        FLUSH_ORIGIN_NEEDS_SESSION_RESET.with(|flag| flag.set(false));
        slot.set(Some(previous));
    }
    Ok(())
}

/// PG15 path: exclusive named origin, queued behind a database advisory lock.
#[cfg(feature = "pg15")]
fn arm_named_flush_origin_pg15(
    slot: &std::cell::Cell<Option<pgrx::pg_sys::RepOriginId>>,
) -> Result<(), String> {
    use std::ffi::CString;

    let database_oid =
        koldstore_worker::DatabaseOid::new(unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32());
    // Queue same-DB parallel prunes instead of failing with "origin already active".
    pgrx::Spi::run_with_args(
        "SELECT pg_catalog.pg_advisory_xact_lock($1, $2)",
        &[
            pgrx::datum::DatumWithOid::from(crate::mirror::lifecycle::FLUSH_ORIGIN_LOCK_NAMESPACE),
            pgrx::datum::DatumWithOid::from(database_oid.get() as i32),
        ],
    )
    .map_err(|error| format!("flush origin advisory lock: {error}"))?;

    let origin_name = crate::mirror::lifecycle::flush_replication_origin_name(database_oid);
    let origin_name = CString::new(origin_name)
        .map_err(|_| "flush replication origin name contains NUL".to_string())?;
    let origin_id = pgrx::PgTryBuilder::new(|| unsafe {
        let mut id = pgrx::pg_sys::replorigin_by_name(origin_name.as_ptr(), true);
        if id == pgrx::pg_sys::InvalidRepOriginId as pgrx::pg_sys::RepOriginId {
            id = pgrx::pg_sys::replorigin_create(origin_name.as_ptr());
        }
        Ok(id)
    })
    .catch_others(|error| {
        let message = match error {
            pgrx::pg_sys::panic::CaughtError::PostgresError(report)
            | pgrx::pg_sys::panic::CaughtError::ErrorReport(report) => report.message().to_string(),
            pgrx::pg_sys::panic::CaughtError::RustPanic { ereport, .. } => {
                ereport.message().to_string()
            }
        };
        Err(format!("flush replication origin lookup/create: {message}"))
    })
    .execute()?;
    if origin_id == pgrx::pg_sys::InvalidRepOriginId as pgrx::pg_sys::RepOriginId {
        return Err("failed to create koldstore_flush replication origin".to_string());
    }
    let previous = unsafe { pgrx::pg_sys::replorigin_session_origin };
    // `replorigin_session_setup` ereports if another backend holds the origin;
    // convert that to a Rust Err so flush soft-fails instead of aborting mid-prune.
    pgrx::PgTryBuilder::new(|| {
        unsafe {
            pgrx::pg_sys::replorigin_session_setup(origin_id);
        }
        Ok(())
    })
    .catch_others(|error| {
        let message = match error {
            pgrx::pg_sys::panic::CaughtError::PostgresError(report)
            | pgrx::pg_sys::panic::CaughtError::ErrorReport(report) => report.message().to_string(),
            pgrx::pg_sys::panic::CaughtError::RustPanic { ereport, .. } => {
                ereport.message().to_string()
            }
        };
        Err(format!("replorigin_session_setup({origin_id}): {message}"))
    })
    .execute()?;
    unsafe {
        pgrx::pg_sys::replorigin_session_origin = origin_id;
    }
    FLUSH_ORIGIN_NEEDS_SESSION_RESET.with(|flag| flag.set(true));
    slot.set(Some(previous));
    Ok(())
}

pub(crate) fn mirror_pending_row_count(table_oid: pgrx::pg_sys::Oid) -> Result<i64, String> {
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
    let statement = plan_mirror_oldest_rows_max_seq(&mirror).map_err(|error| error.to_string())?;
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
    let stats = plan_mirror_stats(&mirror).map_err(|error| error.to_string())?;
    let json = crate::spi::execute_prepared(&stats, &[], crate::spi::first_row::<String>)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "flush stats lookup returned no rows".to_string())?;
    let stats: MirrorSeqStats = serde_json::from_str(&json).map_err(|error| error.to_string())?;
    Ok(stats.into())
}

/// One mirror scan for force-flush all-row + delete aggregates.
fn mirror_force_flush_stats(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<(FlushStats, FlushStats), String> {
    use koldstore_common::MirrorOperation;

    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = MirrorRelation::new(snapshot.mirror_relation.clone());
    let delete_code = MirrorOperation::Delete.code();
    let stats =
        plan_mirror_force_flush_stats(&mirror, delete_code).map_err(|error| error.to_string())?;
    let json = crate::spi::execute_prepared(&stats, &[], crate::spi::first_row::<String>)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "force flush stats lookup returned no rows".to_string())?;
    let value: serde_json::Value =
        serde_json::from_str(&json).map_err(|error| error.to_string())?;
    let all: MirrorSeqStats = serde_json::from_value(
        value
            .get("all")
            .cloned()
            .ok_or_else(|| "force flush stats missing `all`".to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let delete_stats: MirrorSeqStats = serde_json::from_value(
        value
            .get("delete")
            .cloned()
            .ok_or_else(|| "force flush stats missing `delete`".to_string())?,
    )
    .map_err(|error| error.to_string())?;
    Ok((all.into(), delete_stats.into()))
}

/// Mirror `max(seq)` at flush-job start, used to pin multi-pass catch-up.
pub(super) fn mirror_catchup_watermark(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<Option<i64>, String> {
    let stats = mirror_flush_stats(table_oid)?;
    if stats.row_count <= 0 || stats.max_seq <= 0 {
        Ok(None)
    } else {
        Ok(Some(stats.max_seq))
    }
}

/// Row count for the flush progress bar at claim time.
///
/// Policy flushes use the selected flush count (not the full mirror backlog).
/// Force / no-policy flushes use the O(1) pending mirror counter.
pub(super) fn flush_progress_total_estimate(
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
) -> Result<i64, String> {
    let pending = mirror_pending_row_count(table_oid)?.max(0);
    if force {
        return Ok(pending);
    }
    match active_flush_policy(table_oid)? {
        Some(policy) => Ok(policy_flush_row_count(pending, &policy)),
        None => Ok(pending),
    }
}
