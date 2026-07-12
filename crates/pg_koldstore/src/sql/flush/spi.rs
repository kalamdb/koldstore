//! PostgreSQL SPI adapters for flush: stats, catalog writes, and cleanup.

use koldstore_catalog::{decode::RelationContext, ManagedTableSnapshot};
use koldstore_common::QualifiedTableName;
use koldstore_flush::policy::FlushPolicy;
use koldstore_flush::{
    cleanup::plan_seq_range_cleanup, flush_pending_threshold, manifest_from_catalog_rows,
    materialize_pending_upserts, plan_delete_pending_for_scopes, plan_flush_segments_batch_insert,
    plan_list_pending, plan_manifest_row_upsert, plan_pending_segments, plan_upsert_pending,
    policy_flush_row_count, CatalogManifestSegmentRow, FlushStats, PendingSegmentPlan,
    PendingUpsert, PreFlushInput, ResolvedFlushSelection, WrittenFlushSegment,
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

pub(super) fn next_flush_batch_number(table_oid: pgrx::pg_sys::Oid) -> Result<i32, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_catalog::queries::plan_next_flush_batch_number()
        .map_err(|error| error.to_string())?;
    crate::spi::select_one::<i32>(&statement, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "batch number lookup returned no rows".to_string())
}

pub(super) fn active_segment_count(table_oid: pgrx::pg_sys::Oid) -> Result<i64, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_catalog::queries::plan_active_segment_count()
        .map_err(|error| error.to_string())?;
    crate::spi::select_one::<i64>(&statement, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "cold segment count lookup returned no rows".to_string())
}

pub(super) fn manifest_from_active_segments(
    table_oid: pgrx::pg_sys::Oid,
    relation: &RelationContext,
    snapshot: &ManagedTableSnapshot,
    schema_version: i32,
) -> Result<koldstore_manifest::Manifest, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_catalog::queries::plan_active_segments_for_manifest_json()
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

/// Catalogs every segment written by one `flush_table` call.
///
/// Segment rows + normalized `segment_stats` go in one SPI round trip.
/// Exact per-PK catalog hints are intentionally not written: prune with
/// `segment_stats` / Parquet stats so catalog size stays O(segments).
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
    for segment in segments {
        let row = &segment.catalog_row;
        let segment_id = pgrx::Uuid::from_bytes(*segment.segment_id.as_bytes());
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

pub(super) fn upsert_manifest_row(
    table_oid: pgrx::pg_sys::Oid,
    manifest_path: &str,
    segment_count: i32,
    max_seq: i64,
    max_commit_seq: i64,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    let generation = uuid::Uuid::new_v4().to_string();
    let statement = plan_manifest_row_upsert().map_err(|error| error.to_string())?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(manifest_path),
            DatumWithOid::from(generation.as_str()),
            DatumWithOid::from(segment_count),
            DatumWithOid::from(max_seq),
            DatumWithOid::from(max_commit_seq),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// Promotes all `staged` segments for a table to query-visible `published`.
///
/// Used after the manifest object and catalog row are durable for this flush.
pub(super) fn promote_all_staged_segments_to_published(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    // Empty uuid array means "no specific ids" — promote every staged row for
    // the table under the flush advisory lock.
    let statement = koldstore_common::SqlStatement::write(
        "flush promote all staged segments published",
        r#"
UPDATE koldstore.segments
SET status = 'published'
WHERE table_oid = $1::oid
  AND status = 'staged'
"#,
    )
    .map_err(|error| error.to_string())?;
    crate::spi::update(&statement, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?;
    Ok(())
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

fn mirror_pending_row_count(table_oid: pgrx::pg_sys::Oid) -> Result<i64, String> {
    match super::counters::read_table_row_counters(table_oid) {
        Ok(counters) => Ok(counters.mirror_row_count.max(0)),
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

/// Syncs in-memory counters into `koldstore.pending` rows (create or update).
///
/// Returns every pending reservation for the table after sync. Callers apply the
/// hot-row threshold when deciding which rows to flush.
pub(super) fn sync_pending_from_counters(
    table_oid: pgrx::pg_sys::Oid,
    schema_version: i32,
    force: bool,
) -> Result<Vec<PendingRow>, String> {
    let durable_count = mirror_pending_row_count(table_oid)?.max(0) as u64;
    let durable = if durable_count > 0 {
        vec![("".to_string(), durable_count)]
    } else {
        Vec::new()
    };
    let policy = active_flush_policy(table_oid)?;
    let plans = plan_pending_segments(PreFlushInput {
        table_oid: table_oid.to_u32(),
        policy: policy.as_ref(),
        force,
        durable_by_scope: &durable,
    });
    if !plans.is_empty() {
        let upserts = materialize_pending_upserts(schema_version, &plans);
        upsert_pending(table_oid, &upserts)?;
    }
    list_pending(table_oid)
}

/// Catalog pending reservation row used for flush selection.
#[derive(Debug, Clone)]
pub(super) struct PendingRow {
    pub scope_key: String,
    pub row_count: u64,
}

impl PendingRow {
    pub(super) fn to_plan(&self, table_oid: u32) -> Result<PendingSegmentPlan, String> {
        let key = koldstore_common::ScopeCounterKey::from_optional_scope(
            table_oid,
            if self.scope_key.is_empty() {
                None
            } else {
                Some(self.scope_key.as_str())
            },
        )
        .map_err(|error| error.to_string())?;
        Ok(PendingSegmentPlan {
            key,
            row_count: self.row_count,
        })
    }
}

/// Returns pending rows that should flush under the table policy.
pub(super) fn select_flushable_pending(
    pending: &[PendingRow],
    policy: Option<&FlushPolicy>,
    force: bool,
) -> Vec<PendingRow> {
    let threshold = flush_pending_threshold(policy);
    let pairs: Vec<(String, u64)> = pending
        .iter()
        .map(|row| (row.scope_key.clone(), row.row_count))
        .collect();
    let selected = koldstore_flush::select_flushable_pending_rows(&pairs, threshold, force);
    selected
        .into_iter()
        .map(|(scope_key, row_count)| PendingRow {
            scope_key,
            row_count,
        })
        .collect()
}

fn list_pending(table_oid: pgrx::pg_sys::Oid) -> Result<Vec<PendingRow>, String> {
    use pgrx::datum::DatumWithOid;

    let statement = plan_list_pending().map_err(|error| error.to_string())?;
    let json = crate::spi::select_one::<String>(&statement, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?
        .unwrap_or_else(|| "[]".to_string());
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(&json).map_err(|error| error.to_string())?;
    let mut pending = Vec::with_capacity(rows.len());
    for row in rows {
        let scope_key = row
            .get("scope_key")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_string();
        let row_count = row
            .get("row_count")
            .and_then(|value| value.as_i64())
            .unwrap_or(0)
            .max(0) as u64;
        pending.push(PendingRow {
            scope_key,
            row_count,
        });
    }
    Ok(pending)
}

fn upsert_pending(table_oid: pgrx::pg_sys::Oid, upserts: &[PendingUpsert]) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    if upserts.is_empty() {
        return Ok(());
    }

    let mut scope_keys = Vec::with_capacity(upserts.len());
    let mut row_counts = Vec::with_capacity(upserts.len());
    let mut schema_versions = Vec::with_capacity(upserts.len());
    for upsert in upserts {
        scope_keys.push(upsert.scope_key.clone());
        row_counts.push(upsert.row_count);
        schema_versions.push(upsert.schema_version);
    }

    let statement = plan_upsert_pending().map_err(|error| error.to_string())?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(scope_keys),
            DatumWithOid::from(row_counts),
            DatumWithOid::from(schema_versions),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

pub(super) fn delete_pending_for_scopes(
    table_oid: pgrx::pg_sys::Oid,
    scope_keys: &[String],
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    if scope_keys.is_empty() {
        return Ok(());
    }
    let statement = plan_delete_pending_for_scopes().map_err(|error| error.to_string())?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(scope_keys.to_vec()),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// Sync counters into `koldstore.pending` and return reservation count.
pub(super) fn ensure_pending_for_flush(
    table_oid: pgrx::pg_sys::Oid,
    schema_version: i32,
    force: bool,
) -> Result<i64, String> {
    let pending = sync_pending_from_counters(table_oid, schema_version, force)?;
    Ok(pending.len() as i64)
}
