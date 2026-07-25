//! Cold Parquet load and segment pruning for KoldMergeScan.

use std::sync::Arc;
use std::time::Instant;

use koldstore_common::{dedupe_nonblank, ColdRow, CommitSeq, SeqId};
use koldstore_merge::scan::plan::{
    group_segments_newest_first, prune_segment_stats_hints, retain_pre_merge_cold_prune_predicates,
    validate_prune_predicates_indexed, ColdPruneColumnPolicy, SegmentPrunePredicate,
    SegmentStatsHint,
};
use koldstore_parquet::{
    clean_cold_row_to_common, read_clean_cold_rows_from_object_store_with_size, ParquetReadOptions,
    PgColumn,
};
use koldstore_schema::PgType;
use koldstore_storage::{manifest_object_key, open_client_from_catalog_fields};
use pgrx::pg_sys;

use crate::catalog::cache::CachedSegmentStats;

use super::profile::{elapsed_ms, ColdReadProfile, SegmentReadProfile};
use super::qual::segment_prune_predicates;
use super::with_hook_disabled;

/// Catalog prune outcome shared by EXPLAIN planning and cold execution.
struct PlannedColdSegments {
    manifest_stats: Arc<CachedSegmentStats>,
    segments: Vec<SegmentStatsHint>,
    segments_considered: usize,
    segments_pruned_min_max: usize,
    prune_predicates: Vec<SegmentPrunePredicate>,
    pk_probe: Option<(String, Vec<String>)>,
    /// Present only when the caller timed the catalog/SPI load.
    manifest_read_ms: Option<f64>,
}

/// Lazily opens safe newest-first segment groups for one CustomScan.
#[derive(Debug)]
pub(super) struct ColdRowStream {
    client: koldstore_storage::ObjectStoreClient,
    segment_groups: Vec<Vec<SegmentStatsHint>>,
    next_group: usize,
    columns: Vec<PgColumn>,
    primary_key_columns: Vec<String>,
    options: ParquetReadOptions,
}

type ColdBatch = (Vec<ColdRow>, Vec<SegmentReadProfile>);

impl ColdRowStream {
    /// Reads the next overlapping segment group and closes every reader before
    /// returning its decoded rows.
    pub(super) fn next_batch(
        &mut self,
        collect_profile: bool,
    ) -> Result<Option<ColdBatch>, String> {
        let Some(group) = self.segment_groups.get(self.next_group) else {
            return Ok(None);
        };
        self.next_group += 1;
        cold_rows_from_segments(
            &self.client,
            group,
            &self.columns,
            &self.primary_key_columns,
            &self.options,
            collect_profile,
        )
        .map(Some)
    }

    /// Rewinds catalog-owned segment descriptors for PostgreSQL rescan.
    pub(super) fn reset(&mut self) {
        self.next_group = 0;
    }
}

/// Prepares a cold stream without opening or decoding a Parquet file.
pub(super) fn prepare_cold_row_stream(
    table_oid: pg_sys::Oid,
    scanrelid: pg_sys::Index,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    qual: *mut pg_sys::List,
    projected_columns: &[&koldstore_migrate::order::CatalogColumn],
    params: pg_sys::ParamListInfo,
) -> Result<(ColdReadProfile, Option<ColdRowStream>), String> {
    with_hook_disabled(|| {
        let Some(planned) =
            plan_cold_segments(table_oid, scanrelid, snapshot, catalog, qual, params, true)?
        else {
            return Ok((ColdReadProfile::empty("(none)"), None));
        };

        let projection = projection_column_names(projected_columns, &snapshot.primary_key_columns);
        let mut profile = cold_read_profile_from_plan(&planned, projection.clone());
        profile.segments_opened = 0;
        if planned.segments.is_empty() {
            return Ok((profile, None));
        }
        if crate::guc::cold_reads_mode() == crate::settings::ColdReadsMode::Off {
            return Err("cold reads are disabled by koldstore.cold_reads".to_string());
        }

        let columns = catalog
            .columns
            .iter()
            .filter(|column| projection.iter().any(|name| name == &column.name))
            .map(|column| PgColumn::new(column.name.clone(), column.pg_type, true))
            .collect::<Vec<_>>();
        let mut options = ParquetReadOptions::new().with_columns(projection);
        if let Some((column, values)) = &planned.pk_probe {
            options = options.with_pk_values(column.clone(), values.clone());
        }
        if let Some((min, max)) = seq_range_from_predicates(&planned.prune_predicates, "seq") {
            options = options.with_clean_seq_range(min, max);
        }
        if let Some((min, max)) =
            commit_seq_range_from_predicates(&planned.prune_predicates, "commit_seq")
        {
            options = options.with_commit_seq_range("commit_seq", min, max);
        }

        let client = open_client_from_catalog_fields(
            &planned.manifest_stats.storage_type,
            &planned.manifest_stats.base_path,
            &planned.manifest_stats.credentials,
            &planned.manifest_stats.config,
        )
        .map_err(|error| error.to_string())?;
        let segment_groups =
            group_segments_newest_first(planned.segments).map_err(|error| error.to_string())?;
        Ok((
            profile,
            Some(ColdRowStream {
                client,
                segment_groups,
                next_group: 0,
                columns,
                primary_key_columns: snapshot.primary_key_columns.clone(),
                options,
            }),
        ))
    })
}

/// Planned cold profile for EXPLAIN without opening Parquet files.
///
/// Applies the same catalog min/max prune as execution so EXPLAIN's
/// `Parquet Segments Planned` matches what Exec would open.
pub(super) unsafe fn planned_cold_read_profile_for_node(
    node: *mut pg_sys::CustomScanState,
) -> Result<ColdReadProfile, String> {
    with_hook_disabled(|| {
        let table_oid = resolve_scan_table_oid(node)?;
        let plan = (*node).ss.ps.plan;
        let qual = if plan.is_null() {
            std::ptr::null_mut()
        } else {
            (*plan).qual
        };
        let scanrelid = plan
            .cast::<pg_sys::CustomScan>()
            .as_ref()
            .map_or(0, |scan| scan.scan.scanrelid);
        let estate = (*node).ss.ps.state;
        let params = if estate.is_null() {
            std::ptr::null_mut()
        } else {
            (*estate).es_param_list_info
        };

        let catalog = crate::catalog::cache::cached_migration_catalog(table_oid)?;
        let Some(snapshot) = crate::catalog::cache::managed_table_snapshot(table_oid)
            .map_err(|error| error.to_string())?
        else {
            return Ok(ColdReadProfile::empty("(none)"));
        };

        let Some(planned) = plan_cold_segments(
            table_oid,
            scanrelid,
            snapshot.as_ref(),
            catalog.as_ref(),
            qual,
            params,
            false,
        )?
        else {
            return Ok(ColdReadProfile::empty("(none)"));
        };

        let mut profile = cold_read_profile_from_plan(&planned, Vec::new());
        // EXPLAIN lists survivors as planned segment stubs (no Parquet open).
        profile.segments = planned
            .segments
            .iter()
            .map(|segment| SegmentReadProfile {
                object_path: segment.object_path.clone(),
                row_count: 0,
                read_ms: None,
                byte_size: segment.byte_size,
                parquet: None,
            })
            .collect();
        Ok(profile)
    })
}

fn plan_cold_segments(
    table_oid: pg_sys::Oid,
    scanrelid: pg_sys::Index,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    qual: *mut pg_sys::List,
    params: pg_sys::ParamListInfo,
    time_manifest: bool,
) -> Result<Option<PlannedColdSegments>, String> {
    // Pre-merge prune is limited to PK + scope + version cursors (seq /
    // commit_seq). Mutable columns stay residual so an older cold version
    // cannot resurrect after its newer segment is pruned away. Scope uses
    // catalog min/max on the shared manifest today (`scope_key = ''`); later
    // each scope_id gets its own manifest/folder and listing filters by
    // scope_key first.
    let scope_column = snapshot.scope_column.as_deref();
    let prune_predicates = retain_pre_merge_cold_prune_predicates(
        unsafe { segment_prune_predicates(table_oid, scanrelid, qual, &catalog.columns, params) },
        |column_name| cold_prune_policy_for_column(column_name, catalog, scope_column),
    );
    let manifest_started = Instant::now();
    let Some(manifest_stats) = crate::catalog::cache::cached_manifest_segment_stats(table_oid)?
    else {
        return Ok(None);
    };
    let manifest_read_ms = time_manifest.then(|| elapsed_ms(manifest_started));
    // Scope + version cursors are always eligible for catalog stats prune.
    let indexed_filter_columns = dedupe_nonblank(
        catalog
            .primary_key
            .columns
            .iter()
            .map(String::as_str)
            .chain(catalog.indexed_columns.iter().map(String::as_str))
            .chain(scope_column)
            .chain(["seq", "commit_seq"]),
    );
    validate_prune_predicates_indexed(&prune_predicates, &indexed_filter_columns)
        .map_err(|error| error.to_string())?;
    let segments_considered = manifest_stats.segments.len();
    let segments = prune_segment_stats_hints(&manifest_stats.segments, &prune_predicates);
    let segments_pruned_min_max = segments_considered.saturating_sub(segments.len());
    let pk_probe = pk_equality_values(&prune_predicates, &snapshot.primary_key_columns);

    Ok(Some(PlannedColdSegments {
        manifest_stats,
        segments,
        segments_considered,
        segments_pruned_min_max,
        prune_predicates,
        pk_probe,
        manifest_read_ms,
    }))
}

fn cold_read_profile_from_plan(
    planned: &PlannedColdSegments,
    projected_columns: Vec<String>,
) -> ColdReadProfile {
    ColdReadProfile {
        manifest_path: manifest_object_key(&planned.manifest_stats.table_prefix),
        storage_type: planned.manifest_stats.storage_type.clone(),
        base_path: planned.manifest_stats.base_path.clone(),
        // EXPLAIN-only planning leaves this None so Status stays "planned".
        // Execution records the catalog SPI clock (cache hits are near-zero).
        manifest_read_ms: planned.manifest_read_ms,
        segments_considered: planned.segments_considered,
        segments_pruned_min_max: planned.segments_pruned_min_max,
        segments_opened: planned.segments.len(),
        pk_probe: planned.pk_probe.clone(),
        projected_columns,
        segments: vec![],
    }
}

/// Builds the pre-merge prune policy for one column name.
fn cold_prune_policy_for_column(
    column_name: &str,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    scope_column: Option<&str>,
) -> Option<ColdPruneColumnPolicy> {
    // Version cursors are safe for pre-merge prune: they identify specific
    // row versions, they do not resurrect older mutable state.
    if column_name == "seq" || column_name == "commit_seq" {
        return Some(ColdPruneColumnPolicy {
            is_primary_key: false,
            is_scope: false,
            is_version_cursor: true,
            ordered_stats_safe: true,
            equality_stats_safe: true,
        });
    }
    let column = catalog
        .columns
        .iter()
        .find(|column| column.name == column_name)?;
    Some(cold_prune_column_policy(column, scope_column))
}

/// Builds the pre-merge prune policy for one catalog column.
fn cold_prune_column_policy(
    column: &koldstore_migrate::order::CatalogColumn,
    scope_column: Option<&str>,
) -> ColdPruneColumnPolicy {
    let ordered_stats_safe = cold_pruning_type_is_collation_independent(column.pg_type);
    ColdPruneColumnPolicy {
        is_primary_key: column.is_primary_key,
        is_scope: scope_column.is_some_and(|scope| scope == column.name),
        is_version_cursor: false,
        ordered_stats_safe,
        // Text scope ids compare as exact flush-encoded JSON strings.
        equality_stats_safe: ordered_stats_safe || column.pg_type == PgType::Text,
    }
}

/// Whether JSON/Parquet scalar comparison has the same semantics as the
/// PostgreSQL type for conservative cold pruning.
///
/// Text and text arrays are deliberately excluded from *ordered* prune:
/// PostgreSQL collation can make range semantics differ from byte-ordered
/// segment stats. Scope text equality is allowed separately via
/// [`ColdPruneColumnPolicy::equality_stats_safe`].
const fn cold_pruning_type_is_collation_independent(pg_type: PgType) -> bool {
    matches!(
        pg_type,
        PgType::Bool | PgType::Int2 | PgType::Int4 | PgType::Int8 | PgType::Uuid
    )
}

fn projection_column_names(
    projected: &[&koldstore_migrate::order::CatalogColumn],
    primary_key_columns: &[String],
) -> Vec<String> {
    let mut names = projected
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    for pk in primary_key_columns {
        if !names.iter().any(|name| name == pk) {
            names.push(pk.clone());
        }
    }
    names
}

/// Extracts a single-column PK equality probe for Parquet bloom/min-max pruning.
///
/// Only fires for single-column PKs with an equality predicate (`min == max`).
/// Composite PKs keep the conservative full-segment read until multi-column
/// bloom probing is wired.
fn pk_equality_values(
    predicates: &[SegmentPrunePredicate],
    primary_key_columns: &[String],
) -> Option<(String, Vec<String>)> {
    if primary_key_columns.len() != 1 {
        return None;
    }
    let pk = &primary_key_columns[0];
    let predicate = predicates.iter().find(|predicate| {
        predicate.column == *pk
            && predicate.min.is_some()
            && predicate.max.is_some()
            && predicate.min == predicate.max
    })?;
    let value = predicate.min.as_ref()?;
    let literal = match value {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Number(number) => number.to_string(),
        serde_json::Value::Bool(flag) => flag.to_string(),
        _ => return None,
    };
    Some((pk.clone(), vec![literal]))
}

fn seq_range_from_predicates(
    predicates: &[SegmentPrunePredicate],
    column: &str,
) -> Option<(SeqId, SeqId)> {
    let predicate = predicates
        .iter()
        .find(|predicate| predicate.column == column)?;
    // Footer prune needs a closed range; open bounds stay catalog-only.
    let min = predicate.min.as_ref().and_then(json_i64)?;
    let max = predicate.max.as_ref().and_then(json_i64)?;
    Some((SeqId::new(min).ok()?, SeqId::new(max).ok()?))
}

fn commit_seq_range_from_predicates(
    predicates: &[SegmentPrunePredicate],
    column: &str,
) -> Option<(CommitSeq, CommitSeq)> {
    let predicate = predicates
        .iter()
        .find(|predicate| predicate.column == column)?;
    let min = predicate.min.as_ref().and_then(json_i64)?;
    let max = predicate.max.as_ref().and_then(json_i64)?;
    Some((CommitSeq::new(min).ok()?, CommitSeq::new(max).ok()?))
}

fn json_i64(value: &serde_json::Value) -> Option<i64> {
    match value {
        serde_json::Value::Number(number) => number.as_i64(),
        serde_json::Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn cold_rows_from_segments(
    client: &koldstore_storage::ObjectStoreClient,
    segment_hints: &[SegmentStatsHint],
    columns: &[PgColumn],
    primary_key_columns: &[String],
    options: &ParquetReadOptions,
    collect_profile: bool,
) -> Result<(Vec<ColdRow>, Vec<SegmentReadProfile>), String> {
    // One ObjectStore client for all segments (filesystem or S3). Parquet reads
    // are footer-first with range GETs — no full-object download. Known
    // `byte_size` enables bounded footer ranges (avoids suffix GETs on S3).
    // Each segment acquires a reader permit, opens the file, finishes (footer
    // prune and/or projected chunks), then drops the reader + permit before
    // the next segment so backends stay under max_open_parquet_readers.
    let store = client.store();
    let mut rows = Vec::new();
    let mut segments = Vec::with_capacity(if collect_profile {
        segment_hints.len()
    } else {
        0
    });
    for hint in segment_hints {
        let started = collect_profile.then(Instant::now);
        let (segment_rows, parquet_profile) = {
            let _permit = crate::merge_scan::reader_pool::try_acquire_parquet_reader_permit(
                crate::guc::max_open_parquet_readers(),
            )?;
            // Reader lives only inside read_clean_*; early footer-only returns
            // drop the ObjectStoreParquetReader before this block ends.
            read_clean_cold_rows_from_object_store_with_size(
                std::sync::Arc::clone(&store),
                &hint.object_path,
                hint.byte_size,
                columns,
                primary_key_columns,
                options,
            )?
        };
        if collect_profile {
            segments.push(SegmentReadProfile {
                object_path: hint.object_path.clone(),
                row_count: segment_rows.len(),
                read_ms: started.map(elapsed_ms),
                byte_size: hint.byte_size.or(parquet_profile.file_size),
                parquet: Some(parquet_profile),
            });
        }
        for row in segment_rows {
            rows.push(clean_cold_row_to_common(row, primary_key_columns)?);
        }
    }
    Ok((rows, segments))
}

unsafe fn resolve_scan_table_oid(
    node: *mut pg_sys::CustomScanState,
) -> Result<pg_sys::Oid, String> {
    if !(*node).ss.ss_currentRelation.is_null() {
        return Ok((*(*node).ss.ss_currentRelation).rd_id);
    }

    let plan = (*node).ss.ps.plan;
    if plan.is_null() {
        return Err("custom scan plan is missing".to_string());
    }
    let custom_scan = plan.cast::<pg_sys::CustomScan>();
    let scanrelid = (*custom_scan).scan.scanrelid;
    if scanrelid == 0 {
        return Err("custom scan relid is missing".to_string());
    }

    let estate = (*node).ss.ps.state;
    if estate.is_null() {
        return Err("executor state is missing".to_string());
    }
    let rte = pg_sys::rt_fetch(scanrelid, (*estate).es_range_table);
    if rte.is_null() {
        return Err("range table entry is missing".to_string());
    }
    Ok((*rte).relid)
}

#[cfg(test)]
mod tests {
    use koldstore_schema::PgType;

    use super::cold_pruning_type_is_collation_independent;

    #[test]
    fn text_like_types_are_not_safe_for_byte_ordered_cold_pruning() {
        assert!(!cold_pruning_type_is_collation_independent(PgType::Text));
        assert!(!cold_pruning_type_is_collation_independent(
            PgType::TextArray
        ));
        assert!(cold_pruning_type_is_collation_independent(PgType::Int8));
        assert!(cold_pruning_type_is_collation_independent(PgType::Uuid));
    }
}
