//! Cold Parquet load and segment pruning for KoldMergeScan.

use std::collections::BTreeMap;
use std::time::Instant;

use crate::object_store::open_managed_object_store_client;
use koldstore_catalog::{preferred_segment_index_access, SegmentIndexLookupShape};
use koldstore_common::{ColdRow, ColumnId, ColumnRef, SeqId};
use koldstore_merge::scan::plan::{
    group_segments_newest_first, retain_pre_merge_cold_prune_predicates,
    validate_prune_predicates_indexed, ColdPruneColumnPolicy, SegmentPrunePredicate,
    SegmentStatsHint,
};
use koldstore_parquet::{
    clean_cold_row_to_common, read_clean_cold_rows_from_object_store_with_size, ParquetReadOptions,
    PgColumn,
};
use pgrx::pg_sys;

use super::profile::{elapsed_ms, ColdReadProfile, SegmentReadProfile};
use super::qual::segment_prune_predicates;
use super::with_hook_disabled;

/// Returns whether safe bounds prove that no active cold segment can match.
///
/// Planner calls pass no parameters and therefore consider constants only.
/// Executor calls may resolve external prepared-statement parameters.
/// Unsupported or mutable columns produce no proof, while missing/incomplete
/// catalog statistics conservatively retain KoldMergeScan.
///
/// # Safety
///
/// `qual` must be a planner-owned expression list for `scanrelid`.
pub(super) unsafe fn cold_side_proven_empty(
    table_oid: pg_sys::Oid,
    scanrelid: pg_sys::Index,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    qual: *mut pg_sys::List,
    manifest_generation: u64,
    params: pg_sys::ParamListInfo,
) -> Result<bool, String> {
    let predicates = retain_pre_merge_cold_prune_predicates(
        unsafe { segment_prune_predicates(scanrelid, qual, &catalog, params) },
        |column_id| {
            let column = catalog
                .column_by_attnum(column_id)?;
            Some(cold_prune_column_policy(
                column,
                snapshot.scope_column_id,
                snapshot.segment_order_column_id,
            ))
        },
    );
    let encoded_bounds = encode_prune_predicate_bounds(catalog, &predicates)?;

    let mut candidate_columns = encoded_bounds.keys().copied().collect::<Vec<_>>();
    candidate_columns.sort_by_key(|column_id| {
        let preferred = snapshot
            .segment_order_column_id
            .is_some_and(|id| id.get() == *column_id);
        (!preferred, *column_id)
    });

    for column_id in candidate_columns {
        let Some(column) = catalog
            .column_by_attnum(column_id)
        else {
            continue;
        };
        let key = crate::catalog::cache::ColdColumnBoundsCacheKey::new(
            table_oid.to_u32(),
            manifest_generation,
            column_id,
            column.pg_type.type_oid(),
        );
        let Some(cold_bounds) = crate::catalog::cache::cached_cold_column_bounds(key)? else {
            continue;
        };
        let Some((lower, upper)) = encoded_bounds.get(&column_id) else {
            continue;
        };
        if lower
            .as_deref()
            .is_some_and(|value| value > cold_bounds.max_value.as_ref())
            || upper
                .as_deref()
                .is_some_and(|value| value < cold_bounds.min_value.as_ref())
        {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Lazily opens safe newest-first segment groups for one CustomScan.
#[derive(Debug)]
pub(super) struct ColdRowStream {
    client: koldstore_storage::ObjectStoreClient,
    segment_groups: Vec<Vec<SegmentStatsHint>>,
    next_group: usize,
    projection_columns: Vec<ColumnRef>,
    catalog_columns: Vec<koldstore_migrate::order::CatalogColumn>,
    primary_key_columns: Vec<ColumnRef>,
    schema_version: i32,
    pk_probe: Option<(ColumnRef, Vec<String>)>,
}

type ColdBatch = (Vec<ColdRow>, Vec<SegmentReadProfile>);
type PackedRowGroupSpiRow = (
    uuid::Uuid,
    i16,
    i32,
    Vec<i64>,
    Vec<Option<Vec<u8>>>,
    Vec<Option<Vec<u8>>>,
    Vec<Option<i64>>,
);
type SegmentIndexCandidateSpiRow = (
    uuid::Uuid,
    SegmentStatsHint,
    i16,
    std::sync::Arc<crate::catalog::cache::CachedPackedRowGroupIndex>,
);

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
        let (rows, profiles) = cold_rows_from_segments(
            &self.client,
            group,
            &self.projection_columns,
            &self.catalog_columns,
            &self.primary_key_columns,
            self.schema_version,
            self.pk_probe.clone(),
        )?;
        if !collect_profile {
            return Ok(Some((rows, Vec::new())));
        }
        Ok(Some((rows, profiles)))
    }

    /// Rewinds catalog-owned segment descriptors for PostgreSQL rescan.
    pub(super) fn reset(&mut self) {
        self.next_group = 0;
    }

    /// True when catalog still has unopened segment groups for this scan.
    #[must_use]
    pub(super) fn has_pending_segments(&self) -> bool {
        self.next_group < self.segment_groups.len()
    }

    /// Intersects planned row groups with order-frontier competitive groups.
    ///
    /// Missing order-index rows leave the hint unchanged. An empty competitive
    /// set marks the segment with `selected_row_groups = Some([])` so open is skipped.
    pub(super) fn apply_competitive_row_groups(
        &mut self,
        table_oid: pg_sys::Oid,
        scope_key: &str,
        sort_order_id: i32,
        direction: koldstore_merge::scan::OrderDirection,
        hot_key: Option<&[u8]>,
    ) -> Result<(), String> {
        for group in &mut self.segment_groups {
            for hint in group {
                let Some(selected) = super::cold_frontier::competitive_row_groups_for_path(
                    table_oid,
                    scope_key,
                    sort_order_id,
                    &hint.object_path,
                    direction,
                    hot_key,
                )?
                else {
                    continue;
                };
                hint.selected_row_groups = Some(match hint.selected_row_groups.take() {
                    Some(existing) => existing
                        .into_iter()
                        .filter(|rg| selected.contains(rg))
                        .collect(),
                    None => selected,
                });
            }
        }
        Ok(())
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
        let Some(planned) = plan_cold_segments(
            table_oid,
            scanrelid,
            snapshot,
            catalog,
            qual,
            projected_columns,
            params,
        )?
        else {
            return Ok((ColdReadProfile::empty("(none)"), None));
        };

        let mut profile = planned.profile;
        profile.segments_opened = 0;
        if planned.segments.is_empty() {
            return Ok((profile, None));
        }
        if crate::guc::cold_reads_mode() == crate::settings::ColdReadsMode::Off {
            return Err("cold reads are disabled by koldstore.cold_reads".to_string());
        }

        let client = open_managed_object_store_client(
            &planned.storage_type,
            &planned.base_path,
            &planned.credentials,
            &planned.config,
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
                projection_columns: planned.projection_columns,
                catalog_columns: catalog.columns.clone(),
                primary_key_columns: snapshot.primary_key_columns.clone(),
                schema_version: snapshot.schema_version,
                pk_probe: planned.pk_probe,
            }),
        ))
    })
}

struct PlannedColdSegments {
    profile: ColdReadProfile,
    storage_type: String,
    base_path: String,
    credentials: serde_json::Value,
    config: serde_json::Value,
    segments: Vec<SegmentStatsHint>,
    projection_columns: Vec<ColumnRef>,
    pk_probe: Option<(ColumnRef, Vec<String>)>,
}

fn plan_cold_segments(
    table_oid: pg_sys::Oid,
    scanrelid: pg_sys::Index,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    qual: *mut pg_sys::List,
    projected_columns: &[&koldstore_migrate::order::CatalogColumn],
    params: pg_sys::ParamListInfo,
) -> Result<Option<PlannedColdSegments>, String> {
    let scope_column_id = snapshot.scope_column_id;
    let segment_order_column_id = snapshot.segment_order_column_id;
    let prune_predicates = retain_pre_merge_cold_prune_predicates(
        unsafe { segment_prune_predicates(scanrelid, qual, &catalog, params) },
        |column_id| {
            let column = catalog
                .column_by_attnum(column_id)?;
            Some(cold_prune_column_policy(
                column,
                scope_column_id,
                segment_order_column_id,
            ))
        },
    );
    let projection_columns = projection_columns(projected_columns, &snapshot.primary_key_columns);
    let mut requested_columns = projection_columns.clone();
    requested_columns.extend(prune_predicates.iter().filter_map(|predicate| {
        catalog
            .column_by_attnum(predicate.column_id)
            .map(|column| ColumnRef::new(column.column_id, column.name.clone()))
    }));
    requested_columns.sort_by_key(|column| column.column_id);
    requested_columns.dedup_by_key(|column| column.column_id);
    let manifest_started = Instant::now();
    let Some(manifest_stats) =
        crate::catalog::cache::cached_manifest_scan_context(table_oid, &requested_columns)?
    else {
        return Ok(None);
    };
    let manifest_read_ms = elapsed_ms(manifest_started);
    let mut indexed_filter_column_ids = catalog
        .primary_key
        .columns
        .iter()
        .chain(catalog.indexed_columns.iter())
        .map(|column| column.column_id.get())
        .collect::<Vec<_>>();
    if let Some(column_id) = scope_column_id {
        indexed_filter_column_ids.push(column_id.get());
    }
    if let Some(column_id) = segment_order_column_id {
        indexed_filter_column_ids.push(column_id.get());
    }
    indexed_filter_column_ids.sort_unstable();
    indexed_filter_column_ids.dedup();
    validate_prune_predicates_indexed(&prune_predicates, &indexed_filter_column_ids)
        .map_err(|error| error.to_string())?;
    let segments_considered = manifest_stats.segments.len();
    let index_started = Instant::now();
    let resolved = resolve_segment_index_candidates(
        table_oid,
        manifest_stats.generation,
        catalog,
        segment_order_column_id,
        &prune_predicates,
    )?;
    let SegmentIndexCandidateResolution {
        candidates: indexed_candidates,
        shape: segment_index_lookup_shape,
        column_id: index_column_id,
        column_name: index_column_name,
        plan: segment_index_plan,
        query: segment_index_query,
    } = resolved;
    let segment_index_lookup_ms = indexed_candidates
        .as_ref()
        .map(|_| elapsed_ms(index_started));
    let segment_index_candidate_segments = indexed_candidates
        .as_ref()
        .map(|candidates| candidates.len());
    let segments = indexed_candidates.unwrap_or_else(|| manifest_stats.segments.clone());
    let segments_pruned_catalog_index = segments_considered.saturating_sub(segments.len());
    let projection = projection_columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let pk_probe = pk_equality_values(&prune_predicates, &snapshot.primary_key_columns);
    let cold_segments_query = koldstore_catalog::queries::plan_in_sync_manifest_scan_context()
        .ok()
        .map(|statement| statement.sql);
    let profile = ColdReadProfile {
        manifest_path: manifest_stats.manifest_path(),
        storage_type: manifest_stats.storage_type.clone(),
        base_path: manifest_stats.base_path.clone(),
        manifest_read_ms: Some(manifest_read_ms),
        segments_considered,
        segments_pruned_catalog_index,
        segments_opened: segments.len(),
        segment_index_order_column_id: index_column_id,
        segment_index_order_column: index_column_name,
        segment_index_lookup_shape: Some(segment_index_lookup_shape),
        segment_index_plan,
        segment_index_lookup_ms,
        segment_index_candidate_segments,
        cold_segments_query,
        segment_index_query,
        pk_probe: pk_probe
            .as_ref()
            .map(|(column, values)| (column.name.clone(), values.clone())),
        projected_columns: projection,
        segments: vec![],
    };
    Ok(Some(PlannedColdSegments {
        profile,
        storage_type: manifest_stats.storage_type.clone(),
        base_path: manifest_stats.base_path.clone(),
        credentials: manifest_stats.credentials.clone(),
        config: manifest_stats.config.clone(),
        segments,
        projection_columns,
        pk_probe,
    }))
}

/// Result of choosing a prune column and loading catalog index candidates.
struct SegmentIndexCandidateResolution {
    candidates: Option<Vec<SegmentStatsHint>>,
    shape: SegmentIndexLookupShape,
    column_id: Option<i16>,
    column_name: Option<String>,
    plan: Option<String>,
    /// SPI SQL text for the cold_segment_index lookup, when one ran.
    query: Option<String>,
}

/// Result of one SPI segment-index candidate lookup for a fixed column.
struct SegmentIndexCandidateLoad {
    candidates: Option<Vec<SegmentStatsHint>>,
    shape: SegmentIndexLookupShape,
    plan: Option<String>,
    /// SPI SQL text executed for this lookup.
    query: Option<String>,
}

/// Builds the pre-merge prune policy for one catalog column.
fn cold_prune_column_policy(
    column: &koldstore_migrate::order::CatalogColumn,
    scope_column_id: Option<ColumnId>,
    segment_order_column_id: Option<ColumnId>,
) -> ColdPruneColumnPolicy {
    ColdPruneColumnPolicy {
        is_primary_key: column.is_primary_key,
        is_scope: scope_column_id.is_some_and(|id| id == column.column_id),
        is_order_column: segment_order_column_id.is_some_and(|id| id == column.column_id),
        sort_key_indexable: koldstore_sortkey::SortKeyType::from_type_oid(
            column.pg_type.type_oid(),
        )
        .is_some(),
    }
}

/// Picks a Sort Key–allowlisted prune column and asks Postgres for candidates.
///
/// Prefers the configured `segment_order_column_id` when it has a range/equality
/// predicate; otherwise uses the first allowlisted prune predicate. Falls back
/// to the full active segment list when no indexable predicate exists.
fn resolve_segment_index_candidates(
    table_oid: pg_sys::Oid,
    manifest_generation: u64,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    segment_order_column_id: Option<ColumnId>,
    predicates: &[SegmentPrunePredicate],
) -> Result<SegmentIndexCandidateResolution, String> {
    let preferred = segment_order_column_id.and_then(|column_id| {
        catalog
            .column_by_id(column_id)
            .filter(|column| {
                koldstore_sortkey::SortKeyType::from_type_oid(column.pg_type.type_oid()).is_some()
                    && predicates
                        .iter()
                        .any(|predicate| predicate.column_id == column.column_id.get())
            })
    });
    let column = preferred.or_else(|| {
        predicates.iter().find_map(|predicate| {
            catalog.column_by_attnum(predicate.column_id).filter(|column| {
                koldstore_sortkey::SortKeyType::from_type_oid(column.pg_type.type_oid()).is_some()
            })
        })
    });
    let Some(column) = column else {
        let order_name = segment_order_column_id.and_then(|column_id| {
            catalog
                .column_by_id(column_id)
                .map(|column| column.name.clone())
        });
        return Ok(SegmentIndexCandidateResolution {
            candidates: None,
            shape: SegmentIndexLookupShape::AllActive,
            column_id: segment_order_column_id.map(ColumnId::get),
            column_name: order_name,
            plan: None,
            query: None,
        });
    };
    let loaded =
        load_segment_index_candidates(table_oid, manifest_generation, catalog, column, predicates)?;
    Ok(SegmentIndexCandidateResolution {
        candidates: loaded.candidates,
        shape: loaded.shape,
        column_id: Some(column.column_id.get()),
        column_name: Some(column.name.clone()),
        plan: loaded.plan,
        query: loaded.query,
    })
}

fn load_segment_index_candidates(
    table_oid: pg_sys::Oid,
    manifest_generation: u64,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    column: &koldstore_migrate::order::CatalogColumn,
    predicates: &[SegmentPrunePredicate],
) -> Result<SegmentIndexCandidateLoad, String> {
    use pgrx::datum::DatumWithOid;

    let encoded_bounds = encode_prune_predicate_bounds(catalog, predicates)?;
    let (lower, upper) = encoded_bounds
        .get(&column.column_id.get())
        .cloned()
        .unwrap_or((None, None));
    let (statement, shape) = match (&lower, &upper) {
        (Some(_), Some(_)) => (
            koldstore_catalog::queries::plan_cold_segment_candidates_closed_range(),
            SegmentIndexLookupShape::BoundedRange,
        ),
        (Some(_), None) => (
            koldstore_catalog::queries::plan_cold_segment_candidates_lower_bound(),
            SegmentIndexLookupShape::LowerBound,
        ),
        (None, Some(_)) => (
            koldstore_catalog::queries::plan_cold_segment_candidates_upper_bound(),
            SegmentIndexLookupShape::UpperBound,
        ),
        (None, None) => {
            return Ok(SegmentIndexCandidateLoad {
                candidates: None,
                shape: SegmentIndexLookupShape::AllActive,
                plan: None,
                query: None,
            })
        }
    };
    let statement = statement.map_err(|error| error.to_string())?;
    let query = Some(statement.sql.clone());
    let mut args = vec![
        DatumWithOid::from(table_oid),
        DatumWithOid::from(""),
        DatumWithOid::from(i32::from(column.column_id.get())),
        DatumWithOid::from(pg_sys::Oid::from(column.pg_type.type_oid())),
        DatumWithOid::from(i32::from(koldstore_sortkey::CODEC_VERSION)),
    ];
    if let Some(value) = &lower {
        args.push(DatumWithOid::from(value.clone()));
    }
    if let Some(value) = &upper {
        args.push(DatumWithOid::from(value.clone()));
    }

    // Report the index PostgreSQL is expected to prefer for this bound shape.
    // The SQL never forces an index (no HINT / BitmapAnd); the planner may still
    // choose seq_scan or BitmapAnd when cheaper. SPI EXPLAIN is intentionally
    // avoided here — nested EXPLAIN is rejected inside non-volatile function
    // contexts during ordinary SELECTs.
    let plan = Some(preferred_segment_index_access(shape).to_string());

    let loaded_candidates: Vec<SegmentIndexCandidateSpiRow> =
        crate::catalog::owner::with_extension_owner(|| {
            crate::spi::execute_prepared(&statement, &args, |tuples| {
                tuples
                    .into_iter()
                    .map(|tuple| {
                        let object_path = tuple
                            .get::<String>(1)?
                            .ok_or_else(|| missing_candidate_field("path"))?;
                        let byte_size = tuple
                            .get::<i64>(2)?
                            .and_then(|value| u64::try_from(value).ok());
                        let schema_version = tuple
                            .get::<i32>(3)?
                            .ok_or_else(|| missing_candidate_field("schema_version"))?;
                        let min_seq_raw = tuple
                            .get::<i64>(4)?
                            .ok_or_else(|| missing_candidate_field("min_seq"))?;
                        let max_seq_raw = tuple
                            .get::<i64>(5)?
                            .ok_or_else(|| missing_candidate_field("max_seq"))?;
                        let physical_names = tuple
                            .get::<String>(6)?
                            .map(|json| serde_json::from_str(&json).unwrap_or_default())
                            .unwrap_or_default();
                        let segment_id = tuple
                            .get::<pgrx::Uuid>(7)?
                            .map(crate::spi::uuid_from_pgrx)
                            .ok_or_else(|| missing_candidate_field("segment_id"))?;
                        let column_id = tuple
                            .get::<i16>(8)?
                            .ok_or_else(|| missing_candidate_field("column_id"))?;
                        let row_group_count = tuple
                            .get::<i32>(9)?
                            .and_then(|value| usize::try_from(value).ok())
                            .ok_or_else(|| missing_candidate_field("row_group_count"))?;
                        let row_group_row_counts = tuple
                            .get::<Vec<i64>>(10)?
                            .ok_or_else(|| missing_candidate_field("row_group_row_counts"))?;
                        let row_group_min_values = tuple
                            .get::<pgrx::Array<&[u8]>>(11)?
                            .ok_or_else(|| missing_candidate_field("row_group_min_values"))?
                            .iter()
                            .map(|value| value.map(<[u8]>::to_vec))
                            .collect::<Vec<_>>();
                        let row_group_max_values = tuple
                            .get::<pgrx::Array<&[u8]>>(12)?
                            .ok_or_else(|| missing_candidate_field("row_group_max_values"))?
                            .iter()
                            .map(|value| value.map(<[u8]>::to_vec))
                            .collect::<Vec<_>>();
                        let row_group_null_counts = tuple
                            .get::<Vec<Option<i64>>>(13)?
                            .ok_or_else(|| missing_candidate_field("row_group_null_counts"))?;
                        let min_seq = SeqId::new(min_seq_raw).map_err(|error| {
                            pgrx::spi::SpiError::DatumError(
                                pgrx::datum::TryFromDatumError::NoSuchAttributeName(format!(
                                    "invalid min_seq {min_seq_raw}: {error}"
                                )),
                            )
                        })?;
                        let max_seq = SeqId::new(max_seq_raw).map_err(|error| {
                            pgrx::spi::SpiError::DatumError(
                                pgrx::datum::TryFromDatumError::NoSuchAttributeName(format!(
                                    "invalid max_seq {max_seq_raw}: {error}"
                                )),
                            )
                        })?;
                        Ok((
                            segment_id,
                            SegmentStatsHint {
                                object_path,
                                schema_version,
                                physical_names,
                                byte_size,
                                min_seq,
                                max_seq,
                                selected_row_groups: None,
                            },
                            column_id,
                            std::sync::Arc::new(crate::catalog::cache::CachedPackedRowGroupIndex {
                                row_group_count,
                                row_group_row_counts: row_group_row_counts.into(),
                                row_group_min_values: row_group_min_values.into(),
                                row_group_max_values: row_group_max_values.into(),
                                row_group_null_counts: row_group_null_counts.into(),
                            }),
                        ))
                    })
                    .collect()
            })
            .map_err(|error| error.to_string())
        })??;
    let mut candidates = Vec::with_capacity(loaded_candidates.len());
    for (segment_id, hint, column_id, packed_index) in loaded_candidates {
        let key = crate::catalog::cache::PackedRowGroupCacheKey::new(
            table_oid.to_u32(),
            manifest_generation,
            segment_id,
            column_id,
        );
        crate::catalog::cache::cache_packed_row_group_index(key, Some(packed_index));
        candidates.push((segment_id, hint));
    }
    let candidates =
        refine_candidate_row_groups(table_oid, manifest_generation, candidates, &encoded_bounds)?
            .into_iter()
            .filter(|candidate| {
                candidate
                    .selected_row_groups
                    .as_ref()
                    .is_none_or(|row_groups| !row_groups.is_empty())
            })
            .collect();
    Ok(SegmentIndexCandidateLoad {
        candidates: Some(candidates),
        shape,
        plan,
        query,
    })
}

type EncodedPredicateBounds = BTreeMap<i16, (Option<Vec<u8>>, Option<Vec<u8>>)>;

fn encode_prune_predicate_bounds(
    catalog: &koldstore_migrate::ExistingTableCatalog,
    predicates: &[SegmentPrunePredicate],
) -> Result<EncodedPredicateBounds, String> {
    let mut bounds = EncodedPredicateBounds::new();
    for predicate in predicates {
        let Some(column) = catalog.column_by_attnum(predicate.column_id) else {
            continue;
        };
        let Some(expected) =
            koldstore_sortkey::SortKeyType::from_type_oid(column.pg_type.type_oid())
        else {
            continue;
        };
        let entry = bounds.entry(predicate.column_id).or_default();
        if let Some(value) = predicate.min.as_ref() {
            if value.sort_key_type() != expected {
                return Err(format!(
                    "prune lower bound type mismatch for column `{}`",
                    predicate.column
                ));
            }
            let encoded = koldstore_sortkey::encode_sort_key(value)
                .map_err(|error| error.to_string())?;
            if entry.0.as_ref().is_none_or(|current| encoded > *current) {
                entry.0 = Some(encoded);
            }
        }
        if let Some(value) = predicate.max.as_ref() {
            if value.sort_key_type() != expected {
                return Err(format!(
                    "prune upper bound type mismatch for column `{}`",
                    predicate.column
                ));
            }
            let encoded = koldstore_sortkey::encode_sort_key(value)
                .map_err(|error| error.to_string())?;
            if entry.1.as_ref().is_none_or(|current| encoded < *current) {
                entry.1 = Some(encoded);
            }
        }
    }
    Ok(bounds)
}

fn refine_candidate_row_groups(
    table_oid: pg_sys::Oid,
    manifest_generation: u64,
    mut candidates: Vec<(uuid::Uuid, SegmentStatsHint)>,
    encoded_bounds: &EncodedPredicateBounds,
) -> Result<Vec<SegmentStatsHint>, String> {
    use pgrx::datum::DatumWithOid;

    if candidates.is_empty() || encoded_bounds.is_empty() {
        return Ok(candidates.into_iter().map(|(_, hint)| hint).collect());
    }
    let segment_ids = candidates
        .iter()
        .map(|(segment_id, _)| *segment_id)
        .collect::<Vec<_>>();
    let column_ids = encoded_bounds.keys().copied().collect::<Vec<_>>();
    let candidate_positions = candidates
        .iter()
        .enumerate()
        .map(|(position, (segment_id, _))| (*segment_id, position))
        .collect::<BTreeMap<_, _>>();
    let table_oid_u32 = table_oid.to_u32();
    let mut packed_indexes = BTreeMap::new();
    let mut missing_segments = BTreeMap::new();
    for segment_id in &segment_ids {
        for &column_id in &column_ids {
            let key = crate::catalog::cache::PackedRowGroupCacheKey::new(
                table_oid_u32,
                manifest_generation,
                *segment_id,
                column_id,
            );
            match crate::catalog::cache::cached_packed_row_group_index(&key) {
                Some(Some(index)) => {
                    packed_indexes.insert((*segment_id, column_id), index);
                }
                Some(None) => {}
                None => {
                    missing_segments.insert(*segment_id, ());
                }
            }
        }
    }

    if !missing_segments.is_empty() {
        let statement = koldstore_catalog::queries::plan_cold_segment_candidate_row_group_indexes()
            .map_err(|error| error.to_string())?;
        let args = [
            DatumWithOid::from(table_oid),
            DatumWithOid::from(""),
            DatumWithOid::from(
                missing_segments
                    .keys()
                    .copied()
                    .map(crate::spi::uuid_to_pgrx)
                    .collect::<Vec<_>>(),
            ),
            DatumWithOid::from(column_ids.clone()),
        ];
        crate::catalog::cache::record_packed_row_group_spi_load();
        let packed_rows: Vec<PackedRowGroupSpiRow> =
            crate::catalog::owner::with_extension_owner(|| {
                crate::spi::execute_prepared(&statement, &args, |tuples| {
                    tuples
                        .into_iter()
                        .map(|tuple| {
                            Ok((
                                tuple
                                    .get::<pgrx::Uuid>(1)?
                                    .map(crate::spi::uuid_from_pgrx)
                                    .ok_or_else(|| missing_candidate_field("segment_id"))?,
                                tuple
                                    .get::<i16>(2)?
                                    .ok_or_else(|| missing_candidate_field("column_id"))?,
                                tuple
                                    .get::<i32>(3)?
                                    .ok_or_else(|| missing_candidate_field("row_group_count"))?,
                                tuple.get::<Vec<i64>>(4)?.ok_or_else(|| {
                                    missing_candidate_field("row_group_row_counts")
                                })?,
                                tuple
                                    .get::<pgrx::Array<&[u8]>>(5)?
                                    .ok_or_else(|| missing_candidate_field("row_group_min_values"))?
                                    .iter()
                                    .map(|value| value.map(<[u8]>::to_vec))
                                    .collect(),
                                tuple
                                    .get::<pgrx::Array<&[u8]>>(6)?
                                    .ok_or_else(|| missing_candidate_field("row_group_max_values"))?
                                    .iter()
                                    .map(|value| value.map(<[u8]>::to_vec))
                                    .collect(),
                                tuple.get::<Vec<Option<i64>>>(7)?.ok_or_else(|| {
                                    missing_candidate_field("row_group_null_counts")
                                })?,
                            ))
                        })
                        .collect()
                })
                .map_err(|error| error.to_string())
            })??;

        for (
            segment_id,
            column_id,
            row_group_count,
            row_group_row_counts,
            row_group_min_values,
            row_group_max_values,
            row_group_null_counts,
        ) in packed_rows
        {
            if !missing_segments.contains_key(&segment_id) {
                return Err(format!(
                    "packed row-group metadata returned unknown segment `{segment_id}`"
                ));
            }
            let index = std::sync::Arc::new(crate::catalog::cache::CachedPackedRowGroupIndex {
                row_group_count: usize::try_from(row_group_count)
                    .map_err(|error| error.to_string())?,
                row_group_row_counts: row_group_row_counts.into(),
                row_group_min_values: row_group_min_values.into(),
                row_group_max_values: row_group_max_values.into(),
                row_group_null_counts: row_group_null_counts.into(),
            });
            let key = crate::catalog::cache::PackedRowGroupCacheKey::new(
                table_oid_u32,
                manifest_generation,
                segment_id,
                column_id,
            );
            crate::catalog::cache::cache_packed_row_group_index(key, Some(index.clone()));
            packed_indexes.insert((segment_id, column_id), index);
        }

        for (segment_id, ()) in missing_segments {
            for &column_id in &column_ids {
                if packed_indexes.contains_key(&(segment_id, column_id)) {
                    continue;
                }
                let key = crate::catalog::cache::PackedRowGroupCacheKey::new(
                    table_oid_u32,
                    manifest_generation,
                    segment_id,
                    column_id,
                );
                crate::catalog::cache::cache_packed_row_group_index(key, None);
            }
        }
    }

    for ((segment_id, column_id), index) in packed_indexes {
        let Some((lower, upper)) = encoded_bounds.get(&column_id) else {
            continue;
        };
        let Some(&position) = candidate_positions.get(&segment_id) else {
            continue;
        };
        let selected = koldstore_catalog::select_packed_row_groups(
            index.row_group_count,
            &index.row_group_row_counts,
            &index.row_group_min_values,
            &index.row_group_max_values,
            &index.row_group_null_counts,
            lower.as_deref(),
            upper.as_deref(),
        )?;
        let hint = &mut candidates[position].1;
        let retained = hint
            .selected_row_groups
            .get_or_insert_with(|| (0..index.row_group_count).collect());
        retained.retain(|row_group_id| selected.binary_search(row_group_id).is_ok());
    }

    Ok(candidates.into_iter().map(|(_, hint)| hint).collect())
}

fn missing_candidate_field(name: &str) -> pgrx::spi::SpiError {
    pgrx::spi::SpiError::DatumError(pgrx::datum::TryFromDatumError::NoSuchAttributeName(
        name.to_string(),
    ))
}

/// Planned cold profile for EXPLAIN without opening Parquet files.
pub(super) fn planned_cold_read_profile(table_oid: pg_sys::Oid) -> Result<ColdReadProfile, String> {
    with_hook_disabled(|| {
        let Some(manifest_stats) =
            crate::catalog::cache::cached_manifest_scan_context(table_oid, &[])?
        else {
            return Ok(ColdReadProfile::empty("(none)"));
        };
        let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
            .map_err(|error| error.to_string())?;
        let segment_index_order_column_id = snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.segment_order_column_id)
            .map(ColumnId::get);
        Ok(ColdReadProfile {
            manifest_path: manifest_stats.manifest_path(),
            storage_type: manifest_stats.storage_type.clone(),
            base_path: manifest_stats.base_path.clone(),
            manifest_read_ms: None,
            segments_considered: manifest_stats.segments.len(),
            segments_pruned_catalog_index: 0,
            segments_opened: manifest_stats.segments.len(),
            segment_index_order_column_id,
            segment_index_order_column: None,
            segment_index_lookup_shape: segment_index_order_column_id
                .map(|_| SegmentIndexLookupShape::AllActive),
            segment_index_plan: None,
            segment_index_lookup_ms: None,
            segment_index_candidate_segments: None,
            cold_segments_query: koldstore_catalog::queries::plan_in_sync_manifest_scan_context()
                .ok()
                .map(|statement| statement.sql),
            segment_index_query: None,
            pk_probe: None,
            projected_columns: Vec::new(),
            segments: manifest_stats
                .segments
                .iter()
                .map(|segment| SegmentReadProfile {
                    object_path: segment.object_path.clone(),
                    row_count: 0,
                    read_ms: None,
                    byte_size: segment.byte_size,
                    parquet: None,
                })
                .collect(),
        })
    })
}

fn projection_columns(
    projected: &[&koldstore_migrate::order::CatalogColumn],
    primary_key_columns: &[ColumnRef],
) -> Vec<ColumnRef> {
    let mut columns = projected
        .iter()
        .map(|column| ColumnRef::new(column.column_id, column.name.clone()))
        .collect::<Vec<_>>();
    for pk in primary_key_columns {
        if !columns
            .iter()
            .any(|column| column.column_id == pk.column_id)
        {
            columns.push(pk.clone());
        }
    }
    columns
}

/// Extracts a single-column PK equality probe for Parquet bloom/min-max pruning.
///
/// Only fires for single-column PKs with an equality predicate (`min == max`).
/// Composite PKs keep the conservative full-segment read until multi-column
/// bloom probing is wired.
fn pk_equality_values(
    predicates: &[SegmentPrunePredicate],
    primary_key_columns: &[ColumnRef],
) -> Option<(ColumnRef, Vec<String>)> {
    if primary_key_columns.len() != 1 {
        return None;
    }
    let pk = &primary_key_columns[0];
    let predicate = predicates.iter().find(|predicate| {
        predicate.column_id == pk.column_id.get()
            && predicate.min.is_some()
            && predicate.max.is_some()
            && predicate.min == predicate.max
    })?;
    let value = predicate.min.as_ref()?;
    let literal = match value {
        koldstore_sortkey::SortKeyValue::Bool(flag) => flag.to_string(),
        koldstore_sortkey::SortKeyValue::Int2(n) => n.to_string(),
        koldstore_sortkey::SortKeyValue::Int4(n) => n.to_string(),
        koldstore_sortkey::SortKeyValue::Int8(n) => n.to_string(),
        koldstore_sortkey::SortKeyValue::Date(n)
        | koldstore_sortkey::SortKeyValue::Timestamp(n)
        | koldstore_sortkey::SortKeyValue::Timestamptz(n) => n.to_string(),
        koldstore_sortkey::SortKeyValue::Uuid(uuid) => uuid.to_string(),
    };
    Some((pk.clone(), vec![literal]))
}

fn cold_rows_from_segments(
    client: &koldstore_storage::ObjectStoreClient,
    segment_hints: &[SegmentStatsHint],
    projected_columns: &[ColumnRef],
    catalog_columns: &[koldstore_migrate::order::CatalogColumn],
    primary_key_columns: &[ColumnRef],
    current_schema_version: i32,
    pk_probe: Option<(ColumnRef, Vec<String>)>,
) -> Result<(Vec<ColdRow>, Vec<SegmentReadProfile>), String> {
    // One ObjectStore client for all segments (filesystem or S3). Parquet reads
    // are footer-first with range GETs — no full-object download. Known
    // `byte_size` enables bounded footer ranges (avoids suffix GETs on S3).
    let store = client.store();
    let mut rows = Vec::new();
    let mut segments = Vec::with_capacity(segment_hints.len());
    for hint in segment_hints {
        // Columns added after a segment was written have no physical field in
        // that schema version; omit them from the Parquet projection and fill
        // NULL after remap. Renames keep the same column_id with a different
        // physical name in historical_schema.
        let physical_names = projected_columns
            .iter()
            .filter_map(|column| {
                physical_name_for_segment(column, hint, current_schema_version)
                    .transpose()
                    .map(|result| result.map(|physical_name| (column.clone(), physical_name)))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let missing_logical_names = projected_columns
            .iter()
            .filter(|column| {
                !physical_names
                    .iter()
                    .any(|(present, _)| present.column_id == column.column_id)
            })
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let columns = physical_names
            .iter()
            .map(|(column, physical_name)| {
                let catalog_column = catalog_columns
                    .iter()
                    .find(|candidate| candidate.column_id == column.column_id)
                    .ok_or_else(|| {
                        format!(
                            "catalog is missing projected column_id {}",
                            column.column_id
                        )
                    })?;
                Ok(PgColumn::new(
                    physical_name.clone(),
                    catalog_column.pg_type,
                    true,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let physical_pk_names = primary_key_columns
            .iter()
            .map(|column| {
                physical_name_for_segment(column, hint, current_schema_version)?.ok_or_else(|| {
                    format!(
                        "schema version {} is missing primary-key column_id {}",
                        hint.schema_version, column.column_id
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut options = ParquetReadOptions::new()
            .with_columns(
                physical_names
                    .iter()
                    .map(|(_, name)| name.clone())
                    .collect::<Vec<_>>(),
            )
            .with_timeout(client.timeout());
        if let Some(row_groups) = &hint.selected_row_groups {
            options = options.with_row_groups(row_groups.iter().copied());
        }
        if let Some((pk_column, values)) = &pk_probe {
            let physical_pk = physical_name_for_segment(pk_column, hint, current_schema_version)?
                .ok_or_else(|| {
                format!(
                    "schema version {} is missing primary-key column_id {}",
                    hint.schema_version, pk_column.column_id
                )
            })?;
            options = options.with_pk_values(physical_pk, values.clone());
        }
        let started = Instant::now();
        let _permit = crate::merge_scan::reader_pool::try_acquire_parquet_reader_permit(
            crate::guc::max_open_parquet_readers(),
        )?;
        let (segment_rows, parquet_profile) = read_clean_cold_rows_from_object_store_with_size(
            std::sync::Arc::clone(&store),
            &hint.object_path,
            hint.byte_size,
            &columns,
            &physical_pk_names,
            &options,
        )?;
        segments.push(SegmentReadProfile {
            object_path: hint.object_path.clone(),
            row_count: segment_rows.len(),
            read_ms: Some(elapsed_ms(started)),
            byte_size: hint.byte_size.or(parquet_profile.file_size),
            parquet: Some(parquet_profile),
        });
        let logical_pk_names = primary_key_columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        for mut row in segment_rows {
            remap_json_to_logical_names(&mut row.pk_json, &physical_names);
            remap_row_to_logical_names(&mut row.row_image, &physical_names);
            fill_missing_logical_nulls(&mut row.row_image, &missing_logical_names);
            rows.push(clean_cold_row_to_common(row, &logical_pk_names)?);
        }
    }
    Ok((rows, segments))
}

/// Resolves the Parquet field name for a logical column in one segment schema.
///
/// Returns `Ok(None)` when the column was added after the segment was written.
///
/// # Errors
///
/// Returns an error when a renamed/historical column ID is expected but the
/// segment schema map is incomplete for a non-current schema version.
fn physical_name_for_segment(
    column: &ColumnRef,
    hint: &SegmentStatsHint,
    current_schema_version: i32,
) -> Result<Option<String>, String> {
    Ok(koldstore_merge::scan::physical_name_for_segment_column(
        column.column_id.get(),
        &column.name,
        hint,
        current_schema_version,
    ))
}

fn remap_json_to_logical_names(
    value: &mut serde_json::Value,
    physical_names: &[(ColumnRef, String)],
) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    for (column, physical_name) in physical_names {
        if physical_name == &column.name {
            continue;
        }
        if let Some(value) = object.remove(physical_name) {
            object.insert(column.name.clone(), value);
        }
    }
}

fn remap_row_to_logical_names(
    image: &mut koldstore_common::RowImage,
    physical_names: &[(ColumnRef, String)],
) {
    let cells = image.cells_mut();
    for (column, physical_name) in physical_names {
        if physical_name == &column.name {
            continue;
        }
        if let Some(value) = cells.remove(physical_name) {
            cells.insert(column.name.clone(), value);
        }
    }
}

fn fill_missing_logical_nulls(image: &mut koldstore_common::RowImage, missing_names: &[String]) {
    for name in missing_names {
        if !image.contains_key(name) {
            image.insert(name.clone(), koldstore_common::CellValue::Null);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use koldstore_catalog::{preferred_segment_index_access, SegmentIndexLookupShape};
    use koldstore_common::{ColumnId, ColumnRef, SeqId};
    use koldstore_merge::scan::plan::SegmentStatsHint;
    use koldstore_schema::PgType;
    use koldstore_sortkey::SortKeyType;

    use super::{cold_prune_column_policy, physical_name_for_segment};

    #[test]
    fn scope_prune_policy_matches_stable_column_id_after_rename() {
        let column = koldstore_migrate::order::CatalogColumn::bigint(4, "renamed_tenant");

        let policy = cold_prune_column_policy(&column, Some(ColumnId::from_attnum(4)), None);

        assert!(policy.is_scope);
    }

    #[test]
    fn text_like_types_are_not_sort_key_indexable() {
        assert!(SortKeyType::from_type_oid(PgType::Text.type_oid()).is_none());
        assert!(SortKeyType::from_type_oid(PgType::TextArray.type_oid()).is_none());
        assert!(SortKeyType::from_type_oid(PgType::Int8.type_oid()).is_some());
        assert!(SortKeyType::from_type_oid(PgType::Uuid.type_oid()).is_some());
        assert!(SortKeyType::from_type_oid(PgType::Timestamptz.type_oid()).is_some());
    }

    #[test]
    fn segment_index_preferred_access_matches_bound_shape() {
        assert_eq!(
            preferred_segment_index_access(SegmentIndexLookupShape::LowerBound),
            "max_idx"
        );
        assert_eq!(
            preferred_segment_index_access(SegmentIndexLookupShape::UpperBound),
            "min_idx"
        );
        assert_eq!(
            preferred_segment_index_access(SegmentIndexLookupShape::BoundedRange),
            "bitmap_and_or_single"
        );
    }

    #[test]
    fn historical_segment_resolves_physical_name_by_stable_column_id() {
        let column = ColumnRef::new(ColumnId::from_attnum(7), "renamed_body");
        let hint = SegmentStatsHint {
            object_path: "app/items/old.parquet".to_string(),
            schema_version: 1,
            physical_names: BTreeMap::from([(7, "body".to_string())]),
            byte_size: None,
            min_seq: SeqId::new(1).unwrap(),
            max_seq: SeqId::new(10).unwrap(),
            selected_row_groups: None,
        };

        assert_eq!(
            physical_name_for_segment(&column, &hint, 2).unwrap(),
            Some("body".to_string())
        );
        let added = ColumnRef::new(ColumnId::from_attnum(9), "note");
        assert_eq!(physical_name_for_segment(&added, &hint, 2).unwrap(), None);
    }
}
