//! Cold Parquet load and segment pruning for KoldMergeScan.

use std::time::Instant;

use koldstore_common::{ColdRow, ColumnId, ColumnRef};
use koldstore_merge::scan::plan::{
    retain_pre_merge_cold_prune_predicates, validate_prune_predicates_indexed,
    ColdPruneColumnPolicy, SegmentPrunePredicate, SegmentStatsHint,
};
use koldstore_parquet::{
    clean_cold_row_to_common, read_clean_cold_rows_from_object_store_with_size, ParquetReadOptions,
    PgColumn,
};
use koldstore_schema::PgType;
use koldstore_storage::open_client_from_catalog_fields;
use pgrx::pg_sys;

use super::profile::{elapsed_ms, ColdReadProfile, SegmentReadProfile};
use super::qual::segment_prune_predicates;
use super::with_hook_disabled;

/// Loads cold rows for merge, applying catalog prune and Parquet projection.
pub(super) fn load_cold_rows_for_merge(
    table_oid: pg_sys::Oid,
    scanrelid: pg_sys::Index,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    qual: *mut pg_sys::List,
    projected_columns: &[&koldstore_migrate::order::CatalogColumn],
    params: pg_sys::ParamListInfo,
) -> Result<(ColdReadProfile, Vec<ColdRow>), String> {
    with_hook_disabled(|| {
        // Pre-merge prune is limited to PK + scope. Mutable columns stay residual
        // so an older cold version cannot resurrect after its newer segment is
        // pruned away. Scope uses catalog segment-index bounds on the shared
        // manifest today (`scope_key = ''`); later each scope_id gets its own
        // manifest/folder and listing filters by scope_key first.
        let scope_column = snapshot.scope_column.as_deref();
        let segment_order_column_id = snapshot.segment_order_column_id;
        let prune_predicates = retain_pre_merge_cold_prune_predicates(
            unsafe {
                segment_prune_predicates(table_oid, scanrelid, qual, &catalog.columns, params)
            },
            |column_id| {
                let column = catalog
                    .columns
                    .iter()
                    .find(|column| column.column_id.get() == column_id)?;
                Some(cold_prune_column_policy(
                    column,
                    scope_column,
                    segment_order_column_id,
                ))
            },
        );
        let projection_columns =
            projection_columns(projected_columns, &snapshot.primary_key_columns);
        let mut requested_columns = projection_columns.clone();
        requested_columns.extend(prune_predicates.iter().filter_map(|predicate| {
            catalog
                .columns
                .iter()
                .find(|column| column.column_id.get() == predicate.column_id)
                .map(|column| ColumnRef::new(column.column_id, column.name.clone()))
        }));
        requested_columns.sort_by_key(|column| column.column_id);
        requested_columns.dedup_by_key(|column| column.column_id);
        let manifest_started = Instant::now();
        let Some(manifest_stats) =
            crate::catalog::cache::cached_manifest_segment_stats(table_oid, &requested_columns)?
        else {
            return Ok((ColdReadProfile::empty("(none)"), Vec::new()));
        };
        let manifest_read_ms = elapsed_ms(manifest_started);
        // Scope is always eligible for catalog stats prune even when it is not
        // separately listed in indexed_columns (it usually is via secondary indexes).
        let mut indexed_filter_column_ids = catalog
            .primary_key
            .columns
            .iter()
            .chain(catalog.indexed_columns.iter())
            .map(|column| column.column_id.get())
            .collect::<Vec<_>>();
        if let Some(scope) = scope_column {
            indexed_filter_column_ids.extend(
                catalog
                    .columns
                    .iter()
                    .filter(|column| column.name == scope)
                    .map(|column| column.column_id.get()),
            );
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
        let (
            indexed_candidates,
            segment_index_lookup_shape,
            index_column_id,
            index_column_name,
            segment_index_plan,
        ) = resolve_segment_index_candidates(
            table_oid,
            catalog,
            segment_order_column_id,
            &prune_predicates,
        )?;
        let segment_index_lookup_ms = indexed_candidates
            .as_ref()
            .map(|_| elapsed_ms(index_started));
        let segment_index_candidate_segments =
            indexed_candidates.as_ref().map(|candidates| candidates.len());
        let segments = indexed_candidates
            .unwrap_or_else(|| manifest_stats.segments.clone());
        let segments_pruned_catalog_index = segments_considered.saturating_sub(segments.len());
        // Shared-scope catalog SQL still filters `scope_key = ''`. When per-scope
        // manifests land, listing will drop other scopes here and this counter
        // will reflect that primary prune.
        let segments_pruned_scope = 0usize;

        let projection = projection_columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let pk_probe = pk_equality_values(&prune_predicates, &snapshot.primary_key_columns);

        let mut profile = ColdReadProfile {
            manifest_path: manifest_stats.manifest_path.clone(),
            storage_type: manifest_stats.storage_type.clone(),
            base_path: manifest_stats.base_path.clone(),
            manifest_read_ms: Some(manifest_read_ms),
            segments_considered,
            segments_pruned_scope,
            segments_pruned_catalog_index,
            segments_opened: segments.len(),
            segment_index_order_column_id: index_column_id,
            segment_index_order_column: index_column_name,
            segment_index_lookup_shape: Some(segment_index_lookup_shape),
            segment_index_plan,
            segment_index_lookup_ms,
            segment_index_candidate_segments,
            pk_probe: pk_probe
                .as_ref()
                .map(|(column, values)| (column.name.clone(), values.clone())),
            projected_columns: projection.clone(),
            segments: vec![],
        };

        if segments.is_empty() {
            return Ok((profile, Vec::new()));
        }

        if crate::guc::cold_reads_mode() == crate::settings::ColdReadsMode::Off {
            return Err("cold reads are disabled by koldstore.cold_reads".to_string());
        }

        let client = open_client_from_catalog_fields(
            &manifest_stats.storage_type,
            &manifest_stats.base_path,
            &manifest_stats.credentials,
            &manifest_stats.config,
        )
        .map_err(|error| error.to_string())?;
        let (cold_rows, segment_profiles) = cold_rows_from_segments(
            &client,
            &segments,
            &projection_columns,
            &catalog.columns,
            &snapshot.primary_key_columns,
            snapshot.schema_version,
            pk_probe,
        )?;
        profile.segments = segment_profiles;

        Ok((profile, cold_rows))
    })
}

/// Builds the pre-merge prune policy for one catalog column.
fn cold_prune_column_policy(
    column: &koldstore_migrate::order::CatalogColumn,
    scope_column: Option<&str>,
    segment_order_column_id: Option<ColumnId>,
) -> ColdPruneColumnPolicy {
    let ordered_stats_safe = cold_pruning_type_is_collation_independent(column.pg_type);
    ColdPruneColumnPolicy {
        is_primary_key: column.is_primary_key
            || segment_order_column_id.is_some_and(|id| id == column.column_id),
        is_scope: scope_column.is_some_and(|scope| scope == column.name),
        ordered_stats_safe,
        // Text scope ids compare as exact flush-encoded JSON strings.
        equality_stats_safe: ordered_stats_safe || column.pg_type == PgType::Text,
    }
}

/// Whether Sort Key V1 / catalog index pruning has the same order semantics as
/// PostgreSQL for this type (collation-independent scalars only).
const fn cold_pruning_type_is_collation_independent(pg_type: PgType) -> bool {
    matches!(
        pg_type,
        PgType::Bool
            | PgType::Int2
            | PgType::Int4
            | PgType::Int8
            | PgType::Uuid
            | PgType::Timestamptz
    )
}

/// Bound shape used for `koldstore.cold_segment_index` candidate SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SegmentIndexLookupShape {
    BoundedRange,
    LowerBound,
    UpperBound,
    AllActive,
}

impl SegmentIndexLookupShape {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::BoundedRange => "bounded_range",
            Self::LowerBound => "lower_bound",
            Self::UpperBound => "upper_bound",
            Self::AllActive => "all_active",
        }
    }
}

/// Picks a Sort Key–allowlisted prune column and asks Postgres for candidates.
///
/// Prefers the configured `segment_order_column_id` when it has a range/equality
/// predicate; otherwise uses the first allowlisted prune predicate. Falls back
/// to the full active segment list when no indexable predicate exists.
fn resolve_segment_index_candidates(
    table_oid: pg_sys::Oid,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    segment_order_column_id: Option<ColumnId>,
    predicates: &[SegmentPrunePredicate],
) -> Result<
    (
        Option<Vec<SegmentStatsHint>>,
        SegmentIndexLookupShape,
        Option<i16>,
        Option<String>,
        Option<String>,
    ),
    String,
> {
    let preferred = segment_order_column_id.and_then(|column_id| {
        catalog
            .columns
            .iter()
            .find(|column| column.column_id == column_id)
            .filter(|column| {
                koldstore_sortkey::SortKeyType::from_type_oid(column.pg_type.type_oid()).is_some()
                    && predicates
                        .iter()
                        .any(|predicate| predicate.column_id == column.column_id.get())
            })
    });
    let column = preferred.or_else(|| {
        predicates.iter().find_map(|predicate| {
            catalog.columns.iter().find(|column| {
                column.column_id.get() == predicate.column_id
                    && koldstore_sortkey::SortKeyType::from_type_oid(column.pg_type.type_oid())
                        .is_some()
            })
        })
    });
    let Some(column) = column else {
        let order_name = segment_order_column_id.and_then(|column_id| {
            catalog
                .columns
                .iter()
                .find(|column| column.column_id == column_id)
                .map(|column| column.name.clone())
        });
        return Ok((
            None,
            SegmentIndexLookupShape::AllActive,
            segment_order_column_id.map(ColumnId::get),
            order_name,
            None,
        ));
    };
    let (candidates, shape, plan) = load_segment_index_candidates(table_oid, column, predicates)?;
    Ok((
        candidates,
        shape,
        Some(column.column_id.get()),
        Some(column.name.clone()),
        plan,
    ))
}

fn load_segment_index_candidates(
    table_oid: pg_sys::Oid,
    column: &koldstore_migrate::order::CatalogColumn,
    predicates: &[SegmentPrunePredicate],
) -> Result<(Option<Vec<SegmentStatsHint>>, SegmentIndexLookupShape, Option<String>), String> {
    use pgrx::datum::DatumWithOid;

    let Some(sort_type) =
        koldstore_sortkey::SortKeyType::from_type_oid(column.pg_type.type_oid())
    else {
        return Ok((None, SegmentIndexLookupShape::AllActive, None));
    };
    let mut lower = None::<Vec<u8>>;
    let mut upper = None::<Vec<u8>>;
    for predicate in predicates
        .iter()
        .filter(|predicate| predicate.column_id == column.column_id.get())
    {
        if let Some(value) = predicate.min.as_ref() {
            let encoded = koldstore_sortkey::encode_sort_key_json(sort_type, value)
                .map_err(|error| error.to_string())?;
            if lower.as_ref().is_none_or(|current| encoded > *current) {
                lower = Some(encoded);
            }
        }
        if let Some(value) = predicate.max.as_ref() {
            let encoded = koldstore_sortkey::encode_sort_key_json(sort_type, value)
                .map_err(|error| error.to_string())?;
            if upper.as_ref().is_none_or(|current| encoded < *current) {
                upper = Some(encoded);
            }
        }
    }
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
        (None, None) => return Ok((None, SegmentIndexLookupShape::AllActive, None)),
    };
    let statement = statement.map_err(|error| error.to_string())?;
    let mut args = vec![
        DatumWithOid::from(table_oid),
        DatumWithOid::from(""),
        DatumWithOid::from(i32::from(column.column_id.get())),
        DatumWithOid::from(pg_sys::Oid::from(column.pg_type.type_oid())),
        DatumWithOid::from(i32::from(koldstore_sortkey::CODEC_VERSION)),
    ];
    if let Some(value) = lower {
        args.push(DatumWithOid::from(value));
    }
    if let Some(value) = upper {
        args.push(DatumWithOid::from(value));
    }

    // Report the index PostgreSQL is expected to prefer for this bound shape.
    // The SQL never forces an index (no HINT / BitmapAnd); the planner may still
    // choose seq_scan or BitmapAnd when cheaper. SPI EXPLAIN is intentionally
    // avoided here — nested EXPLAIN is rejected inside non-volatile function
    // contexts during ordinary SELECTs.
    let plan = Some(preferred_segment_index_plan(shape).to_string());

    let candidates = crate::catalog::owner::with_extension_owner(|| {
        crate::spi::execute_prepared(&statement, &args, |tuples| {
            tuples
                .into_iter()
                .map(|tuple| {
                    let object_path = tuple
                        .get::<String>(1)?
                        .ok_or_else(|| missing_candidate_field("object_path"))?;
                    let byte_size = tuple
                        .get::<i64>(2)?
                        .and_then(|value| u64::try_from(value).ok());
                    let schema_version = tuple
                        .get::<i32>(3)?
                        .ok_or_else(|| missing_candidate_field("schema_version"))?;
                    let physical_names = tuple
                        .get::<String>(8)?
                        .map(|json| serde_json::from_str(&json).unwrap_or_default())
                        .unwrap_or_default();
                    Ok(SegmentStatsHint {
                        object_path,
                        schema_version,
                        physical_names,
                        byte_size,
                    })
                })
                .collect()
        })
        .map_err(|error| error.to_string())
    })??;
    Ok((Some(candidates), shape, plan))
}

/// Preferred cold_segment_index access path for a bound shape (not forced).
const fn preferred_segment_index_plan(shape: SegmentIndexLookupShape) -> &'static str {
    match shape {
        SegmentIndexLookupShape::BoundedRange => "bitmap_and_or_single",
        SegmentIndexLookupShape::LowerBound => "max_idx",
        SegmentIndexLookupShape::UpperBound => "min_idx",
        SegmentIndexLookupShape::AllActive => "seq_scan",
    }
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
            crate::catalog::cache::cached_manifest_segment_stats(table_oid, &[])?
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
            manifest_path: manifest_stats.manifest_path.clone(),
            storage_type: manifest_stats.storage_type.clone(),
            base_path: manifest_stats.base_path.clone(),
            manifest_read_ms: None,
            segments_considered: manifest_stats.segments.len(),
            segments_pruned_scope: 0,
            segments_pruned_catalog_index: 0,
            segments_opened: manifest_stats.segments.len(),
            segment_index_order_column_id,
            segment_index_order_column: None,
            segment_index_lookup_shape: segment_index_order_column_id
                .map(|_| SegmentIndexLookupShape::AllActive),
            segment_index_plan: None,
            segment_index_lookup_ms: None,
            segment_index_candidate_segments: None,
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
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Number(number) => number.to_string(),
        serde_json::Value::Bool(flag) => flag.to_string(),
        _ => return None,
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
        let mut options = ParquetReadOptions::new().with_columns(
            physical_names
                .iter()
                .map(|(_, name)| name.clone())
                .collect::<Vec<_>>(),
        );
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
            remap_row_to_logical_names(&mut row.pk_json, &physical_names);
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
    if let Some(name) = hint.physical_names.get(&column.column_id.get()) {
        return Ok(Some(name.clone()));
    }
    if hint.schema_version == current_schema_version {
        return Ok(Some(column.name.clone()));
    }
    // Additive columns (and drop+add with a new attnum) are absent from older
    // segment schemas; callers materialize NULL for the current logical name.
    Ok(None)
}

fn remap_row_to_logical_names(
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

fn fill_missing_logical_nulls(value: &mut serde_json::Value, missing_names: &[String]) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    for name in missing_names {
        object
            .entry(name.clone())
            .or_insert(serde_json::Value::Null);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use koldstore_common::{ColumnId, ColumnRef};
    use koldstore_merge::scan::plan::SegmentStatsHint;
    use koldstore_schema::PgType;

    use super::{
        cold_pruning_type_is_collation_independent, physical_name_for_segment,
        preferred_segment_index_plan,
    };

    #[test]
    fn text_like_types_are_not_safe_for_byte_ordered_cold_pruning() {
        assert!(!cold_pruning_type_is_collation_independent(PgType::Text));
        assert!(!cold_pruning_type_is_collation_independent(
            PgType::TextArray
        ));
        assert!(cold_pruning_type_is_collation_independent(PgType::Int8));
        assert!(cold_pruning_type_is_collation_independent(PgType::Uuid));
    }

    #[test]
    fn segment_index_plan_preference_matches_bound_shape() {
        assert_eq!(
            preferred_segment_index_plan(super::SegmentIndexLookupShape::LowerBound),
            "max_idx"
        );
        assert_eq!(
            preferred_segment_index_plan(super::SegmentIndexLookupShape::UpperBound),
            "min_idx"
        );
        assert_eq!(
            preferred_segment_index_plan(super::SegmentIndexLookupShape::BoundedRange),
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
        };

        assert_eq!(
            physical_name_for_segment(&column, &hint, 2).unwrap(),
            Some("body".to_string())
        );
        let added = ColumnRef::new(ColumnId::from_attnum(9), "note");
        assert_eq!(physical_name_for_segment(&added, &hint, 2).unwrap(), None);
    }
}
