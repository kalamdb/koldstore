//! Cold Parquet load and segment pruning for KoldMergeScan.

use std::time::Instant;

use koldstore_common::{dedupe_nonblank, ColdRow};
use koldstore_merge::scan::plan::{
    prune_segment_stats_hints, validate_prune_predicates_indexed, SegmentPrunePredicate,
    SegmentStatsHint,
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
        // Only primary-key predicates are safe before winner resolution. A
        // mutable or security column can differ between older and newer cold
        // versions; pruning its newer segment could resurrect the older row.
        let cold_prunable_primary_keys = catalog
            .columns
            .iter()
            .filter(|column| {
                column.is_primary_key && cold_pruning_type_is_collation_independent(column.pg_type)
            })
            .map(|column| column.name.as_str())
            .collect::<std::collections::HashSet<_>>();
        let prune_predicates = unsafe {
            segment_prune_predicates(table_oid, scanrelid, qual, &catalog.columns, params)
        }
        .into_iter()
        .filter(|predicate| cold_prunable_primary_keys.contains(predicate.column.as_str()))
        .collect::<Vec<_>>();
        let predicate_columns = dedupe_nonblank(
            prune_predicates
                .iter()
                .map(|predicate| predicate.column.as_str()),
        );
        let manifest_started = Instant::now();
        let Some(manifest_stats) =
            crate::catalog::cache::cached_manifest_segment_stats(table_oid, &predicate_columns)?
        else {
            return Ok((ColdReadProfile::empty("(none)"), Vec::new()));
        };
        let manifest_read_ms = elapsed_ms(manifest_started);
        let indexed_filter_columns = dedupe_nonblank(
            catalog
                .primary_key
                .columns
                .iter()
                .map(String::as_str)
                .chain(catalog.indexed_columns.iter().map(String::as_str)),
        );
        validate_prune_predicates_indexed(&prune_predicates, &indexed_filter_columns)
            .map_err(|error| error.to_string())?;
        let segments_considered = manifest_stats.segments.len();
        let segments = prune_segment_stats_hints(&manifest_stats.segments, &prune_predicates);
        let segments_pruned_min_max = segments_considered.saturating_sub(segments.len());
        // Shared-scope catalog SQL already filters `scope_key = ''`; scoped prune
        // counters stay 0 until multi-scope segments are returned to the scan.
        let segments_pruned_scope = 0usize;

        let projection = projection_column_names(projected_columns, &snapshot.primary_key_columns);
        let pk_probe = pk_equality_values(&prune_predicates, &snapshot.primary_key_columns);

        let mut profile = ColdReadProfile {
            manifest_path: manifest_stats.manifest_path.clone(),
            storage_type: manifest_stats.storage_type.clone(),
            base_path: manifest_stats.base_path.clone(),
            manifest_read_ms: Some(manifest_read_ms),
            segments_considered,
            segments_pruned_scope,
            segments_pruned_min_max,
            segments_opened: segments.len(),
            pk_probe: pk_probe.clone(),
            projected_columns: projection.clone(),
            segments: vec![],
        };

        if segments.is_empty() {
            return Ok((profile, Vec::new()));
        }

        if crate::guc::cold_reads_mode() == crate::settings::ColdReadsMode::Off {
            return Err("cold reads are disabled by koldstore.cold_reads".to_string());
        }

        let parquet_columns = catalog
            .columns
            .iter()
            .filter(|column| projection.iter().any(|name| name == &column.name))
            .map(|column| PgColumn::new(column.name.clone(), column.pg_type, true))
            .collect::<Vec<_>>();
        let mut options = ParquetReadOptions::new().with_columns(projection);
        // Point-lookup path: push PK equality into Parquet row-group prune
        // (column-chunk min/max + native bloom filters written on flush).
        if let Some((column, values)) = pk_probe {
            options = options.with_pk_values(column, values);
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
            &parquet_columns,
            &snapshot.primary_key_columns,
            &options,
        )?;
        profile.segments = segment_profiles;

        Ok((profile, cold_rows))
    })
}

/// Whether JSON/Parquet scalar comparison has the same semantics as the
/// PostgreSQL type for conservative cold pruning.
///
/// Text and text arrays are deliberately excluded: PostgreSQL collation can
/// make both range and equality semantics differ from byte-ordered segment
/// stats and bloom filters. Unsupported literal types remain excluded until
/// their exact PostgreSQL ordering is represented in cold metadata.
const fn cold_pruning_type_is_collation_independent(pg_type: PgType) -> bool {
    matches!(
        pg_type,
        PgType::Bool | PgType::Int2 | PgType::Int4 | PgType::Int8 | PgType::Uuid
    )
}

/// Planned cold profile for EXPLAIN without opening Parquet files.
pub(super) fn planned_cold_read_profile(table_oid: pg_sys::Oid) -> Result<ColdReadProfile, String> {
    with_hook_disabled(|| {
        let Some(manifest_stats) =
            crate::catalog::cache::cached_manifest_segment_stats(table_oid, &[])?
        else {
            return Ok(ColdReadProfile::empty("(none)"));
        };
        Ok(ColdReadProfile {
            manifest_path: manifest_stats.manifest_path.clone(),
            storage_type: manifest_stats.storage_type.clone(),
            base_path: manifest_stats.base_path.clone(),
            manifest_read_ms: None,
            segments_considered: manifest_stats.segments.len(),
            segments_pruned_scope: 0,
            segments_pruned_min_max: 0,
            segments_opened: manifest_stats.segments.len(),
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

fn cold_rows_from_segments(
    client: &koldstore_storage::ObjectStoreClient,
    segment_hints: &[SegmentStatsHint],
    columns: &[PgColumn],
    primary_key_columns: &[String],
    options: &ParquetReadOptions,
) -> Result<(Vec<ColdRow>, Vec<SegmentReadProfile>), String> {
    // One ObjectStore client for all segments (filesystem or S3). Parquet reads
    // are footer-first with range GETs — no full-object download. Known
    // `byte_size` enables bounded footer ranges (avoids suffix GETs on S3).
    let store = client.store();
    let mut rows = Vec::new();
    let mut segments = Vec::with_capacity(segment_hints.len());
    for hint in segment_hints {
        let started = Instant::now();
        let _permit = crate::merge_scan::reader_pool::try_acquire_parquet_reader_permit(
            crate::guc::max_open_parquet_readers(),
        )?;
        let (segment_rows, parquet_profile) = read_clean_cold_rows_from_object_store_with_size(
            std::sync::Arc::clone(&store),
            &hint.object_path,
            hint.byte_size,
            columns,
            primary_key_columns,
            options,
        )?;
        segments.push(SegmentReadProfile {
            object_path: hint.object_path.clone(),
            row_count: segment_rows.len(),
            read_ms: Some(elapsed_ms(started)),
            byte_size: hint.byte_size.or(parquet_profile.file_size),
            parquet: Some(parquet_profile),
        });
        for row in segment_rows {
            rows.push(clean_cold_row_to_common(row, primary_key_columns)?);
        }
    }
    Ok((rows, segments))
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
