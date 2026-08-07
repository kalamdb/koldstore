//! Public change-feed SQL entrypoint (`koldstore.changes_since`).
//!
//! Catalog-routed seq cursor: open at most one cold Parquet segment at a time
//! (stream until `limit`), or read the hot `__cl` mirror with a real `LIMIT`.
//! Pages advance by exclusive `seq`; the same PK may appear again on a later
//! page with a higher seq. Optional `scope_key` is a first-class filter hook.

#[cfg(feature = "pg")]
use crate::object_store::open_managed_object_store_client;
#[cfg(feature = "pg")]
use koldstore_common::{
    scope::active_scope_for_table, ChangeSource, MirrorChange, MirrorOperation, QualifiedTableName,
    ScopeKey, SeqId, SqlParamType, TableKind, TableOid,
};
#[cfg(feature = "pg")]
use koldstore_merge::events::{self, DEFAULT_CHANGE_LIMIT};
#[cfg(feature = "pg")]
use koldstore_merge::scan::SegmentStatsHint;
#[cfg(feature = "pg")]
use koldstore_parquet::{
    read_clean_cold_rows_from_object_store_with_size, CleanColdRow, ParquetReadOptions, PgColumn,
};
#[cfg(feature = "pg")]
use pgrx::datum::DatumWithOid;
#[cfg(feature = "pg")]
use pgrx::iter::TableIterator;
#[cfg(feature = "pg")]
use pgrx::prelude::*;

/// Change event returned by `koldstore.changes_since`.
///
/// SQL contract (KalamDB subscribe-compatible):
/// ```sql
/// koldstore.changes_since(
///   table_name regclass,
///   since_seq  bigint  DEFAULT 0,      -- exclusive resume cursor (`from`)
///   limit_rows integer DEFAULT 1000,   -- forward page size (`batch_size`)
///   last_rows  integer DEFAULT NULL    -- newest-N rewind when since_seq is 0
/// )
/// ```
///
/// Precedence matches KalamDB: when `since_seq > 0`, resume mode wins and
/// `last_rows` is ignored. Otherwise `last_rows` rewinds to the newest N
/// retained changes (delivered oldest→newest). `since_seq = 0` with no
/// `last_rows` means from the start of retained history. Pages advance by
/// exclusive `seq`; the same PK may appear again later with a higher seq.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(
    name = "changes_since",
    schema = "koldstore",
    security_definer,
    parallel_safe,
    stable
)]
fn changes_since_pg(
    table_name: pgrx::PgRelation,
    since_seq: default!(i64, 0),
    limit_rows: default!(i32, 1000),
    last_rows: default!(Option<i32>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(seq, i64),
        name!(op, i16),
        name!(pk, pgrx::JsonB),
        name!(deleted, bool),
        name!(row_image, Option<pgrx::JsonB>),
        name!(source, String),
    ),
> {
    match changes_since_pg_impl(table_name.oid(), since_seq, Some(limit_rows), last_rows) {
        Ok(rows) => TableIterator::new(rows),
        Err(error) => pgrx::error!("changes_since failed: {error}"),
    }
}

/// One `changes_since` output row: `(seq, op, pk, deleted, row_image, source)`.
#[cfg(feature = "pg")]
type ChangesSinceRow = (i64, i16, pgrx::JsonB, bool, Option<pgrx::JsonB>, String);

#[cfg(feature = "pg")]
fn changes_since_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    since_seq: i64,
    limit_rows: Option<i32>,
    last_rows: Option<i32>,
) -> Result<Vec<ChangesSinceRow>, String> {
    if since_seq < 0 {
        return Err("since_seq must be >= 0".to_string());
    }
    let limit = limit_rows.unwrap_or(DEFAULT_CHANGE_LIMIT);
    if limit <= 0 {
        return Err("limit_rows must be positive".to_string());
    }
    if let Some(last_rows) = last_rows {
        if last_rows <= 0 {
            return Err("last_rows must be positive".to_string());
        }
        if last_rows > limit {
            return Err("last_rows must be <= limit_rows".to_string());
        }
    }

    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "table is not managed by koldstore".to_string())?;
    if !snapshot.active {
        return Err("managed table is not active".to_string());
    }

    let mirror = QualifiedTableName::from_table_name(&snapshot.mirror_relation);
    let pk_names: Vec<String> = snapshot
        .primary_key_columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let scope = resolve_changes_since_scope(&snapshot, &pk_names)?;
    let scope_key = scope.as_ref().map(|(key, _)| key.clone());
    let scope_column = scope.as_ref().map(|(_, column)| column.as_str());

    // Positive since_seq wins over last_rows (KalamDB precedence).
    let use_last_rows = since_seq == 0 && last_rows.is_some();
    let selected = if use_last_rows {
        let last_rows = last_rows.expect("checked above");
        fetch_last_rows_page(
            table_oid,
            &snapshot,
            &mirror,
            &pk_names,
            last_rows,
            scope_column,
            scope_key.as_ref(),
        )?
    } else {
        fetch_since_seq_page(
            table_oid,
            &snapshot,
            &mirror,
            &pk_names,
            since_seq,
            limit as usize,
            scope_column,
            scope_key.as_ref(),
        )?
    };

    Ok(selected
        .into_iter()
        .map(|change| {
            let source = match change.source {
                ChangeSource::HotMirror => "hot".to_string(),
                ChangeSource::ColdRecord => "cold".to_string(),
            };
            (
                change.seq.get(),
                change.operation.code(),
                pgrx::JsonB(change.pk_json),
                change.deleted,
                change.row_image_json.map(pgrx::JsonB),
                source,
            )
        })
        .collect())
}

#[cfg(feature = "pg")]
fn resolve_changes_since_scope(
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    pk_names: &[String],
) -> Result<Option<(ScopeKey, String)>, String> {
    let table_kind = if snapshot.scope_column.is_some() {
        TableKind::User
    } else {
        TableKind::Shared
    };
    let active = active_scope_for_table(table_kind, crate::guc::user_id().as_deref())
        .map_err(|error| error.to_string())?;
    let Some(active) = active else {
        return Ok(None);
    };
    let Some(scope_column) = snapshot.scope_column.as_ref() else {
        return Ok(None);
    };
    // `__cl` stores PK columns only. Scope filtering can push into the mirror
    // query only when the application scope column is part of the primary key.
    if !pk_names.iter().any(|name| name == scope_column) {
        return Err(format!(
            "changes_since on user-scoped tables requires scope_column `{scope_column}` to be part of the primary key (mirror has PK columns only)"
        ));
    }
    Ok(Some((active, scope_column.clone())))
}

/// Catalog-routed page for `since_seq` + `limit_rows`.
#[cfg(feature = "pg")]
fn fetch_since_seq_page(
    table_oid: pgrx::pg_sys::Oid,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    mirror: &QualifiedTableName,
    pk_names: &[String],
    since_seq: i64,
    limit: usize,
    scope_column: Option<&str>,
    scope_key: Option<&ScopeKey>,
) -> Result<Vec<MirrorChange>, String> {
    let manifest = crate::catalog::cache::cached_manifest_scan_context(table_oid, &[])?;
    let segments = manifest
        .as_ref()
        .map(|ctx| ctx.segments.as_slice())
        .unwrap_or(&[]);
    let oldest_cold = segments.iter().map(|segment| segment.min_seq.get()).min();
    let newest_cold = segments.iter().map(|segment| segment.max_seq.get()).max();

    if let Some(oldest) = oldest_cold {
        if since_seq > 0 && since_seq < oldest - 1 {
            return Err(format!(
                "change records before sequence {oldest} are no longer retained"
            ));
        }
    }

    let past_cold = segments.is_empty() || newest_cold.is_some_and(|max| since_seq >= max);
    if past_cold {
        return fetch_hot_mirror_changes(
            table_oid.to_u32(),
            mirror,
            pk_names,
            since_seq,
            limit,
            scope_column,
            scope_key,
        );
    }

    let mut page = Vec::with_capacity(limit);
    let mut cursor = since_seq;
    while page.len() < limit {
        let Some(segment) = next_cold_segment(segments, cursor) else {
            let rest = limit - page.len();
            let hot = fetch_hot_mirror_changes(
                table_oid.to_u32(),
                mirror,
                pk_names,
                cursor,
                rest,
                scope_column,
                scope_key,
            )?;
            page.extend(hot);
            break;
        };

        let need = limit - page.len();
        let cold = read_cold_segment_page(table_oid, snapshot, segment, cursor, need, scope_key)?;
        if cold.is_empty() {
            // Stats prune / scope filter emptied this segment for the cursor —
            // advance past it and try the next catalog candidate.
            cursor = segment.max_seq.get();
            continue;
        }
        cursor = cold.last().map(|row| row.seq.get()).unwrap_or(cursor);
        page.extend(cold);
        if page.len() >= limit {
            break;
        }
        // Page still short: either the segment EOF'd under the row limit, or
        // scope filtering dropped rows. Re-enter with the advanced cursor so
        // the same segment can continue, or the next catalog candidate / mirror.
    }

    Ok(page)
}

/// Newest-N rewind: mirror first, then one newest cold segment if shortfall.
#[cfg(feature = "pg")]
fn fetch_last_rows_page(
    table_oid: pgrx::pg_sys::Oid,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    mirror: &QualifiedTableName,
    pk_names: &[String],
    last_rows: i32,
    scope_column: Option<&str>,
    scope_key: Option<&ScopeKey>,
) -> Result<Vec<MirrorChange>, String> {
    let hot = fetch_hot_mirror_last_rows(
        table_oid.to_u32(),
        mirror,
        pk_names,
        last_rows,
        scope_column,
        scope_key,
    )?;
    if hot.len() as i32 >= last_rows {
        return Ok(
            events::changes_last(&hot, table_oid.to_u32(), scope_key, last_rows)
                .map_err(|error| error.to_string())?,
        );
    }

    let manifest = crate::catalog::cache::cached_manifest_scan_context(table_oid, &[])?;
    let segments = manifest
        .as_ref()
        .map(|ctx| ctx.segments.as_slice())
        .unwrap_or(&[]);
    let Some(newest) = segments.iter().max_by_key(|segment| {
        (
            segment.max_seq.get(),
            segment.min_seq.get(),
            &segment.object_path,
        )
    }) else {
        return Ok(
            events::changes_last(&hot, table_oid.to_u32(), scope_key, last_rows)
                .map_err(|error| error.to_string())?,
        );
    };

    let need = (last_rows as usize).saturating_sub(hot.len());
    // Stream the newest segment with an ascending seq read, keep a bounded
    // newest-N window (O(last_rows) memory, not O(segment)).
    let cold = read_cold_segment_newest_window(table_oid, snapshot, newest, need, scope_key)?;
    let mut combined = cold;
    combined.extend(hot);
    events::changes_last(&combined, table_oid.to_u32(), scope_key, last_rows)
        .map_err(|error| error.to_string())
}

/// Oldest published segment that can still contribute rows after `since_seq`.
#[cfg(feature = "pg")]
fn next_cold_segment<'a>(
    segments: &'a [SegmentStatsHint],
    since_seq: i64,
) -> Option<&'a SegmentStatsHint> {
    segments
        .iter()
        .filter(|segment| segment.max_seq.get() > since_seq)
        .min_by_key(|segment| {
            (
                segment.min_seq.get(),
                segment.max_seq.get(),
                segment.object_path.as_str(),
            )
        })
}

#[cfg(feature = "pg")]
fn fetch_hot_mirror_changes(
    table_oid: u32,
    mirror: &QualifiedTableName,
    pk_names: &[String],
    since_seq: i64,
    limit: usize,
    scope_column: Option<&str>,
    scope_key: Option<&ScopeKey>,
) -> Result<Vec<MirrorChange>, String> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let limit_i32 = i32::try_from(limit).unwrap_or(i32::MAX);
    let plan = events::plan_mirror_changes_since(mirror, pk_names, scope_column)
        .map_err(|error| error.to_string())?;
    let mut statement = plan.statement;
    if plan.scope_parameter_index.is_none() && statement.sql.contains("$3::integer") {
        statement.sql = statement.sql.replace("$3::integer", "$2::integer");
        statement.param_types = vec![SqlParamType::BigInt, SqlParamType::Integer];
    }

    let rows = crate::catalog::owner::with_extension_owner(|| {
        let params: Vec<DatumWithOid<'_>> = match (plan.scope_parameter_index, scope_key) {
            (Some(_), Some(scope_key)) => vec![
                DatumWithOid::from(since_seq),
                DatumWithOid::from(scope_key.as_str()),
                DatumWithOid::from(limit_i32),
            ],
            _ => vec![DatumWithOid::from(since_seq), DatumWithOid::from(limit_i32)],
        };
        crate::spi::execute_prepared(&statement, &params, |tuples| {
            decode_hot_mirror_tuples(table_oid, tuples, scope_key)
        })
        .map_err(|error| error.to_string())
    })??;
    Ok(rows)
}

#[cfg(feature = "pg")]
fn fetch_hot_mirror_last_rows(
    table_oid: u32,
    mirror: &QualifiedTableName,
    pk_names: &[String],
    last_rows: i32,
    scope_column: Option<&str>,
    scope_key: Option<&ScopeKey>,
) -> Result<Vec<MirrorChange>, String> {
    let plan = events::plan_mirror_changes_last(mirror, pk_names, scope_column)
        .map_err(|error| error.to_string())?;
    let rows = crate::catalog::owner::with_extension_owner(|| {
        let params: Vec<DatumWithOid<'_>> = match (plan.scope_parameter_index, scope_key) {
            (Some(_), Some(scope_key)) => {
                vec![
                    DatumWithOid::from(scope_key.as_str()),
                    DatumWithOid::from(last_rows),
                ]
            }
            _ => vec![DatumWithOid::from(last_rows)],
        };
        crate::spi::execute_prepared(&plan.statement, &params, |tuples| {
            decode_hot_mirror_tuples(table_oid, tuples, scope_key)
        })
        .map_err(|error| error.to_string())
    })??;
    Ok(rows)
}

#[cfg(feature = "pg")]
fn decode_hot_mirror_tuples(
    table_oid: u32,
    tuples: pgrx::spi::SpiTupleTable<'_>,
    scope_key: Option<&ScopeKey>,
) -> pgrx::spi::Result<Vec<MirrorChange>> {
    let mut out = Vec::new();
    for tuple in tuples {
        let seq: i64 = tuple
            .get(1)?
            .ok_or_else(|| crate::spi::missing_attribute("seq"))?;
        let op: i16 = tuple
            .get(2)?
            .ok_or_else(|| crate::spi::missing_attribute("op"))?;
        let pk: pgrx::JsonB = tuple
            .get(3)?
            .ok_or_else(|| crate::spi::missing_attribute("pk"))?;
        let deleted: bool = tuple
            .get(4)?
            .ok_or_else(|| crate::spi::missing_attribute("deleted"))?;
        let row_image: Option<pgrx::JsonB> = tuple.get(5)?;
        let operation = MirrorOperation::from_code(op).map_err(|error| {
            pgrx::spi::SpiError::DatumError(pgrx::datum::TryFromDatumError::NoSuchAttributeName(
                error.to_string(),
            ))
        })?;
        let seq = SeqId::new(seq).map_err(|error| {
            pgrx::spi::SpiError::DatumError(pgrx::datum::TryFromDatumError::NoSuchAttributeName(
                error.to_string(),
            ))
        })?;
        out.push(MirrorChange {
            table_oid: TableOid::from_raw(table_oid),
            scope_key: scope_key.cloned(),
            pk_json: pk.0,
            operation,
            seq,
            deleted,
            row_image_json: row_image.map(|value| value.0),
            source: ChangeSource::HotMirror,
        });
    }
    Ok(out)
}

#[cfg(feature = "pg")]
fn read_cold_segment_page(
    table_oid: pgrx::pg_sys::Oid,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    segment: &SegmentStatsHint,
    since_seq: i64,
    limit: usize,
    scope_key: Option<&ScopeKey>,
) -> Result<Vec<MirrorChange>, String> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    if crate::guc::cold_reads_mode() == crate::settings::ColdReadsMode::Off {
        return Err("cold reads are disabled by koldstore.cold_reads".to_string());
    }

    let catalog = crate::catalog::cache::cached_migration_catalog(table_oid)?;
    let (pg_columns, physical_names, physical_pk_names) =
        cold_projection_for_segment(snapshot, &catalog, segment)?;

    let Some(manifest) = crate::catalog::cache::cached_manifest_scan_context(table_oid, &[])?
    else {
        return Ok(Vec::new());
    };
    let client = open_managed_object_store_client(
        &manifest.storage_type,
        &manifest.base_path,
        &manifest.credentials,
        &manifest.config,
    )
    .map_err(|error| error.to_string())?;
    let store = client.store();

    let min_seq = SeqId::new(since_seq.saturating_add(1).max(1)).map_err(|e| e.to_string())?;
    let options = ParquetReadOptions::new()
        .with_columns(physical_names.clone())
        .with_clean_seq_range(min_seq, segment.max_seq)
        .with_row_limit(limit)
        .with_timeout(client.timeout());

    let _permit = crate::merge_scan::reader_pool::try_acquire_parquet_reader_permit(
        crate::guc::max_open_parquet_readers(),
    )?;
    let (rows, _) = read_clean_cold_rows_from_object_store_with_size(
        std::sync::Arc::clone(&store),
        &segment.object_path,
        segment.byte_size,
        &pg_columns,
        &physical_pk_names,
        &options,
    )?;

    let mut changes = Vec::with_capacity(rows.len());
    for row in rows {
        let change = cold_row_to_mirror_change(
            table_oid.to_u32(),
            &snapshot.primary_key_columns,
            &physical_pk_names,
            &physical_names,
            &catalog.columns,
            row,
            snapshot.scope_column.as_deref(),
        )?;
        if let Some(required) = scope_key {
            match change.scope_key.as_ref() {
                Some(actual) if actual == required => {}
                _ => continue,
            }
        }
        changes.push(change);
    }
    // Scope filter can drop rows after the reader already early-stopped; that
    // is acceptable — the next page advances by the last emitted seq.
    Ok(changes)
}

/// Streams one segment and retains only the newest `limit` rows (bounded).
#[cfg(feature = "pg")]
fn read_cold_segment_newest_window(
    table_oid: pgrx::pg_sys::Oid,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    segment: &SegmentStatsHint,
    limit: usize,
    scope_key: Option<&ScopeKey>,
) -> Result<Vec<MirrorChange>, String> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    if crate::guc::cold_reads_mode() == crate::settings::ColdReadsMode::Off {
        return Err("cold reads are disabled by koldstore.cold_reads".to_string());
    }

    let catalog = crate::catalog::cache::cached_migration_catalog(table_oid)?;
    let (pg_columns, physical_names, physical_pk_names) =
        cold_projection_for_segment(snapshot, &catalog, segment)?;

    let Some(manifest) = crate::catalog::cache::cached_manifest_scan_context(table_oid, &[])?
    else {
        return Ok(Vec::new());
    };
    let client = open_managed_object_store_client(
        &manifest.storage_type,
        &manifest.base_path,
        &manifest.credentials,
        &manifest.config,
    )
    .map_err(|error| error.to_string())?;
    let store = client.store();

    let options = ParquetReadOptions::new()
        .with_columns(physical_names.clone())
        .with_timeout(client.timeout());
    let _permit = crate::merge_scan::reader_pool::try_acquire_parquet_reader_permit(
        crate::guc::max_open_parquet_readers(),
    )?;
    let (rows, _) = read_clean_cold_rows_from_object_store_with_size(
        std::sync::Arc::clone(&store),
        &segment.object_path,
        segment.byte_size,
        &pg_columns,
        &physical_pk_names,
        &options,
    )?;

    let mut window: Vec<MirrorChange> = Vec::new();
    for row in rows {
        let change = cold_row_to_mirror_change(
            table_oid.to_u32(),
            &snapshot.primary_key_columns,
            &physical_pk_names,
            &physical_names,
            &catalog.columns,
            row,
            snapshot.scope_column.as_deref(),
        )?;
        if let Some(required) = scope_key {
            match change.scope_key.as_ref() {
                Some(actual) if actual == required => {}
                _ => continue,
            }
        }
        window.push(change);
        if window.len() > limit {
            // Drop the oldest so the window stays the newest `limit` in encounter
            // order. Segments are written ascending by seq, so this equals newest-N.
            window.remove(0);
        }
    }
    Ok(window)
}

#[cfg(feature = "pg")]
fn cold_projection_for_segment(
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    segment: &SegmentStatsHint,
) -> Result<(Vec<PgColumn>, Vec<String>, Vec<String>), String> {
    let physical_pk_names: Vec<String> = snapshot
        .primary_key_columns
        .iter()
        .map(|column| {
            physical_name_for_pk(column, segment, snapshot.schema_version).ok_or_else(|| {
                format!(
                    "cold segment `{}` is missing primary-key column `{}`",
                    segment.object_path, column.name
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut pg_columns = Vec::with_capacity(catalog.columns.len());
    let mut physical_names = Vec::with_capacity(catalog.columns.len());
    for column in &catalog.columns {
        let Some(physical) = koldstore_merge::scan::physical_name_for_segment_column(
            column.column_id.get(),
            &column.name,
            segment,
            snapshot.schema_version,
        ) else {
            continue;
        };
        physical_names.push(physical.to_string());
        pg_columns.push(PgColumn::new(physical.to_string(), column.pg_type, true));
    }
    if pg_columns.is_empty() {
        // Fall back to PK-only projection when catalog columns cannot map.
        for (logical, physical) in snapshot.primary_key_columns.iter().zip(&physical_pk_names) {
            let catalog_column = catalog
                .columns
                .iter()
                .find(|candidate| candidate.column_id == logical.column_id)
                .ok_or_else(|| {
                    format!(
                        "migration catalog is missing primary-key column_id {}",
                        logical.column_id
                    )
                })?;
            pg_columns.push(PgColumn::new(
                physical.clone(),
                catalog_column.pg_type,
                true,
            ));
            physical_names.push(physical.clone());
        }
    }
    Ok((pg_columns, physical_names, physical_pk_names))
}

#[cfg(feature = "pg")]
fn cold_row_to_mirror_change(
    table_oid: u32,
    logical_pk: &[koldstore_common::ColumnRef],
    physical_pk_names: &[String],
    physical_app_names: &[String],
    catalog_columns: &[koldstore_migrate::order::CatalogColumn],
    mut row: CleanColdRow,
    scope_column: Option<&str>,
) -> Result<MirrorChange, String> {
    // Remap physical PK field names back to logical snapshot names.
    if let serde_json::Value::Object(ref mut map) = row.pk_json {
        let mut remapped = serde_json::Map::new();
        for (logical, physical) in logical_pk.iter().zip(physical_pk_names.iter()) {
            let value = map
                .remove(physical)
                .or_else(|| map.remove(&logical.name))
                .ok_or_else(|| {
                    format!(
                        "cold change row missing primary-key field `{}`",
                        logical.name
                    )
                })?;
            remapped.insert(logical.name.clone(), value);
        }
        row.pk_json = serde_json::Value::Object(remapped);
    }

    // Remap application row_image keys to logical catalog names when possible.
    let mut logical_image = serde_json::Map::new();
    let image_json = row.row_image.to_json();
    if let serde_json::Value::Object(map) = image_json {
        for (physical, value) in map {
            let logical = catalog_columns
                .iter()
                .zip(physical_app_names.iter())
                .find(|(_, phys)| phys.as_str() == physical.as_str())
                .map(|(column, _)| column.name.clone())
                .or_else(|| {
                    logical_pk
                        .iter()
                        .zip(physical_pk_names.iter())
                        .find(|(_, phys)| phys.as_str() == physical.as_str())
                        .map(|(column, _)| column.name.clone())
                })
                .unwrap_or(physical);
            logical_image.insert(logical, value);
        }
    }

    let scope_key = scope_key_from_pk_json(&row.pk_json, scope_column);
    let operation = MirrorOperation::from_code(row.op).map_err(|error| error.to_string())?;
    let seq = SeqId::new(row.seq).map_err(|error| error.to_string())?;
    Ok(MirrorChange {
        table_oid: TableOid::from_raw(table_oid),
        scope_key,
        pk_json: row.pk_json,
        operation,
        seq,
        deleted: row.deleted,
        row_image_json: (!row.deleted).then(|| serde_json::Value::Object(logical_image)),
        source: ChangeSource::ColdRecord,
    })
}

#[cfg(feature = "pg")]
fn scope_key_from_pk_json(
    pk_json: &serde_json::Value,
    scope_column: Option<&str>,
) -> Option<ScopeKey> {
    let scope_column = scope_column?;
    let value = pk_json.get(scope_column)?;
    let text = match value {
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string().trim_matches('"').to_string(),
    };
    ScopeKey::new(text).ok()
}

#[cfg(feature = "pg")]
fn physical_name_for_pk(
    column: &koldstore_common::ColumnRef,
    segment: &SegmentStatsHint,
    current_schema_version: i32,
) -> Option<String> {
    koldstore_merge::scan::physical_name_for_segment_column(
        column.column_id.get(),
        &column.name,
        segment,
        current_schema_version,
    )
    .map(str::to_string)
}
