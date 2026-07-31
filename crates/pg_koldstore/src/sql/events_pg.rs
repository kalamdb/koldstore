//! Public change-feed SQL entrypoint (`koldstore.changes_since`).
//!
//! Merges latest-state rows from the hot `__cl` mirror and flushed cold Parquet
//! metadata, ordered by exclusive mirror `seq` cursor.

#[cfg(feature = "pg")]
use koldstore_common::{
    ChangeSource, MirrorChange, MirrorOperation, QualifiedTableName, SeqId, SqlParamType,
};
#[cfg(feature = "pg")]
use koldstore_merge::events::{self, DEFAULT_CHANGE_LIMIT};
#[cfg(feature = "pg")]
use koldstore_parquet::{
    read_clean_cold_rows_from_object_store_with_size, CleanColdRow, ParquetReadOptions, PgColumn,
};
#[cfg(feature = "pg")]
use koldstore_storage::open_client_from_catalog_fields;
#[cfg(feature = "pg")]
use pgrx::datum::DatumWithOid;
#[cfg(feature = "pg")]
use pgrx::iter::TableIterator;
#[cfg(feature = "pg")]
use pgrx::prelude::*;

/// Latest-state change event returned by `koldstore.changes_since`.
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
/// `last_rows` means from the start of retained history.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(
    name = "changes_since",
    schema = "koldstore",
    security_definer,
    parallel_safe,
    stable
)]
fn changes_since_pg(
    table_name: pgrx::pg_sys::Oid,
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
    match changes_since_pg_impl(table_name, since_seq, Some(limit_rows), last_rows) {
        Ok(rows) => TableIterator::new(rows),
        Err(error) => pgrx::error!("changes_since failed: {error}"),
    }
}

#[cfg(feature = "pg")]
fn changes_since_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    since_seq: i64,
    limit_rows: Option<i32>,
    last_rows: Option<i32>,
) -> Result<Vec<(i64, i16, pgrx::JsonB, bool, Option<pgrx::JsonB>, String)>, String> {
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
        // KalamDB: last_rows must fit in one batch.
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

    let table_oid_u32 = table_oid.to_u32();

    // KalamDB precedence: positive since_seq (from) wins over last_rows.
    let use_last_rows = since_seq == 0 && last_rows.is_some();
    let selected = if use_last_rows {
        let last_rows = last_rows.expect("checked above");
        let hot = fetch_hot_mirror_last_rows(table_oid_u32, &mirror, &pk_names, last_rows)?;
        let cold_floor = if hot.len() as i32 >= last_rows {
            hot.iter().map(|row| row.seq.get()).min().unwrap_or(1) - 1
        } else {
            0
        };
        let (cold, _) = fetch_cold_changes(table_oid, &snapshot, cold_floor)?;
        let mut combined = hot;
        combined.extend(cold);
        events::changes_last(&combined, table_oid_u32, None, last_rows)
            .map_err(|error| error.to_string())?
    } else {
        let hot = fetch_hot_mirror_changes(table_oid_u32, &mirror, &pk_names, since_seq)?;
        let (cold, oldest_cold) = fetch_cold_changes(table_oid, &snapshot, since_seq)?;
        let mut combined = hot;
        combined.extend(cold);
        let oldest_available = oldest_cold.and_then(|seq| SeqId::new(seq).ok());
        events::changes_since(
            &combined,
            table_oid_u32,
            None,
            since_seq,
            Some(limit),
            oldest_available,
        )
        .map_err(|error| error.to_string())?
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
fn fetch_hot_mirror_changes(
    table_oid: u32,
    mirror: &QualifiedTableName,
    pk_names: &[String],
    since_seq: i64,
) -> Result<Vec<MirrorChange>, String> {
    let plan = events::plan_mirror_changes_since(mirror, pk_names, None)
        .map_err(|error| error.to_string())?;
    // Collect every hot candidate with seq > since_seq; the merge step applies
    // the caller limit after hot+cold latest-state resolution.
    let mut statement = plan.statement;
    if plan.scope_parameter_index.is_none() && statement.sql.contains("$3::integer") {
        statement.sql = statement.sql.replace("$3::integer", "$2::integer");
        statement.param_types = vec![SqlParamType::BigInt, SqlParamType::Integer];
    }

    let rows = crate::catalog::owner::with_extension_owner(|| {
        crate::spi::execute_prepared(
            &statement,
            &[DatumWithOid::from(since_seq), DatumWithOid::from(i32::MAX)],
            |tuples| decode_hot_mirror_tuples(table_oid, tuples),
        )
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
) -> Result<Vec<MirrorChange>, String> {
    let plan =
        events::plan_mirror_changes_last(mirror, pk_names).map_err(|error| error.to_string())?;
    let rows = crate::catalog::owner::with_extension_owner(|| {
        crate::spi::execute_prepared(
            &plan.statement,
            &[DatumWithOid::from(last_rows)],
            |tuples| decode_hot_mirror_tuples(table_oid, tuples),
        )
        .map_err(|error| error.to_string())
    })??;
    Ok(rows)
}

#[cfg(feature = "pg")]
fn decode_hot_mirror_tuples(
    table_oid: u32,
    tuples: pgrx::spi::SpiTupleTable<'_>,
) -> pgrx::spi::Result<Vec<MirrorChange>> {
    let mut out = Vec::new();
    for tuple in tuples {
        let seq: i64 = tuple.get(1)?.ok_or_else(|| spi_missing("seq"))?;
        let op: i16 = tuple.get(2)?.ok_or_else(|| spi_missing("op"))?;
        let pk: pgrx::JsonB = tuple.get(3)?.ok_or_else(|| spi_missing("pk"))?;
        let deleted: bool = tuple.get(4)?.ok_or_else(|| spi_missing("deleted"))?;
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
            table_oid,
            scope_key: None,
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
fn fetch_cold_changes(
    table_oid: pgrx::pg_sys::Oid,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    since_seq: i64,
) -> Result<(Vec<MirrorChange>, Option<i64>), String> {
    let Some(manifest) = crate::catalog::cache::cached_manifest_scan_context(table_oid, &[])?
    else {
        return Ok((Vec::new(), None));
    };

    let oldest = manifest
        .segments
        .iter()
        .map(|segment| segment.min_seq.get())
        .min();

    let candidates: Vec<_> = manifest
        .segments
        .iter()
        .filter(|segment| segment.max_seq.get() > since_seq)
        .collect();
    if candidates.is_empty() {
        return Ok((Vec::new(), oldest));
    }

    if crate::guc::cold_reads_mode() == crate::settings::ColdReadsMode::Off {
        return Err("cold reads are disabled by koldstore.cold_reads".to_string());
    }

    let catalog = crate::catalog::cache::cached_migration_catalog(table_oid)?;
    let client = open_client_from_catalog_fields(
        &manifest.storage_type,
        &manifest.base_path,
        &manifest.credentials,
        &manifest.config,
    )
    .map_err(|error| error.to_string())?;
    let store = client.store();

    let mut changes = Vec::new();
    for segment in candidates {
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

        let pg_columns: Vec<PgColumn> = snapshot
            .primary_key_columns
            .iter()
            .zip(physical_pk_names.iter())
            .map(|(column, physical)| {
                let catalog_column = catalog
                    .columns
                    .iter()
                    .find(|candidate| candidate.column_id == column.column_id)
                    .ok_or_else(|| {
                        format!(
                            "migration catalog is missing primary-key column_id {}",
                            column.column_id
                        )
                    })?;
                Ok(PgColumn::new(
                    physical.clone(),
                    catalog_column.pg_type,
                    true,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;

        let mut options = ParquetReadOptions::new().with_columns(physical_pk_names.clone());
        if let Ok(min_seq) = SeqId::new(since_seq.saturating_add(1).max(1)) {
            options = options.with_clean_seq_range(min_seq, segment.max_seq);
        }

        let (rows, _) = read_clean_cold_rows_from_object_store_with_size(
            std::sync::Arc::clone(&store),
            &segment.object_path,
            segment.byte_size,
            &pg_columns,
            &physical_pk_names,
            &options,
        )?;

        for row in rows {
            if row.seq <= since_seq {
                continue;
            }
            changes.push(cold_row_to_mirror_change(
                table_oid.to_u32(),
                &snapshot.primary_key_columns,
                &physical_pk_names,
                row,
            )?);
        }
    }

    Ok((changes, oldest))
}

#[cfg(feature = "pg")]
fn cold_row_to_mirror_change(
    table_oid: u32,
    logical_pk: &[koldstore_common::ColumnRef],
    physical_pk_names: &[String],
    mut row: CleanColdRow,
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

    let operation = MirrorOperation::from_code(row.op).map_err(|error| error.to_string())?;
    let seq = SeqId::new(row.seq).map_err(|error| error.to_string())?;
    Ok(MirrorChange {
        table_oid,
        scope_key: None,
        pk_json: row.pk_json,
        operation,
        seq,
        deleted: row.deleted,
        row_image_json: (!row.deleted).then_some(row.row_image),
        source: ChangeSource::ColdRecord,
    })
}

#[cfg(feature = "pg")]
fn physical_name_for_pk(
    column: &koldstore_common::ColumnRef,
    segment: &koldstore_merge::scan::SegmentStatsHint,
    current_schema_version: i32,
) -> Option<String> {
    if segment.schema_version == current_schema_version {
        return Some(
            segment
                .physical_names
                .get(&column.column_id.get())
                .cloned()
                .unwrap_or_else(|| column.name.clone()),
        );
    }
    segment
        .physical_names
        .get(&column.column_id.get())
        .cloned()
        .or_else(|| Some(column.name.clone()))
}

#[cfg(feature = "pg")]
fn spi_missing(field: &str) -> pgrx::spi::SpiError {
    pgrx::spi::SpiError::DatumError(pgrx::datum::TryFromDatumError::NoSuchAttributeName(
        field.to_string(),
    ))
}
