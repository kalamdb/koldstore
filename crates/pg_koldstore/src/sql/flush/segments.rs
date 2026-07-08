//! Parquet segment writes and catalog bookkeeping SPI adapters for flush.

use koldstore_catalog::decode::FlushStorageContext;
use koldstore_common::QualifiedTableName;
use koldstore_flush::{
    cleanup::plan_clean_schema_cleanup, manifest_from_catalog_rows, plan_flush_cold_segment_insert,
    plan_flush_pk_hint_insert, plan_manifest_row_upsert, CatalogManifestSegmentRow,
};

use super::stats::FlushStats;
use super::write::FlushWriteChunk;

pub(super) fn next_flush_batch_number(table_oid: pgrx::pg_sys::Oid) -> Result<i32, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_catalog::queries::plan_next_flush_batch_number()
        .map_err(|error| error.to_string())?;
    crate::spi::select_one::<i32>(&statement, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "batch number lookup returned no rows".to_string())
}

pub(super) fn write_parquet_segment(
    path: &std::path::Path,
    columns: &[koldstore_parquet::PgColumn],
    rows: &[koldstore_parquet::CleanColdRecordPlan],
    primary_key_columns: &[String],
    compression: &str,
) -> Result<i64, String> {
    let batch = koldstore_parquet::record_batch_from_clean_cold_records(columns, rows)?;
    let file = std::fs::File::create(path).map_err(|error| error.to_string())?;
    let writer = koldstore_parquet::ParquetSegmentWriter::new(
        koldstore_parquet::WriterOptions {
            compression: compression.to_string(),
            ..koldstore_parquet::WriterOptions::default()
        }
        .with_statistics_columns([koldstore_parquet::ColdMetadataColumn::Seq.name()])
        .with_bloom_filter_columns(primary_key_columns.iter().map(String::as_str)),
    );
    writer
        .write_record_batches(file, batch.schema(), [batch])
        .map_err(|error| error.to_string())?;
    let len = std::fs::metadata(path)
        .map_err(|error| error.to_string())?
        .len();
    i64::try_from(len).map_err(|error| error.to_string())
}

pub(super) fn manifest_from_active_cold_segments(
    table_oid: pgrx::pg_sys::Oid,
    relation: &koldstore_catalog::decode::RelationContext,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    schema_version: i32,
) -> Result<koldstore_manifest::Manifest, String> {
    use pgrx::datum::DatumWithOid;

    let statement = koldstore_catalog::queries::plan_active_cold_segments_for_manifest_json()
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

#[allow(clippy::too_many_arguments)]
pub(super) fn write_flush_segment(
    table_oid: pgrx::pg_sys::Oid,
    relation: &koldstore_catalog::decode::RelationContext,
    storage: &FlushStorageContext,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    columns: &[koldstore_parquet::PgColumn],
    chunk: &FlushWriteChunk,
    batch_number: i32,
    chunk_stats: &FlushStats,
) -> Result<(), String> {
    let prefix = format!("{}/{}", relation.namespace, relation.name);
    let batch_file_name = format!("batch-{batch_number}.parquet");
    let object_path = format!("{prefix}/{batch_file_name}");
    let absolute_segment_path = std::path::Path::new(&storage.base_path).join(&object_path);
    if let Some(parent) = absolute_segment_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }

    let byte_size = write_parquet_segment(
        &absolute_segment_path,
        columns,
        &chunk.rows,
        &snapshot.primary_key_columns,
        &storage.compression,
    )?;
    let segment_id = uuid::Uuid::new_v4();
    insert_cold_segment(
        table_oid,
        segment_id,
        &object_path,
        batch_number,
        chunk_stats,
        byte_size,
        storage.schema_version,
    )?;
    insert_cold_pk_hint(
        table_oid,
        segment_id,
        &object_path,
        chunk_stats.max_seq,
        chunk_stats.max_commit_seq,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_cold_segment(
    table_oid: pgrx::pg_sys::Oid,
    segment_id: uuid::Uuid,
    object_path: &str,
    batch_number: i32,
    stats: &FlushStats,
    byte_size: i64,
    schema_version: i32,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    let statement = plan_flush_cold_segment_insert().map_err(|error| error.to_string())?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(pgrx::Uuid::from_bytes(*segment_id.as_bytes())),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(object_path),
            DatumWithOid::from(batch_number),
            DatumWithOid::from(stats.min_seq),
            DatumWithOid::from(stats.max_seq),
            DatumWithOid::from(stats.min_commit_seq),
            DatumWithOid::from(stats.max_commit_seq),
            DatumWithOid::from(stats.row_count),
            DatumWithOid::from(byte_size),
            DatumWithOid::from(schema_version),
            DatumWithOid::from(pgrx::JsonB(serde_json::json!({
                "seq": {"min": stats.min_seq, "max": stats.max_seq}
            }))),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
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

fn insert_cold_pk_hint(
    table_oid: pgrx::pg_sys::Oid,
    segment_id: uuid::Uuid,
    seed: &str,
    latest_seq: i64,
    latest_commit_seq: i64,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    let statement = plan_flush_pk_hint_insert().map_err(|error| error.to_string())?;
    crate::spi::update(
        &statement,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(seed),
            DatumWithOid::from(pgrx::Uuid::from_bytes(*segment_id.as_bytes())),
            DatumWithOid::from(latest_seq),
            DatumWithOid::from(latest_commit_seq),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

pub(super) fn prune_flushed_hot_rows(
    table_oid: pgrx::pg_sys::Oid,
    primary_key_columns: &[String],
    cleanup_rows: &[serde_json::Value],
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    if cleanup_rows.is_empty() {
        return Ok(());
    }

    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table = QualifiedTableName::parse(&relation).map_err(|error| error.to_string())?;
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = QualifiedTableName::from_table_name(&snapshot.mirror_relation);
    let plan = plan_clean_schema_cleanup(&table, &mirror, primary_key_columns)
        .map_err(|error| error.to_string())?;
    let cleanup_arg = &[DatumWithOid::from(pgrx::JsonB(serde_json::Value::Array(
        cleanup_rows.to_vec(),
    )))];
    crate::merge_scan::pg::with_custom_scan_disabled(|| {
        pgrx::Spi::connect_mut(|client| {
            client
                .update("SET LOCAL session_replication_role = replica", None, &[])
                .map_err(|error| error.to_string())?;
            client
                .update(&plan.statement.sql, None, cleanup_arg)
                .map_err(|error| error.to_string())?;
            Ok::<(), String>(())
        })
        .map_err(|error| error.to_string())
    })?;
    Ok(())
}
