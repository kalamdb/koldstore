//! Parquet segment writes and catalog bookkeeping for flush.

use koldstore_catalog::decode::FlushStorageContext;
use koldstore_common::QualifiedTableName;
use koldstore_flush::cleanup::plan_clean_schema_cleanup;
use serde::Deserialize;

use super::stats::FlushStats;
use super::write::FlushWriteChunk;

#[derive(Debug, Deserialize)]
struct CatalogManifestSegment {
    object_path: String,
    batch_number: i32,
    min_seq: i64,
    max_seq: i64,
    min_commit_seq: i64,
    max_commit_seq: i64,
    row_count: i64,
    byte_size: i64,
    schema_version: i32,
    column_stats: serde_json::Value,
}

pub(super) fn next_flush_batch_number(table_oid: pgrx::pg_sys::Oid) -> Result<i32, String> {
    pgrx::Spi::get_one_with_args::<i32>(
        "SELECT COALESCE(max(batch_number), 0) + 1 FROM koldstore.cold_segments WHERE table_oid = $1::oid AND scope_key = ''",
        &[pgrx::datum::DatumWithOid::from(table_oid)],
    )
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
    let rows = active_cold_segments_for_manifest(table_oid)?;
    let mut manifest = koldstore_manifest::Manifest::new_shared(
        relation.namespace.clone(),
        relation.name.clone(),
        u32::try_from(schema_version).map_err(|error| error.to_string())?,
    );
    for row in rows {
        manifest.append_segment(manifest_segment_from_catalog_row(relation, snapshot, row)?);
    }
    Ok(manifest)
}

fn active_cold_segments_for_manifest(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<Vec<CatalogManifestSegment>, String> {
    use pgrx::datum::DatumWithOid;

    let json = pgrx::Spi::get_one_with_args::<String>(
        r#"
SELECT COALESCE(jsonb_agg(
    jsonb_build_object(
        'object_path', object_path,
        'batch_number', batch_number,
        'min_seq', min_seq,
        'max_seq', max_seq,
        'min_commit_seq', min_commit_seq,
        'max_commit_seq', max_commit_seq,
        'row_count', row_count,
        'byte_size', byte_size,
        'schema_version', schema_version,
        'column_stats', column_stats
    )
    ORDER BY batch_number, segment_id
)::text, '[]')
FROM koldstore.cold_segments
WHERE table_oid = $1::oid
  AND scope_key = ''
  AND status = 'active'
"#,
        &[DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or_else(|| "[]".to_string());

    serde_json::from_str(&json).map_err(|error| error.to_string())
}

fn manifest_segment_from_catalog_row(
    relation: &koldstore_catalog::decode::RelationContext,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    row: CatalogManifestSegment,
) -> Result<koldstore_manifest::ManifestSegment, String> {
    let manifest_path = manifest_relative_segment_path(relation, &row.object_path);
    let mut segment = koldstore_manifest::ManifestSegment::committed(
        u32::try_from(row.batch_number).map_err(|error| error.to_string())?,
        manifest_path,
        row.min_seq..=row.max_seq,
        row.min_commit_seq..=row.max_commit_seq,
        u64::try_from(row.row_count).map_err(|error| error.to_string())?,
        u64::try_from(row.byte_size).map_err(|error| error.to_string())?,
        u32::try_from(row.schema_version).map_err(|error| error.to_string())?,
    );
    segment.column_stats = manifest_column_stats(row.column_stats);
    segment
        .bloom_filters
        .push(koldstore_manifest::ManifestBloomFilter::bloom(
            snapshot.primary_key_columns.clone(),
            Some(0.01),
        ));
    segment.pk_filter = Some(koldstore_manifest::PkFilter::exact(vec![1]));
    Ok(segment)
}

fn manifest_relative_segment_path(
    relation: &koldstore_catalog::decode::RelationContext,
    object_path: &str,
) -> String {
    let prefix = format!("{}/{}/", relation.namespace, relation.name);
    object_path
        .strip_prefix(&prefix)
        .unwrap_or(object_path)
        .to_string()
}

fn manifest_column_stats(
    column_stats: serde_json::Value,
) -> std::collections::BTreeMap<String, koldstore_manifest::ManifestColumnStats> {
    let mut stats = std::collections::BTreeMap::new();
    let Some(columns) = column_stats.as_object() else {
        return stats;
    };

    for (column, value) in columns {
        let Some(min) = value.get("min") else {
            continue;
        };
        let Some(max) = value.get("max") else {
            continue;
        };
        stats.insert(
            column.clone(),
            koldstore_manifest::ManifestColumnStats::new(min.clone(), max.clone()),
        );
    }

    stats
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
    prune_flushed_hot_rows(
        table_oid,
        &snapshot.primary_key_columns,
        &chunk.cleanup_rows,
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
    pgrx::Spi::run_with_args(
        r#"
INSERT INTO koldstore.cold_segments (
    segment_id,
    table_oid,
    scope_key,
    object_path,
    batch_number,
    min_seq,
    max_seq,
    min_commit_seq,
    max_commit_seq,
    row_count,
    byte_size,
    schema_version,
    column_stats,
    status
)
VALUES (
    $1::uuid,
    $2::oid,
    '',
    $3::text,
    $4::integer,
    $5::bigint,
    $6::bigint,
    $7::bigint,
    $8::bigint,
    $9::bigint,
    $10::bigint,
    $11::integer,
    $12::jsonb,
    'active'
)
"#,
        &[
            pgrx::datum::DatumWithOid::from(pgrx::Uuid::from_bytes(*segment_id.as_bytes())),
            pgrx::datum::DatumWithOid::from(table_oid),
            pgrx::datum::DatumWithOid::from(object_path),
            pgrx::datum::DatumWithOid::from(batch_number),
            pgrx::datum::DatumWithOid::from(stats.min_seq),
            pgrx::datum::DatumWithOid::from(stats.max_seq),
            pgrx::datum::DatumWithOid::from(stats.min_commit_seq),
            pgrx::datum::DatumWithOid::from(stats.max_commit_seq),
            pgrx::datum::DatumWithOid::from(stats.row_count),
            pgrx::datum::DatumWithOid::from(byte_size),
            pgrx::datum::DatumWithOid::from(schema_version),
            pgrx::datum::DatumWithOid::from(pgrx::JsonB(serde_json::json!({
                "seq": {"min": stats.min_seq, "max": stats.max_seq}
            }))),
        ],
    )
    .map_err(|error| error.to_string())
}

pub(super) fn upsert_manifest_row(
    table_oid: pgrx::pg_sys::Oid,
    manifest_path: &str,
    segment_count: i32,
    max_seq: i64,
    max_commit_seq: i64,
) -> Result<(), String> {
    let generation = uuid::Uuid::new_v4().to_string();
    pgrx::Spi::run_with_args(
        r#"
INSERT INTO koldstore.manifest (
    table_oid,
    scope_key,
    manifest_path,
    etag,
    generation,
    sync_state,
    segment_count,
    max_seq,
    max_commit_seq,
    last_error,
    updated_at
)
VALUES ($1::oid, '', $2::text, NULL, $3::text, 'in_sync', $4::integer, $5::bigint, $6::bigint, NULL, now())
ON CONFLICT (table_oid, scope_key)
DO UPDATE SET
    manifest_path = EXCLUDED.manifest_path,
    generation = EXCLUDED.generation,
    sync_state = 'in_sync',
    segment_count = EXCLUDED.segment_count,
    max_seq = EXCLUDED.max_seq,
    max_commit_seq = EXCLUDED.max_commit_seq,
    last_error = NULL,
    updated_at = now()
"#,
        &[
            pgrx::datum::DatumWithOid::from(table_oid),
            pgrx::datum::DatumWithOid::from(manifest_path),
            pgrx::datum::DatumWithOid::from(generation.as_str()),
            pgrx::datum::DatumWithOid::from(segment_count),
            pgrx::datum::DatumWithOid::from(max_seq),
            pgrx::datum::DatumWithOid::from(max_commit_seq),
        ],
    )
    .map_err(|error| error.to_string())
}

fn insert_cold_pk_hint(
    table_oid: pgrx::pg_sys::Oid,
    segment_id: uuid::Uuid,
    seed: &str,
    latest_seq: i64,
    latest_commit_seq: i64,
) -> Result<(), String> {
    pgrx::Spi::run_with_args(
        r#"
INSERT INTO koldstore.cold_pk_hints (
    table_oid,
    scope_key,
    pk_hash,
    segment_id,
    hint_kind,
    latest_seq,
    latest_commit_seq
)
VALUES ($1::oid, '', decode(md5($2::text), 'hex'), $3::uuid, 'exact', $4::bigint, $5::bigint)
ON CONFLICT DO NOTHING
"#,
        &[
            pgrx::datum::DatumWithOid::from(table_oid),
            pgrx::datum::DatumWithOid::from(seed),
            pgrx::datum::DatumWithOid::from(pgrx::Uuid::from_bytes(*segment_id.as_bytes())),
            pgrx::datum::DatumWithOid::from(latest_seq),
            pgrx::datum::DatumWithOid::from(latest_commit_seq),
        ],
    )
    .map_err(|error| error.to_string())
}

fn prune_flushed_hot_rows(
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
