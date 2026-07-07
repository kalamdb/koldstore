//! Parquet segment writes and catalog bookkeeping for flush.

use koldstore_catalog::decode::FlushStorageContext;
use koldstore_common::QualifiedTableName;
use koldstore_flush::cleanup::plan_clean_schema_cleanup;

use super::stats::FlushStats;
use super::write::FlushWriteChunk;

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

pub(super) fn parquet_sha256_checksum(path: &std::path::Path) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;

    let mut file = std::fs::File::open(path).map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = file.read(&mut buffer).map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
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
) -> Result<(koldstore_manifest::ManifestSegment, i64), String> {
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
    let segment_checksum = parquet_sha256_checksum(&absolute_segment_path)?;
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
    let mut segment = koldstore_manifest::ManifestSegment::committed(
        batch_number as u32,
        batch_file_name,
        chunk_stats.min_seq..=chunk_stats.max_seq,
        chunk_stats.min_commit_seq..=chunk_stats.max_commit_seq,
        chunk_stats.row_count as u64,
        byte_size as u64,
        storage.schema_version as u32,
    );
    segment.checksum = Some(segment_checksum);
    segment.column_stats.insert(
        koldstore_parquet::ColdMetadataColumn::Seq
            .name()
            .to_string(),
        koldstore_manifest::ManifestColumnStats::new(
            serde_json::json!(chunk_stats.min_seq),
            serde_json::json!(chunk_stats.max_seq),
        ),
    );
    segment
        .bloom_filters
        .push(koldstore_manifest::ManifestBloomFilter::bloom(
            snapshot.primary_key_columns.clone(),
            Some(0.01),
        ));
    segment.pk_filter = Some(koldstore_manifest::PkFilter::exact(vec![1]));
    insert_cold_pk_hint(
        table_oid,
        segment_id,
        &object_path,
        chunk_stats.max_seq,
        chunk_stats.max_commit_seq,
    )?;
    prune_flushed_hot_rows(table_oid, &snapshot.primary_key_columns, &chunk.cleanup_rows)?;
    Ok((segment, byte_size))
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
