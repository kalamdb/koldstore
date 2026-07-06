//! PostgreSQL flush SQL entrypoints.

pub use koldstore_flush::ops::*;

#[cfg(feature = "pg")]
use koldstore_common::{ScopeKey, TableName};

/// Enqueues a flush job through the SQL API.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "enqueue_flush_job", schema = "koldstore", security_definer)]
pub fn enqueue_flush_job_pg(
    table_oid: pgrx::pg_sys::Oid,
    scope_key: Option<&str>,
    force: bool,
) -> i64 {
    enqueue_flush_job_pg_impl(table_oid, scope_key, force)
        .unwrap_or_else(|error| pgrx::error!("enqueue flush job failed: {error}"))
}

#[cfg(feature = "pg")]
fn enqueue_flush_job_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    scope_key: Option<&str>,
    force: bool,
) -> Result<i64, String> {
    use pgrx::datum::DatumWithOid;

    let table_name = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table_name = TableName::parse(&table_name).map_err(|error| error.to_string())?;
    let scope_key = scope_key
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
        .map(ScopeKey::new)
        .transpose()
        .map_err(|error| error.to_string())?;
    let scope_key_arg = scope_key
        .as_ref()
        .map(ScopeKey::as_str)
        .map(ToString::to_string);
    let plan = enqueue_flush_job_plan(flush_table_request(table_name, scope_key, force), None)
        .map_err(|error| error.to_string())?;

    let inserted = crate::spi::update_one::<pgrx::Uuid>(
        &plan.statement,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(scope_key_arg.as_deref()),
            DatumWithOid::from(Option::<i64>::None),
            DatumWithOid::from(force),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(if inserted.is_some() { 1 } else { 0 })
}

/// Enqueues a segment recovery job through the SQL API.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "recover_segments", schema = "koldstore", security_definer)]
pub fn recover_segments_pg(table_oid: pgrx::pg_sys::Oid, dry_run: bool) -> i64 {
    recover_segments_pg_impl(table_oid, dry_run)
        .unwrap_or_else(|error| pgrx::error!("recover segments failed: {error}"))
}

#[cfg(feature = "pg")]
fn recover_segments_pg_impl(table_oid: pgrx::pg_sys::Oid, dry_run: bool) -> Result<i64, String> {
    use pgrx::datum::DatumWithOid;

    let table_name = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table_name = TableName::parse(&table_name).map_err(|error| error.to_string())?;
    let plan =
        recover_segments_plan(Some(table_name), dry_run).map_err(|error| error.to_string())?;
    let inserted = crate::spi::update_one::<pgrx::Uuid>(
        &plan.statement,
        &[DatumWithOid::from(table_oid), DatumWithOid::from(dry_run)],
    )
    .map_err(|error| error.to_string())?;
    Ok(if inserted.is_some() { 1 } else { 0 })
}

/// Flushes one managed table scope from SQL.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "flush_table", schema = "koldstore", security_definer)]
pub fn flush_table_pg(table_oid: pgrx::pg_sys::Oid) -> i64 {
    flush_table_pg_impl(table_oid)
        .unwrap_or_else(|error| pgrx::error!("flush table failed: {error}"))
}

#[cfg(feature = "pg")]
fn flush_table_pg_impl(table_oid: pgrx::pg_sys::Oid) -> Result<i64, String> {
    let relation = crate::catalog::resolve::relation_context(table_oid)?;
    let storage = crate::catalog::resolve::active_flush_storage_context(table_oid)?;
    let stats = flush_stats(table_oid)?;
    if stats.row_count == 0 {
        return Ok(0);
    }

    let batch_number = next_flush_batch_number(table_oid)?;
    let prefix = format!("{}/{}", relation.namespace, relation.name);
    let batch_file_name = format!("batch-{batch_number}.parquet");
    let object_path = format!("{prefix}/{batch_file_name}");
    let manifest_path = format!("{prefix}/manifest.json");
    let absolute_segment_path = std::path::Path::new(&storage.base_path).join(&object_path);
    let absolute_manifest_path = std::path::Path::new(&storage.base_path).join(&manifest_path);
    if let Some(parent) = absolute_segment_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }

    let byte_size = write_parquet_segment(&absolute_segment_path, stats.row_count, stats.min_seq)?;
    let segment_checksum = parquet_sha256_checksum(&absolute_segment_path)?;
    let segment_id = uuid::Uuid::new_v4();
    insert_cold_segment(
        table_oid,
        segment_id,
        &object_path,
        batch_number,
        &stats,
        byte_size,
        storage.schema_version,
    )?;

    let mut manifest = if absolute_manifest_path.exists() {
        serde_json::from_str::<koldstore_manifest::Manifest>(
            &std::fs::read_to_string(&absolute_manifest_path).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?
    } else {
        koldstore_manifest::Manifest::new_shared(
            relation.namespace.clone(),
            relation.name.clone(),
            storage.schema_version as u32,
        )
    };
    let mut segment = koldstore_manifest::ManifestSegment::committed(
        batch_number as u32,
        batch_file_name,
        stats.min_seq..=stats.max_seq,
        stats.min_commit_seq..=stats.max_commit_seq,
        stats.row_count as u64,
        byte_size as u64,
        storage.schema_version as u32,
    );
    segment.checksum = Some(segment_checksum);
    segment.column_stats.insert(
        koldstore_parquet::ColdMetadataColumn::Seq
            .name()
            .to_string(),
        koldstore_manifest::ManifestColumnStats::new(
            serde_json::json!(stats.min_seq),
            serde_json::json!(stats.max_seq),
        ),
    );
    segment
        .bloom_filters
        .push(koldstore_manifest::ManifestBloomFilter::bloom(
            vec!["id".to_string()],
            Some(0.01),
        ));
    segment.pk_filter = Some(koldstore_manifest::PkFilter::exact(vec![1]));
    manifest.append_segment(segment);

    if let Some(parent) = absolute_manifest_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    std::fs::write(
        &absolute_manifest_path,
        serde_json::to_vec_pretty(&manifest).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;

    upsert_manifest_row(
        table_oid,
        &manifest_path,
        manifest.segments.len() as i32,
        manifest.max_seq,
        manifest.max_commit_seq,
    )?;
    insert_cold_pk_hint(
        table_oid,
        segment_id,
        &object_path,
        stats.max_seq,
        stats.max_commit_seq,
    )?;
    mark_flush_jobs_completed(table_oid)?;

    Ok(stats.row_count)
}

#[cfg(feature = "pg")]
#[derive(Debug)]
struct FlushStats {
    row_count: i64,
    min_seq: i64,
    max_seq: i64,
    min_commit_seq: i64,
    max_commit_seq: i64,
}

#[cfg(feature = "pg")]
fn flush_stats(table_oid: pgrx::pg_sys::Oid) -> Result<FlushStats, String> {
    use koldstore_mirror::{plan_mirror_stats, MirrorRelation};

    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = MirrorRelation::new(snapshot.mirror_relation);
    let stats = koldstore_mirror::mirror_to_sql(plan_mirror_stats(&mirror))
        .map_err(|error| error.to_string())?;
    let json = crate::spi::execute_prepared(&stats, &[], crate::spi::first_row::<String>)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "flush stats lookup returned no rows".to_string())?;
    let value =
        serde_json::from_str::<serde_json::Value>(&json).map_err(|error| error.to_string())?;
    Ok(FlushStats {
        row_count: crate::catalog::decode::json_i64(&value, "row_count")?,
        min_seq: crate::catalog::decode::json_i64(&value, "min_seq")?,
        max_seq: crate::catalog::decode::json_i64(&value, "max_seq")?,
        min_commit_seq: crate::catalog::decode::json_i64(&value, "min_commit_seq")?,
        max_commit_seq: crate::catalog::decode::json_i64(&value, "max_commit_seq")?,
    })
}

#[cfg(feature = "pg")]
fn next_flush_batch_number(table_oid: pgrx::pg_sys::Oid) -> Result<i32, String> {
    pgrx::Spi::get_one_with_args::<i32>(
        "SELECT COALESCE(max(batch_number), 0) + 1 FROM koldstore.cold_segments WHERE table_oid = $1::oid AND scope_key = ''",
        &[pgrx::datum::DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "batch number lookup returned no rows".to_string())
}

#[cfg(feature = "pg")]
fn write_parquet_segment(
    path: &std::path::Path,
    row_count: i64,
    min_seq: i64,
) -> Result<i64, String> {
    use std::sync::Arc;

    use koldstore_parquet::ColdMetadataColumn;

    let seq_column = ColdMetadataColumn::Seq.name();
    let rows = (0..row_count)
        .map(|offset| min_seq + offset)
        .collect::<Vec<_>>();
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        seq_column,
        arrow_schema::DataType::Int64,
        false,
    )]));
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::Int64Array::from(rows))],
    )
    .map_err(|error| error.to_string())?;
    let file = std::fs::File::create(path).map_err(|error| error.to_string())?;
    let writer = koldstore_parquet::ParquetSegmentWriter::new(
        koldstore_parquet::WriterOptions::default()
            .with_statistics_columns([seq_column])
            .with_bloom_filter_columns(["id"]),
    );
    writer
        .write_record_batches(file, schema, [batch])
        .map_err(|error| error.to_string())?;
    let len = std::fs::metadata(path)
        .map_err(|error| error.to_string())?
        .len();
    i64::try_from(len).map_err(|error| error.to_string())
}

#[cfg(feature = "pg")]
fn parquet_sha256_checksum(path: &std::path::Path) -> Result<String, String> {
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

#[cfg(feature = "pg")]
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

#[cfg(feature = "pg")]
fn upsert_manifest_row(
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

#[cfg(feature = "pg")]
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

#[cfg(feature = "pg")]
fn mark_flush_jobs_completed(table_oid: pgrx::pg_sys::Oid) -> Result<(), String> {
    pgrx::Spi::run_with_args(
        r#"
UPDATE koldstore.jobs
SET status = 'completed',
    phase = 'finished',
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = now(),
    updated_at = now()
WHERE table_oid = $1::oid
  AND job_type = 'flush'
  AND status IN ('pending', 'running')
"#,
        &[pgrx::datum::DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())
}
