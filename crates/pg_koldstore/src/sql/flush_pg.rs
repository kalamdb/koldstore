//! PostgreSQL flush SQL entrypoints.

pub use koldstore_flush::ops::*;

#[cfg(feature = "pg")]
use koldstore_common::{QualifiedTableName, ScopeKey, TableName};
#[cfg(feature = "pg")]
use koldstore_flush::policy::{select_mirror_flush_candidates, FlushPolicy, MirrorPolicyRow};
#[cfg(feature = "pg")]
use std::time::SystemTime;

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
pub fn flush_table_pg(table_oid: pgrx::pg_sys::Oid) -> pgrx::Uuid {
    flush_table_pg_impl(table_oid)
        .unwrap_or_else(|error| pgrx::error!("flush table failed: {error}"))
}

#[cfg(feature = "pg")]
fn flush_table_pg_impl(table_oid: pgrx::pg_sys::Oid) -> Result<pgrx::Uuid, String> {
    crate::sql::job_lock_pg::lock_table_job(table_oid)?;
    let job_id = ensure_flush_job(table_oid)?;
    mark_flush_job_running(job_id, table_oid)?;
    let relation = crate::catalog::resolve::relation_context(table_oid)?;
    let storage = crate::catalog::resolve::active_flush_storage_context(table_oid)?;
    let stats = resolve_flush_stats(table_oid, false)?;
    if stats.row_count == 0 {
        mark_flush_job_completed(job_id, table_oid, 0, 0, 0)?;
        return Ok(pgrx::Uuid::from_bytes(*job_id.as_bytes()));
    }
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let catalog = crate::sql::migrate_pg::migration_catalog(table_oid.to_u32())?;
    let write_input = crate::merge_scan::pg::with_custom_scan_disabled(|| {
        flush_write_input(
            table_oid,
            storage.schema_version as u32,
            &snapshot.primary_key_columns,
            &catalog.columns,
            stats.max_seq,
        )
    })?;
    if i64::try_from(write_input.rows.len()).map_err(|error| error.to_string())? != stats.row_count
    {
        return Err(format!(
            "flush row selection mismatch: stats reported {} rows but writer built {} rows",
            stats.row_count,
            write_input.rows.len()
        ));
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

    let byte_size = write_parquet_segment(
        &absolute_segment_path,
        &write_input.columns,
        &write_input.rows,
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
            snapshot.primary_key_columns.clone(),
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
    mark_flush_job_completed(
        job_id,
        table_oid,
        stats.row_count,
        stats.max_seq,
        stats.max_commit_seq,
    )?;
    prune_flushed_hot_rows(
        table_oid,
        &snapshot.primary_key_columns,
        &write_input.cleanup_rows,
    )?;
    crate::catalog::cache::invalidate_table(table_oid);

    Ok(pgrx::Uuid::from_bytes(*job_id.as_bytes()))
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
fn resolve_flush_stats(table_oid: pgrx::pg_sys::Oid, force: bool) -> Result<FlushStats, String> {
    let all = flush_stats(table_oid)?;
    if all.row_count == 0 || force {
        return Ok(all);
    }
    let Some(policy_text) = active_flush_policy_text(table_oid)? else {
        return Ok(all);
    };
    let policy = FlushPolicy::parse(&policy_text).map_err(|error| error.to_string())?;
    let rows = load_mirror_policy_rows(table_oid)?;
    let candidates = select_mirror_flush_candidates(&policy, &rows, SystemTime::now());
    if candidates.is_empty() {
        return Ok(FlushStats {
            row_count: 0,
            min_seq: 0,
            max_seq: 0,
            min_commit_seq: 0,
            max_commit_seq: 0,
        });
    }
    let seqs = candidates
        .iter()
        .map(|row| row.seq.get())
        .collect::<Vec<_>>();
    let min_seq = *seqs.iter().min().expect("flush candidates are non-empty");
    let max_seq = *seqs.iter().max().expect("flush candidates are non-empty");
    Ok(FlushStats {
        row_count: i64::try_from(candidates.len()).map_err(|error| error.to_string())?,
        min_seq,
        max_seq,
        min_commit_seq: min_seq,
        max_commit_seq: max_seq,
    })
}

#[cfg(feature = "pg")]
fn active_flush_policy_text(table_oid: pgrx::pg_sys::Oid) -> Result<Option<String>, String> {
    use pgrx::datum::DatumWithOid;

    let policy = pgrx::Spi::get_one_with_args::<String>(
        r#"
SELECT options->>'flush_policy'
FROM koldstore.schemas
WHERE table_oid = $1::oid
  AND active
ORDER BY version DESC
LIMIT 1
"#,
        &[DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())?;
    Ok(policy
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty()))
}

#[cfg(feature = "pg")]
fn load_mirror_policy_rows(table_oid: pgrx::pg_sys::Oid) -> Result<Vec<MirrorPolicyRow>, String> {
    use koldstore_mirror::MirrorRelation;

    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = MirrorRelation::new(snapshot.mirror_relation);
    let pk_json = snapshot
        .primary_key_columns
        .iter()
        .map(|column| format!("'{column}', mirror.\"{column}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
SELECT COALESCE(jsonb_agg(
    jsonb_build_object(
        'pk_json', jsonb_build_object({pk_json}),
        'seq', mirror."seq",
        'changed_at', mirror."changed_at"
    )
    ORDER BY mirror."seq"
)::text, '[]')
FROM {mirror} AS mirror
"#,
        mirror = mirror.quoted()
    );
    let json = pgrx::Spi::get_one::<String>(&sql)
        .map_err(|error| error.to_string())?
        .unwrap_or_else(|| "[]".to_string());
    let values =
        serde_json::from_str::<Vec<serde_json::Value>>(&json).map_err(|error| error.to_string())?;
    values
        .into_iter()
        .map(decode_mirror_policy_row)
        .collect::<Result<Vec<_>, _>>()
}

#[cfg(feature = "pg")]
fn decode_mirror_policy_row(value: serde_json::Value) -> Result<MirrorPolicyRow, String> {
    use koldstore_common::SeqId;

    let object = value
        .as_object()
        .ok_or_else(|| "mirror policy row must be a JSON object".to_string())?;
    let pk_json = object
        .get("pk_json")
        .cloned()
        .ok_or_else(|| "mirror policy row is missing `pk_json`".to_string())?;
    let seq = object
        .get("seq")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| "mirror policy row is missing integer `seq`".to_string())?;
    let changed_at = object
        .get("changed_at")
        .ok_or_else(|| "mirror policy row is missing `changed_at`".to_string())?;
    Ok(MirrorPolicyRow {
        pk_json,
        seq: SeqId::new(seq).map_err(|error| error.to_string())?,
        changed_at: parse_mirror_changed_at(changed_at)?,
    })
}

#[cfg(feature = "pg")]
fn parse_mirror_changed_at(value: &serde_json::Value) -> Result<SystemTime, String> {
    use chrono::{DateTime, NaiveDateTime, Utc};

    let text = value
        .as_str()
        .ok_or_else(|| "mirror `changed_at` must be a string".to_string())?;
    if let Ok(parsed) = DateTime::parse_from_rfc3339(text) {
        return Ok(SystemTime::from(parsed));
    }
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f %z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
    ];
    for format in formats {
        if let Ok(parsed) = DateTime::parse_from_str(text, format) {
            return Ok(SystemTime::from(parsed));
        }
        if let Ok(parsed) = NaiveDateTime::parse_from_str(text, format) {
            return Ok(SystemTime::from(
                DateTime::<Utc>::from_naive_utc_and_offset(parsed, Utc),
            ));
        }
    }
    Err(format!("unsupported mirror `changed_at` value `{text}`"))
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
struct FlushWriteInput {
    columns: Vec<koldstore_parquet::PgColumn>,
    rows: Vec<koldstore_parquet::CleanColdRecordPlan>,
    cleanup_rows: Vec<serde_json::Value>,
}

#[cfg(feature = "pg")]
fn flush_write_input(
    table_oid: pgrx::pg_sys::Oid,
    schema_version: u32,
    primary_key_columns: &[String],
    columns: &[koldstore_migrate::order::CatalogColumn],
    max_seq: i64,
) -> Result<FlushWriteInput, String> {
    use pgrx::datum::DatumWithOid;

    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table = QualifiedTableName::parse(&relation).map_err(|error| error.to_string())?;
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = QualifiedTableName::from_table_name(&snapshot.mirror_relation);
    let base_columns = columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let selection =
        plan_mirror_flush_selection(&table, &mirror, primary_key_columns, &base_columns, None)
            .map_err(|error| error.to_string())?;
    let statement = crate::spi::SpiStatement::read_with_params(
        "select flush rows as json",
        &format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(selected) ORDER BY selected.seq)::text, '[]') FROM ({}) AS selected",
            selection.statement.sql
        ),
        selection.statement.param_types.clone(),
    )
    .map_err(|error| error.to_string())?;
    let json = crate::spi::execute_prepared(
        &statement,
        &[DatumWithOid::from(max_seq)],
        crate::spi::first_row::<String>,
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "flush row selection returned no rows".to_string())?;
    let values =
        serde_json::from_str::<Vec<serde_json::Value>>(&json).map_err(|error| error.to_string())?;
    let columns = columns
        .iter()
        .map(|column| {
            koldstore_parquet::PgColumn::from_catalog(&column.name, &column.type_name, true)
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let rows = values
        .iter()
        .cloned()
        .map(|value| flush_row_plan(value, &base_columns, primary_key_columns, schema_version))
        .collect::<Result<Vec<_>, _>>()?;
    let cleanup_rows = values
        .iter()
        .map(|value| cleanup_row_json(value, primary_key_columns))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(FlushWriteInput {
        columns,
        rows,
        cleanup_rows,
    })
}

#[cfg(feature = "pg")]
fn flush_row_plan(
    value: serde_json::Value,
    base_columns: &[String],
    primary_key_columns: &[String],
    schema_version: u32,
) -> Result<koldstore_parquet::CleanColdRecordPlan, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "flush row selection must return JSON objects".to_string())?;
    let seq = object
        .get("seq")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| "flush row is missing integer field `seq`".to_string())?;
    let op = object
        .get("op")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| "flush row is missing integer field `op`".to_string())
        .and_then(|op| i16::try_from(op).map_err(|error| error.to_string()))?;
    let changed_at = object
        .get("changed_at")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "flush row is missing string field `changed_at`".to_string())?;
    let row_values = base_columns
        .iter()
        .map(|column| {
            (
                column.clone(),
                object
                    .get(column)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            )
        })
        .collect::<Vec<_>>();
    koldstore_parquet::plan_clean_cold_record(
        row_values,
        primary_key_columns,
        seq,
        op,
        changed_at,
        schema_version,
    )
}

#[cfg(feature = "pg")]
fn cleanup_row_json(
    value: &serde_json::Value,
    primary_key_columns: &[String],
) -> Result<serde_json::Value, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "flush row selection must return JSON objects".to_string())?;
    let seq = object
        .get("seq")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| "flush row is missing integer field `seq`".to_string())?;
    let op = object
        .get("op")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| "flush row is missing integer field `op`".to_string())
        .and_then(|op| i16::try_from(op).map_err(|error| error.to_string()))?;
    let mut row = serde_json::Map::new();
    for column in primary_key_columns {
        let value = object
            .get(column)
            .ok_or_else(|| format!("flush row is missing primary-key field `{column}`"))?;
        row.insert(
            column.clone(),
            serde_json::Value::String(value_to_cleanup_text(value)?),
        );
    }
    row.insert("seq".to_string(), serde_json::json!(seq));
    row.insert("op".to_string(), serde_json::json!(op));
    Ok(serde_json::Value::Object(row))
}

#[cfg(feature = "pg")]
fn value_to_cleanup_text(value: &serde_json::Value) -> Result<String, String> {
    match value {
        serde_json::Value::Null => {
            Err("cleanup row cannot contain null primary-key values".to_string())
        }
        serde_json::Value::String(text) => Ok(text.clone()),
        serde_json::Value::Number(number) => Ok(number.to_string()),
        serde_json::Value::Bool(flag) => Ok(flag.to_string()),
        other => serde_json::to_string(other).map_err(|error| error.to_string()),
    }
}

#[cfg(feature = "pg")]
fn prune_flushed_hot_rows(
    table_oid: pgrx::pg_sys::Oid,
    primary_key_columns: &[String],
    cleanup_rows: &[serde_json::Value],
) -> Result<(), String> {
    use koldstore_flush::cleanup::plan_clean_schema_cleanup;
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

#[cfg(feature = "pg")]
fn write_parquet_segment(
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
fn ensure_flush_job(table_oid: pgrx::pg_sys::Oid) -> Result<uuid::Uuid, String> {
    use pgrx::datum::DatumWithOid;

    let existing = pgrx::Spi::get_one_with_args::<pgrx::Uuid>(
        r#"
SELECT (
    SELECT id
    FROM koldstore.jobs
    WHERE table_oid = $1::oid
      AND scope_key = ''
      AND job_type = 'flush'
      AND status IN ('pending', 'running')
    ORDER BY updated_at, id
    LIMIT 1
)
"#,
        &[DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())?;
    if let Some(existing) = existing {
        return Ok(uuid::Uuid::from_bytes(*existing.as_bytes()));
    }

    let job_id = uuid::Uuid::new_v4();
    pgrx::Spi::run_with_args(
        r#"
INSERT INTO koldstore.jobs (
    id,
    table_oid,
    scope_key,
    job_type,
    status,
    phase,
    payload
)
VALUES (
    $1::uuid,
    $2::oid,
    '',
    'flush',
    'pending',
    'pending',
    jsonb_build_object('force', false)
)
"#,
        &[
            DatumWithOid::from(pgrx::Uuid::from_bytes(*job_id.as_bytes())),
            DatumWithOid::from(table_oid),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(job_id)
}

#[cfg(feature = "pg")]
fn mark_flush_job_running(job_id: uuid::Uuid, table_oid: pgrx::pg_sys::Oid) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    pgrx::Spi::run_with_args(
        r#"
UPDATE koldstore.jobs
SET status = 'running',
    phase = 'writing',
    attempts = CASE WHEN attempts = 0 THEN 1 ELSE attempts END,
    last_heartbeat_at = now(),
    updated_at = now()
WHERE id = $1::uuid
  AND table_oid = $2::oid
  AND job_type = 'flush'
  AND status IN ('pending', 'running')
"#,
        &[
            DatumWithOid::from(pgrx::Uuid::from_bytes(*job_id.as_bytes())),
            DatumWithOid::from(table_oid),
        ],
    )
    .map_err(|error| error.to_string())
}

#[cfg(feature = "pg")]
fn mark_flush_job_completed(
    job_id: uuid::Uuid,
    table_oid: pgrx::pg_sys::Oid,
    rows_flushed: i64,
    checkpoint_seq: i64,
    checkpoint_commit_seq: i64,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    pgrx::Spi::run_with_args(
        r#"
UPDATE koldstore.jobs
SET status = 'completed',
    phase = 'finished',
    rows_processed = $3::bigint,
    rows_flushed = $3::bigint,
    checkpoint_seq = $4::bigint,
    checkpoint_commit_seq = $5::bigint,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = now(),
    updated_at = now()
WHERE id = $1::uuid
  AND table_oid = $2::oid
  AND job_type = 'flush'
  AND status IN ('pending', 'running')
"#,
        &[
            DatumWithOid::from(pgrx::Uuid::from_bytes(*job_id.as_bytes())),
            DatumWithOid::from(table_oid),
            DatumWithOid::from(rows_flushed),
            DatumWithOid::from(checkpoint_seq),
            DatumWithOid::from(checkpoint_commit_seq),
        ],
    )
    .map_err(|error| error.to_string())
}
