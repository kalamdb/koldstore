//! Operational SQL helpers.

use koldstore_core::{CommitSeq, ScopeKey, SeqId, TableName};
use thiserror::Error;
use uuid::Uuid;

use crate::flush::job::{FlushJobPhase, FlushLeaseSeconds, JobLeaseEpoch};
use crate::spi::SpiStatement;

/// Placeholder status key names returned by table status.
pub const TABLE_STATUS_FIELDS: &[&str] = &[
    "hot_rows",
    "cold_segment_count",
    "manifest_state",
    "pending_jobs",
    "storage_binding",
    "last_error",
];

/// SQL-callable flush API function names exposed through pgrx.
pub const FLUSH_SQL_FUNCTIONS: &[&str] = &[
    "koldstore.set_flush_policy",
    "koldstore.enqueue_flush_job",
    "koldstore.flush_table",
    "koldstore.flush_pending",
    "koldstore.recover_segments",
];

/// Operational maintenance command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpsCommand {
    /// Export a managed table as a kalamdb-compatible archive.
    ExportTable { table_name: TableName },
    /// Import is a parser boundary until cold artifact ownership is implemented.
    ImportTable { table_name: TableName },
}

/// Operational planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OpsError {
    /// Unsupported command boundary.
    #[error("unsupported koldstore_exec command")]
    UnsupportedCommand,
    /// Import is intentionally not implemented in the MVP.
    #[error("IMPORT TABLE is not supported in this MVP")]
    ImportUnsupported,
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
}

/// Planned table status query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableStatusPlan {
    /// Table filter.
    pub table_name: TableName,
    /// Optional scope filter.
    pub scope_key: Option<ScopeKey>,
    /// Parameterized catalog statement.
    pub statement: SpiStatement,
}

/// Planned manifest backup query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupManifestPlan {
    /// Optional table filter.
    pub table_name: Option<TableName>,
    /// Optional scope filter.
    pub scope_key: Option<ScopeKey>,
    /// Parameterized manifest statement.
    pub statement: SpiStatement,
}

/// Planned cold storage validation query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidateColdStoragePlan {
    /// Optional table filter.
    pub table_name: Option<TableName>,
    /// Parameterized validation seed statement.
    pub statement: SpiStatement,
}

/// Planned recovery query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverSegmentsPlan {
    /// Recovery request.
    pub request: RecoverSegmentsRequest,
    /// Parameterized recovery/job statement.
    pub statement: SpiStatement,
}

/// Planned flush job claim query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushJobClaimPlan {
    /// Maximum jobs to claim.
    pub limit: u32,
    /// Lease duration to bind for each claimed job.
    pub lease_seconds: FlushLeaseSeconds,
    /// Parameterized claim statement.
    pub statement: SpiStatement,
}

/// Planned lease-guarded flush job progress update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushJobProgressPlan {
    /// Job id.
    pub job_id: Uuid,
    /// Current lease owner.
    pub lease_owner: Uuid,
    /// Current lease epoch.
    pub lease_epoch: JobLeaseEpoch,
    /// Durable phase.
    pub phase: FlushJobPhase,
    /// Last flushed `_seq` checkpoint.
    pub checkpoint_seq: SeqId,
    /// Last flushed `_commit_seq` checkpoint.
    pub checkpoint_commit_seq: CommitSeq,
    /// Completed batches.
    pub batches_completed: u32,
    /// Flushed rows.
    pub rows_flushed: u64,
    /// Parameterized progress statement.
    pub statement: SpiStatement,
}

/// Planned lease-guarded flush job finish update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushJobFinishPlan {
    /// Job id.
    pub job_id: Uuid,
    /// Current lease owner.
    pub lease_owner: Uuid,
    /// Current lease epoch.
    pub lease_epoch: JobLeaseEpoch,
    /// Whether the job finished successfully.
    pub success: bool,
    /// Optional error trace for failures.
    pub error_trace: Option<String>,
    /// Parameterized finish statement.
    pub statement: SpiStatement,
}

/// Planned `koldstore_exec` export/import boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KoldstoreExecPlan {
    /// Parsed command.
    pub command: OpsCommand,
    /// Archive manifest path for export commands.
    pub archive_manifest_path: String,
    /// Parameterized export statement.
    pub statement: SpiStatement,
}

/// Result of a cold-storage validation run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ValidationSummary {
    /// Number of manifest records checked.
    pub manifests_checked: u64,
    /// Number of cold segments checked.
    pub segments_checked: u64,
    /// Whether catalog consistency checks passed.
    pub catalog_consistent: bool,
}

/// Recovery request for orphan objects and local catalog repair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverSegmentsRequest {
    /// Optional table filter.
    pub table_name: Option<TableName>,
    /// Dry-run mode records what would happen without mutating cold artifacts.
    pub dry_run: bool,
}

/// Flush request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushRequest {
    /// Table name.
    pub table_name: TableName,
    /// Optional scope key.
    pub scope_key: Option<ScopeKey>,
    /// Force flush.
    pub force: bool,
}

/// Planned flush job enqueue mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushJobEnqueuePlan {
    /// Flush request.
    pub request: FlushRequest,
    /// Inclusive `_seq` upper bound for rows this job may flush.
    pub seq_upper_bound: Option<SeqId>,
    /// Parameterized enqueue statement.
    pub statement: SpiStatement,
}

/// Flush policy update request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetFlushPolicyRequest {
    /// Table name.
    pub table_name: TableName,
    /// New flush policy, or `None` to disable automatic flush.
    pub flush_policy: Option<String>,
}

/// Flush-pending request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushPendingRequest {
    /// Maximum pending scopes to flush.
    pub limit: u32,
}

/// Creates a flush policy update request.
#[must_use]
pub const fn set_flush_policy_request(
    table_name: TableName,
    flush_policy: Option<String>,
) -> SetFlushPolicyRequest {
    SetFlushPolicyRequest {
        table_name,
        flush_policy,
    }
}

/// Creates a flush request.
#[must_use]
pub const fn flush_table_request(
    table_name: TableName,
    scope_key: Option<ScopeKey>,
    force: bool,
) -> FlushRequest {
    FlushRequest {
        table_name,
        scope_key,
        force,
    }
}

/// Creates a flush-pending request.
#[must_use]
pub const fn flush_pending_request(limit: u32) -> FlushPendingRequest {
    FlushPendingRequest { limit }
}

/// Plans enqueueing a flush job for a table/scope and optional `_seq` watermark.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn enqueue_flush_job_plan(
    request: FlushRequest,
    seq_upper_bound: Option<SeqId>,
) -> Result<FlushJobEnqueuePlan, OpsError> {
    let statement = SpiStatement::write(
        "enqueue flush job",
        r#"
INSERT INTO koldstore.jobs (
    id,
    table_oid,
    scope_key,
    job_type,
    status,
    phase,
    flush_seq_upper_bound,
    payload
)
VALUES (
    gen_random_uuid(),
    $1::regclass::oid,
    COALESCE($2::text, ''),
    'flush',
    'pending',
    'pending',
    $3::bigint,
    jsonb_build_object('force', $4::boolean)
)
ON CONFLICT (table_oid, scope_key)
WHERE job_type = 'flush' AND status IN ('pending', 'running')
DO NOTHING
RETURNING id
"#,
    )
    .map_err(|error| OpsError::Spi(error.to_string()))?;

    Ok(FlushJobEnqueuePlan {
        request,
        seq_upper_bound,
        statement,
    })
}

/// Parses the limited `koldstore_exec` command boundary.
#[must_use]
pub fn classify_command(command: &str) -> Option<OpsCommand> {
    let normalized = command.trim();
    let upper = normalized.to_ascii_uppercase();
    if upper.starts_with("EXPORT TABLE ") {
        TableName::parse(&normalized["EXPORT TABLE ".len()..])
            .ok()
            .map(|table_name| OpsCommand::ExportTable { table_name })
    } else if upper.starts_with("IMPORT TABLE ") {
        TableName::parse(&normalized["IMPORT TABLE ".len()..])
            .ok()
            .map(|table_name| OpsCommand::ImportTable { table_name })
    } else {
        None
    }
}

/// Plans `koldstore.table_status`.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn table_status_plan(
    table_name: TableName,
    scope_key: Option<ScopeKey>,
) -> Result<TableStatusPlan, OpsError> {
    let statement = SpiStatement::read(
        "table status",
        "SELECT s.table_oid, m.sync_state AS manifest_state, COALESCE(m.segment_count, 0) AS cold_segment_count, COALESCE(j.pending_jobs, 0) AS pending_jobs, s.storage_id AS storage_binding, m.last_error FROM koldstore.schemas s LEFT JOIN koldstore.manifest m ON m.table_oid = s.table_oid AND ($2::text IS NULL OR m.scope_key = $2) LEFT JOIN LATERAL (SELECT count(*) AS pending_jobs FROM koldstore.jobs j WHERE j.table_oid = s.table_oid AND j.status IN ('pending', 'running') AND ($2::text IS NULL OR j.scope_key = $2)) j ON true WHERE s.table_oid = $1::regclass::oid",
    )
    .map_err(|error| OpsError::Spi(error.to_string()))?;

    Ok(TableStatusPlan {
        table_name,
        scope_key,
        statement,
    })
}

/// Plans `koldstore.backup_manifest`.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn backup_manifest_plan(
    table_name: Option<TableName>,
    scope_key: Option<ScopeKey>,
) -> Result<BackupManifestPlan, OpsError> {
    let statement = SpiStatement::read(
        "backup manifest",
        "SELECT manifest_path, etag, generation, max_seq, max_commit_seq FROM koldstore.manifest WHERE ($1::regclass IS NULL OR table_oid = $1::regclass::oid) AND ($2::text IS NULL OR scope_key = $2)",
    )
    .map_err(|error| OpsError::Spi(error.to_string()))?;

    Ok(BackupManifestPlan {
        table_name,
        scope_key,
        statement,
    })
}

/// Plans `koldstore.validate_cold_storage`.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn validate_cold_storage_plan(
    table_name: Option<TableName>,
) -> Result<ValidateColdStoragePlan, OpsError> {
    let statement = SpiStatement::read(
        "validate cold storage",
        "SELECT m.manifest_path, cs.object_path, cs.row_count, cs.column_stats, h.pk_hash FROM koldstore.manifest m LEFT JOIN koldstore.cold_segments cs ON cs.table_oid = m.table_oid AND cs.scope_key = m.scope_key AND cs.status = 'active' LEFT JOIN koldstore.cold_pk_hints h ON h.table_oid = cs.table_oid AND h.scope_key = cs.scope_key AND h.segment_id = cs.segment_id WHERE ($1::regclass IS NULL OR m.table_oid = $1::regclass::oid)",
    )
    .map_err(|error| OpsError::Spi(error.to_string()))?;

    Ok(ValidateColdStoragePlan {
        table_name,
        statement,
    })
}

/// Plans `koldstore.recover_segments`.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn recover_segments_plan(
    table_name: Option<TableName>,
    dry_run: bool,
) -> Result<RecoverSegmentsPlan, OpsError> {
    let statement = SpiStatement::write(
        "recover segments",
        "INSERT INTO koldstore.jobs (id, table_oid, job_type, status, attempts, error_trace) SELECT gen_random_uuid(), $1::regclass::oid, 'recover_segments', CASE WHEN $2::boolean THEN 'dry_run' ELSE 'pending' END, 0, NULL RETURNING id",
    )
    .map_err(|error| OpsError::Spi(error.to_string()))?;

    Ok(RecoverSegmentsPlan {
        request: RecoverSegmentsRequest {
            table_name,
            dry_run,
        },
        statement,
    })
}

/// Enqueues a flush job through the SQL API.
#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
#[pgrx::pg_extern(name = "enqueue_flush_job", schema = "koldstore", security_definer)]
pub fn enqueue_flush_job_pg(
    table_oid: pgrx::pg_sys::Oid,
    scope_key: Option<&str>,
    force: bool,
) -> i64 {
    enqueue_flush_job_pg_impl(table_oid, scope_key, force)
        .unwrap_or_else(|error| pgrx::error!("enqueue flush job failed: {error}"))
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn enqueue_flush_job_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    scope_key: Option<&str>,
    force: bool,
) -> Result<i64, String> {
    use pgrx::datum::DatumWithOid;

    let scope_key = scope_key
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
        .map(ScopeKey::new)
        .transpose()
        .map_err(|error| error.to_string())?;

    let inserted = pgrx::Spi::get_one_with_args::<i64>(
        r#"
WITH inserted AS (
    INSERT INTO koldstore.jobs (
        id,
        table_oid,
        scope_key,
        job_type,
        status,
        phase,
        flush_seq_upper_bound,
        payload
    )
    VALUES (
        gen_random_uuid(),
        $1::oid,
        COALESCE($2::text, ''),
        'flush',
        'pending',
        'pending',
        NULL,
        jsonb_build_object('force', $3::boolean)
    )
    ON CONFLICT (table_oid, scope_key)
    WHERE job_type = 'flush' AND status IN ('pending', 'running')
    DO NOTHING
    RETURNING 1
)
SELECT count(*)::bigint FROM inserted
"#,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(scope_key.as_ref().map(ScopeKey::as_str)),
            DatumWithOid::from(force),
        ],
    )
    .map_err(|error| error.to_string())?;
    Ok(inserted.unwrap_or(0))
}

/// Enqueues a segment recovery job through the SQL API.
#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
#[pgrx::pg_extern(name = "recover_segments", schema = "koldstore", security_definer)]
pub fn recover_segments_pg(table_oid: pgrx::pg_sys::Oid, dry_run: bool) -> i64 {
    recover_segments_pg_impl(table_oid, dry_run)
        .unwrap_or_else(|error| pgrx::error!("recover segments failed: {error}"))
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn recover_segments_pg_impl(table_oid: pgrx::pg_sys::Oid, dry_run: bool) -> Result<i64, String> {
    use pgrx::datum::DatumWithOid;

    let inserted = pgrx::Spi::get_one_with_args::<i64>(
        r#"
WITH inserted AS (
    INSERT INTO koldstore.jobs (
        id, table_oid, job_type, status, attempts, error_trace
    )
    SELECT
        gen_random_uuid(),
        $1::oid,
        'recover_segments',
        CASE WHEN $2::boolean THEN 'dry_run' ELSE 'pending' END,
        0,
        NULL
    RETURNING 1
)
SELECT count(*)::bigint FROM inserted
"#,
        &[DatumWithOid::from(table_oid), DatumWithOid::from(dry_run)],
    )
    .map_err(|error| error.to_string())?;
    Ok(inserted.unwrap_or(0))
}

/// Plans a scalable flush-job claim using row-level locking and leases.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn claim_flush_jobs_plan(
    limit: u32,
    lease_seconds: FlushLeaseSeconds,
) -> Result<FlushJobClaimPlan, OpsError> {
    let statement = SpiStatement::write(
        "claim flush jobs",
        r#"
WITH candidate AS (
    SELECT id
    FROM koldstore.jobs
    WHERE job_type = 'flush'
      AND status IN ('pending', 'running')
      AND run_after <= now()
      AND (
          status = 'pending'
          OR lease_expires_at IS NULL
          OR lease_expires_at < now()
      )
    ORDER BY priority DESC, updated_at, id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE koldstore.jobs AS j
SET status = 'running',
    phase = 'claimed',
    attempts = CASE WHEN j.status = 'pending' THEN j.attempts + 1 ELSE j.attempts END,
    lease_owner = $2::uuid,
    lease_expires_at = now() + ($3::integer * interval '1 second'),
    lease_epoch = j.lease_epoch + 1,
    updated_at = now(),
    last_heartbeat_at = now()
FROM candidate
WHERE j.id = candidate.id
RETURNING j.id, j.table_oid, j.scope_key, j.lease_epoch, j.flush_seq_upper_bound
"#,
    )
    .map_err(|error| OpsError::Spi(error.to_string()))?;

    Ok(FlushJobClaimPlan {
        limit,
        lease_seconds,
        statement,
    })
}

/// Plans a lease-guarded progress update for a running flush job.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
#[allow(clippy::too_many_arguments)]
pub fn flush_job_progress_plan(
    job_id: Uuid,
    lease_owner: Uuid,
    lease_epoch: JobLeaseEpoch,
    phase: FlushJobPhase,
    checkpoint_seq: SeqId,
    checkpoint_commit_seq: CommitSeq,
    batches_completed: u32,
    rows_flushed: u64,
) -> Result<FlushJobProgressPlan, OpsError> {
    let statement = SpiStatement::write(
        "flush job progress",
        r#"
UPDATE koldstore.jobs
SET phase = $4::text,
    checkpoint_seq = $5::bigint,
    checkpoint_commit_seq = $6::bigint,
    batches_completed = $7::integer,
    rows_flushed = $8::bigint,
    last_heartbeat_at = now(),
    updated_at = now()
WHERE id = $1::uuid
  AND lease_owner = $2::uuid
  AND lease_epoch = $3::bigint
  AND status = 'running'
RETURNING id
"#,
    )
    .map_err(|error| OpsError::Spi(error.to_string()))?;

    Ok(FlushJobProgressPlan {
        job_id,
        lease_owner,
        lease_epoch,
        phase,
        checkpoint_seq,
        checkpoint_commit_seq,
        batches_completed,
        rows_flushed,
        statement,
    })
}

/// Plans a lease-guarded finish update for a running flush job.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn finish_flush_job_plan(
    job_id: Uuid,
    lease_owner: Uuid,
    lease_epoch: JobLeaseEpoch,
    success: bool,
    error_trace: Option<String>,
) -> Result<FlushJobFinishPlan, OpsError> {
    let statement = SpiStatement::write(
        "finish flush job",
        r#"
UPDATE koldstore.jobs
SET status = CASE WHEN $4::boolean THEN 'completed' ELSE 'error' END,
    phase = CASE WHEN $4::boolean THEN 'finished' ELSE phase END,
    error_trace = $5::text,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = now(),
    updated_at = now()
WHERE id = $1::uuid
  AND lease_owner = $2::uuid
  AND lease_epoch = $3::bigint
  AND status = 'running'
RETURNING id
"#,
    )
    .map_err(|error| OpsError::Spi(error.to_string()))?;

    Ok(FlushJobFinishPlan {
        job_id,
        lease_owner,
        lease_epoch,
        success,
        error_trace,
        statement,
    })
}

/// Plans the limited `koldstore_exec` export/import boundary.
///
/// # Errors
///
/// Returns an error for unsupported commands, unsupported imports, or invalid
/// SPI statement metadata.
pub fn plan_koldstore_exec(command: &str) -> Result<KoldstoreExecPlan, OpsError> {
    match classify_command(command).ok_or(OpsError::UnsupportedCommand)? {
        OpsCommand::ExportTable { table_name } => {
            let archive_manifest_path =
                format!("{}/manifest.json", table_name.as_str().replace('.', "/"));
            let statement = SpiStatement::read(
                "export table archive",
                "SELECT m.manifest_path, cs.object_path, cs.row_count, cs.byte_size FROM koldstore.manifest m LEFT JOIN koldstore.cold_segments cs ON cs.table_oid = m.table_oid AND cs.scope_key = m.scope_key AND cs.status = 'active' WHERE m.table_oid = $1::regclass::oid",
            )
            .map_err(|error| OpsError::Spi(error.to_string()))?;
            Ok(KoldstoreExecPlan {
                command: OpsCommand::ExportTable { table_name },
                archive_manifest_path,
                statement,
            })
        }
        OpsCommand::ImportTable { .. } => Err(OpsError::ImportUnsupported),
    }
}

/// Flushes one managed table scope from SQL.
#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
#[pgrx::pg_extern(name = "flush_table", schema = "koldstore", security_definer)]
pub fn flush_table_pg(table_oid: pgrx::pg_sys::Oid) -> i64 {
    flush_table_pg_impl(table_oid)
        .unwrap_or_else(|error| pgrx::error!("flush table failed: {error}"))
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn flush_table_pg_impl(table_oid: pgrx::pg_sys::Oid) -> Result<i64, String> {
    let relation = relation_context(table_oid)?;
    let storage = flush_storage_context(table_oid)?;
    let stats = flush_stats(&relation.quoted)?;
    if stats.row_count == 0 {
        return Ok(0);
    }

    let batch_number = next_flush_batch_number(table_oid)?;
    let prefix = format!("{}/{}", relation.namespace, relation.name);
    let segment_path = format!("{prefix}/batch-{batch_number}.parquet");
    let manifest_path = format!("{prefix}/manifest.json");
    let absolute_segment_path = std::path::Path::new(&storage.base_path).join(&segment_path);
    let absolute_manifest_path = std::path::Path::new(&storage.base_path).join(&manifest_path);
    if let Some(parent) = absolute_segment_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }

    let byte_size = write_parquet_segment(&absolute_segment_path, stats.row_count, stats.min_seq)?;
    let segment_id = uuid::Uuid::new_v4();
    insert_cold_segment(
        table_oid,
        segment_id,
        &segment_path,
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
        segment_path.clone(),
        stats.min_seq..=stats.max_seq,
        stats.min_commit_seq..=stats.max_commit_seq,
        stats.row_count as u64,
        byte_size as u64,
        storage.schema_version as u32,
    );
    segment.column_stats.insert(
        "_seq".to_string(),
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
        &segment_path,
        stats.max_seq,
        stats.max_commit_seq,
    )?;
    mark_flush_jobs_completed(table_oid)?;

    Ok(stats.row_count)
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
#[derive(Debug)]
struct RelationContext {
    namespace: String,
    name: String,
    quoted: String,
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
#[derive(Debug)]
struct FlushStorageContext {
    base_path: String,
    schema_version: i32,
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
#[derive(Debug)]
struct FlushStats {
    row_count: i64,
    min_seq: i64,
    max_seq: i64,
    min_commit_seq: i64,
    max_commit_seq: i64,
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn relation_context(table_oid: pgrx::pg_sys::Oid) -> Result<RelationContext, String> {
    let value = spi_json_one(
        r#"
SELECT jsonb_build_object('namespace', n.nspname, 'name', c.relname)::text
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = $1::oid
"#,
        &[pgrx::datum::DatumWithOid::from(table_oid)],
        "relation lookup returned no rows",
    )?;
    let namespace = json_string(&value, "namespace")?;
    let name = json_string(&value, "name")?;
    Ok(RelationContext {
        quoted: format!("{}.{}", quote_ident(&namespace), quote_ident(&name)),
        namespace,
        name,
    })
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn flush_storage_context(table_oid: pgrx::pg_sys::Oid) -> Result<FlushStorageContext, String> {
    let value = spi_json_one(
        r#"
SELECT jsonb_build_object(
    'base_path', st.base_path,
    'schema_version', s.version
)::text
FROM koldstore.schemas s
JOIN koldstore.storage st ON st.id = s.storage_id
WHERE s.table_oid = $1::oid
  AND s.active
ORDER BY s.version DESC
LIMIT 1
"#,
        &[pgrx::datum::DatumWithOid::from(table_oid)],
        "active schema/storage lookup returned no rows",
    )?;
    Ok(FlushStorageContext {
        base_path: json_string(&value, "base_path")?,
        schema_version: json_i64(&value, "schema_version")? as i32,
    })
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn flush_stats(quoted_relation: &str) -> Result<FlushStats, String> {
    let value = spi_json_one_without_args(
        &format!(
            r#"
SELECT jsonb_build_object(
    'row_count', count(*),
    'min_seq', COALESCE(min("_seq"), 0),
    'max_seq', COALESCE(max("_seq"), 0),
    'min_commit_seq', COALESCE(min("_commit_seq"), 0),
    'max_commit_seq', COALESCE(max("_commit_seq"), 0)
)::text
FROM ONLY {quoted_relation}
WHERE NOT "_deleted"
"#
        ),
        "flush stats lookup returned no rows",
    )?;
    Ok(FlushStats {
        row_count: json_i64(&value, "row_count")?,
        min_seq: json_i64(&value, "min_seq")?,
        max_seq: json_i64(&value, "max_seq")?,
        min_commit_seq: json_i64(&value, "min_commit_seq")?,
        max_commit_seq: json_i64(&value, "max_commit_seq")?,
    })
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn next_flush_batch_number(table_oid: pgrx::pg_sys::Oid) -> Result<i32, String> {
    pgrx::Spi::get_one_with_args::<i32>(
        "SELECT COALESCE(max(batch_number), 0) + 1 FROM koldstore.cold_segments WHERE table_oid = $1::oid AND scope_key = ''",
        &[pgrx::datum::DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "batch number lookup returned no rows".to_string())
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn write_parquet_segment(
    path: &std::path::Path,
    row_count: i64,
    min_seq: i64,
) -> Result<i64, String> {
    use std::sync::Arc;

    let rows = (0..row_count)
        .map(|offset| min_seq + offset)
        .collect::<Vec<_>>();
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "_seq",
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
            .with_statistics_columns(["_seq"])
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

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
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
                "_seq": {"min": stats.min_seq, "max": stats.max_seq}
            }))),
        ],
    )
    .map_err(|error| error.to_string())
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
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

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
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

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
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

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn spi_json_one(
    sql: &str,
    args: &[pgrx::datum::DatumWithOid<'_>],
    missing_message: &str,
) -> Result<serde_json::Value, String> {
    let json = pgrx::Spi::get_one_with_args::<String>(sql, args)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| missing_message.to_string())?;
    serde_json::from_str(&json).map_err(|error| error.to_string())
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn spi_json_one_without_args(
    sql: &str,
    missing_message: &str,
) -> Result<serde_json::Value, String> {
    let json = pgrx::Spi::get_one::<String>(sql)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| missing_message.to_string())?;
    serde_json::from_str(&json).map_err(|error| error.to_string())
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn json_string(value: &serde_json::Value, key: &str) -> Result<String, String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| format!("missing string field `{key}`"))
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn json_i64(value: &serde_json::Value, key: &str) -> Result<i64, String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| format!("missing integer field `{key}`"))
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
