//! Operational SQL planning for flush jobs and maintenance commands.
//!
//! Owns flush enqueue, recovery request shapes, `describe_table` (which mixes
//! catalog counters with live hot/mirror SQL), and thin wrappers around
//! catalog-owned backup/validate/export SELECTs. Inline flush job lifecycle
//! lives in `table_jobs`. PostgreSQL `#[pg_extern]` wrappers stay in
//! `pg_koldstore`.

use koldstore_common::{
    is_safe_identifier, quote_ident, QualifiedTableName, ScopeKey, SeqId, SqlParamType,
    SqlStatement, TableName,
};
use thiserror::Error;

use crate::jobs_sql::ACTIVE_FLUSH_JOB_CONFLICT_PREDICATE;

/// Placeholder status key names returned by table status.
pub const TABLE_STATUS_FIELDS: &[&str] = &[
    "hot_rows",
    "cold_segment_count",
    "manifest_state",
    "pending_jobs",
    "jobs",
    "storage_binding",
    "last_error",
];

/// SQL-callable flush API function names exposed through pgrx.
pub const FLUSH_SQL_FUNCTIONS: &[&str] = &[
    "koldstore.enqueue_flush_job",
    "koldstore.flush_table",
    "koldstore.recover_segments",
    "koldstore.describe_table",
    "koldstore.manage_table",
    "koldstore.unmanage_table",
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
    Sql(String),
}

/// Planned table status query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableStatusPlan {
    /// Table filter.
    pub table_name: TableName,
    /// Parameterized catalog statement.
    pub statement: SqlStatement,
}

/// Planned manifest backup query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupManifestPlan {
    /// Optional table filter.
    pub table_name: Option<TableName>,
    /// Optional scope filter.
    pub scope_key: Option<ScopeKey>,
    /// Parameterized manifest statement.
    pub statement: SqlStatement,
}

/// Planned cold storage validation query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidateColdStoragePlan {
    /// Optional table filter.
    pub table_name: Option<TableName>,
    /// Parameterized validation seed statement.
    pub statement: SqlStatement,
}

/// Planned recovery query metadata for library callers / tests.
///
/// Live orphan recovery is executed inline by
/// `koldstore.recover_segments` in `pg_koldstore` (LIST + pending expiry). This
/// plan only carries the request; it does **not** enqueue jobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverSegmentsPlan {
    /// Recovery request.
    pub request: RecoverSegmentsRequest,
}

/// Planned `koldstore_exec` export/import boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KoldstoreExecPlan {
    /// Parsed command.
    pub command: OpsCommand,
    /// Archive manifest path for export commands.
    pub archive_manifest_path: String,
    /// Parameterized export statement.
    pub statement: SqlStatement,
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
    pub statement: SqlStatement,
}

/// Planned clean-schema mirror flush selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorFlushSelectionPlan {
    /// Source user table.
    pub table: QualifiedTableName,
    /// Table-specific mirror table.
    pub mirror_table: QualifiedTableName,
    /// Parameterized selection statement.
    pub statement: SqlStatement,
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

/// Plans INSERT … ON CONFLICT DO NOTHING then lookup of the active flush job id.
///
/// Inserts a pending flush job when none is active. On conflict with an existing
/// pending/running flush job, returns that job's id. When `force = true`, upgrades
/// an existing **pending** job's payload force intent.
///
/// Bind parameters:
/// - `$1` table oid (`regclass::oid`)
/// - `$2` scope key text (NULL → `''`)
/// - `$3` optional `flush_seq_upper_bound`
/// - `$4` force boolean
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn plan_enqueue_or_lookup_flush_job(
    request: FlushRequest,
    seq_upper_bound: Option<SeqId>,
) -> Result<FlushJobEnqueuePlan, OpsError> {
    let statement = SqlStatement::write(
        "enqueue or lookup flush job",
        &format!(
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
        $1::regclass::oid,
        COALESCE($2::text, ''),
        'flush',
        'pending',
        'pending',
        $3::bigint,
        jsonb_build_object('force', $4::boolean)
    )
    ON CONFLICT (table_oid, scope_key)
    WHERE {ACTIVE_FLUSH_JOB_CONFLICT_PREDICATE}
    DO NOTHING
    RETURNING id
),
force_upgrade AS (
    UPDATE koldstore.jobs j
    SET payload = j.payload || jsonb_build_object('force', true),
        updated_at = now()
    WHERE $4::boolean
      AND NOT EXISTS (SELECT 1 FROM inserted)
      AND j.table_oid = $1::regclass::oid
      AND j.scope_key = COALESCE($2::text, '')
      AND j.job_type = 'flush'
      AND j.status = 'pending'
      AND COALESCE((j.payload->>'force')::boolean, false) IS DISTINCT FROM true
    RETURNING j.id
)
SELECT COALESCE(
    (SELECT id FROM inserted LIMIT 1),
    (SELECT id FROM force_upgrade LIMIT 1),
    (
        SELECT id
        FROM koldstore.jobs
        WHERE table_oid = $1::regclass::oid
          AND scope_key = COALESCE($2::text, '')
          AND {ACTIVE_FLUSH_JOB_CONFLICT_PREDICATE}
        ORDER BY updated_at, id
        LIMIT 1
    )
)
"#
        ),
    )
    .map_err(|error| OpsError::Sql(error.to_string()))?;

    Ok(FlushJobEnqueuePlan {
        request,
        seq_upper_bound,
        statement,
    })
}

/// Plans selection of one due pending flush job for a one-shot executor.
///
/// Returns JSON `{"table_oid":…,"force":…}` or empty string when the queue is
/// empty. Does not lock the jobs row; session table ownership serializes claim.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn plan_select_pending_flush_candidate() -> Result<SqlStatement, OpsError> {
    SqlStatement::read(
        "select pending flush candidate",
        r#"
SELECT COALESCE((
    SELECT jsonb_build_object(
        'table_oid', table_oid::bigint,
        'force', COALESCE((payload->>'force')::boolean, false)
    )::text
    FROM koldstore.jobs
    WHERE job_type = 'flush'
      AND status = 'pending'
      AND available_at <= now()
    ORDER BY available_at, updated_at, id
    LIMIT 1
), '')
"#,
    )
    .map_err(|error| OpsError::Sql(error.to_string()))
}

/// Plans a count of due pending flush jobs (for executor spawn budget).
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn plan_count_pending_flush_jobs() -> Result<SqlStatement, OpsError> {
    SqlStatement::read(
        "count pending flush jobs",
        r#"
SELECT count(*)::bigint
FROM koldstore.jobs
WHERE job_type = 'flush'
  AND status = 'pending'
  AND available_at <= now()
"#,
    )
    .map_err(|error| OpsError::Sql(error.to_string()))
}

/// Plans one keyset-batched page of mirror-backed flush rows.
///
/// PERFORMANCE: Used by the streaming flush path. Returns one page of rows as a plain
/// `SELECT` (no `jsonb_agg`); `pg_koldstore` decodes SPI heap tuples directly.
///
/// Bind parameters:
/// - `$1` mirror `seq` upper bound (`max_seq`)
/// - `$2` exclusive lower bound (`after_seq`)
/// - `$3` page size limit
///
/// # Errors
///
/// Returns an error when identifiers are unsafe or statement metadata cannot be prepared.
pub fn plan_mirror_flush_selection_batch(
    table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key_columns: &[String],
    base_columns: &[String],
    scope_column: Option<&str>,
    mirror_ops: Option<&[i16]>,
) -> Result<MirrorFlushSelectionPlan, OpsError> {
    plan_mirror_flush_selection_batch_with_order_key(
        table,
        mirror_table,
        primary_key_columns,
        base_columns,
        scope_column,
        mirror_ops,
        false,
    )
}

/// Plans one keyset page and optionally returns the mirror's encoded order key.
///
/// # Errors
///
/// Returns an error when identifiers are unsafe or statement metadata cannot be prepared.
pub fn plan_mirror_flush_selection_batch_with_order_key(
    table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key_columns: &[String],
    base_columns: &[String],
    scope_column: Option<&str>,
    mirror_ops: Option<&[i16]>,
    include_order_key: bool,
) -> Result<MirrorFlushSelectionPlan, OpsError> {
    plan_mirror_flush_selection_inner(
        table,
        mirror_table,
        primary_key_columns,
        base_columns,
        scope_column,
        mirror_ops,
        include_order_key,
    )
}

#[allow(clippy::too_many_arguments)]
fn plan_mirror_flush_selection_inner(
    table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key_columns: &[String],
    base_columns: &[String],
    scope_column: Option<&str>,
    mirror_ops: Option<&[i16]>,
    include_order_key: bool,
) -> Result<MirrorFlushSelectionPlan, OpsError> {
    if primary_key_columns.is_empty() {
        return Err(OpsError::Sql(
            "flush selection requires primary key".to_string(),
        ));
    }
    let primary_key: Vec<&str> = primary_key_columns.iter().map(String::as_str).collect();
    let pk_columns = koldstore_mirror::quoted_pk_columns(&primary_key)
        .map_err(|error| OpsError::Sql(error.to_string()))?;
    let base_columns = base_columns
        .iter()
        .map(|column| validate_identifier(column))
        .collect::<Result<Vec<_>, _>>()?;
    // Tombstone-only passes only need PK + seq/op from the mirror; joining hot
    // would pull TOAST payloads that parquet nulls for deletes anyway.
    let delete_only = mirror_ops.is_some_and(|ops| ops == [3]);
    let mut select_columns = base_columns
        .iter()
        .map(|column| {
            if pk_columns.iter().any(|pk| pk == column) {
                format!("mirror.{column} AS {column}")
            } else if delete_only {
                format!("NULL AS {column}")
            } else {
                format!("hot.{column} AS {column}")
            }
        })
        .collect::<Vec<_>>();
    select_columns.extend([
        format!(
            "mirror.{} AS \"seq\"",
            koldstore_mirror::MirrorColumn::Seq.quoted_name()
        ),
        format!(
            "mirror.{} AS \"op\"",
            koldstore_mirror::MirrorColumn::Op.quoted_name()
        ),
    ]);
    if include_order_key {
        select_columns.push("mirror.\"order_key\" AS order_key".to_string());
    }

    let mut where_clauses = vec![
        "mirror.\"seq\" <= $1::bigint".to_string(),
        "mirror.\"seq\" > $2::bigint".to_string(),
    ];
    let mut param_types = vec![
        SqlParamType::BigInt,
        SqlParamType::BigInt,
        SqlParamType::BigInt,
    ];
    let scope_param = 4_usize;
    if let Some(ops) = mirror_ops {
        if !ops.is_empty() {
            where_clauses
                .push(crate::jobs_sql::mirror_ops_where_clause(ops).expect("non-empty ops"));
        }
    }
    if let Some(scope_column) = scope_column {
        let predicate =
            koldstore_common::scope::scope_predicate_sql("mirror", scope_column, scope_param)
                .map_err(|error| OpsError::Sql(error.to_string()))?;
        where_clauses.push(predicate);
        param_types.push(SqlParamType::Text);
    }
    let from_clause = if delete_only {
        format!("FROM {mirror} AS mirror", mirror = mirror_table.quoted())
    } else {
        let join = pk_columns
            .iter()
            .map(|column| format!("mirror.{column} = hot.{column}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        format!(
            "FROM {mirror} AS mirror\nLEFT JOIN ONLY {table} AS hot\n  ON {join}",
            mirror = mirror_table.quoted(),
            table = table.quoted(),
        )
    };
    let sql = format!(
        r#"
SELECT {select_columns}
{from_clause}
WHERE {where_clause}
ORDER BY mirror."seq" ASC
LIMIT $3::bigint
"#,
        select_columns = select_columns.join(", "),
        where_clause = where_clauses.join(" AND "),
    );
    let statement =
        SqlStatement::read_with_params("select mirror-backed flush rows batch", &sql, param_types)
            .map_err(|error| OpsError::Sql(error.to_string()))?;

    Ok(MirrorFlushSelectionPlan {
        table: table.clone(),
        mirror_table: mirror_table.clone(),
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

/// Plans `koldstore.describe_table` for one managed table and mirror relation.
///
/// The caller supplies validated quoted table and mirror relation names. The
/// returned JSON includes hot heap, mirror, and cold row accounting used by
/// storage verification tests and operators. Counters are table-wide.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn describe_table_plan(
    table: &QualifiedTableName,
    mirror: &QualifiedTableName,
) -> Result<TableStatusPlan, OpsError> {
    let statement = SqlStatement::read_with_params(
        "table status",
        &format!(
            r#"
SELECT jsonb_build_object(
    -- Treat 0 like unknown so a stale post-manage counter (async apply race)
    -- falls back to the live heap count, matching mirror_rows below.
    'hot_rows', COALESCE(NULLIF(m.hot_row_count, 0), (SELECT count(*)::bigint FROM ONLY {table})),
    'mirror_rows', COALESCE(NULLIF(m.mirror_row_count, 0), (SELECT count(*)::bigint FROM {mirror})),
    'cold_row_count', COALESCE(m.cold_row_count, (
        SELECT sum(cs.row_count)::bigint
        FROM koldstore.cold_segments cs
        WHERE cs.table_oid = $1::regclass::oid
          AND cs.status = 'active'
    ), 0),
    'cold_segment_count', COALESCE(NULLIF(m.segment_count, 0), (
        SELECT count(*)::bigint
        FROM koldstore.cold_segments cs
        WHERE cs.table_oid = $1::regclass::oid
          AND cs.status = 'active'
    ), 0),
    'heap_size_bytes', pg_relation_size($1::regclass),
    'table_size_bytes', pg_table_size($1::regclass),
    'index_size_bytes', pg_indexes_size($1::regclass),
    'manifest_state', m.sync_state,
    'manifest_max_seq', COALESCE(m.max_seq, 0),
    'pending_jobs', COALESCE(j.pending_jobs, 0),
    'jobs', COALESCE(jobs.jobs, '[]'::jsonb),
    'storage_binding', s.storage_id::text,
    'last_error', m.last_error
)::text
FROM koldstore.schemas s
LEFT JOIN koldstore.manifest m
  ON m.table_oid = s.table_oid
 AND m.scope_key = ''
LEFT JOIN LATERAL (
    SELECT count(*)::bigint AS pending_jobs
    FROM koldstore.jobs j
    WHERE j.table_oid = s.table_oid
      AND j.status IN ('pending', 'running')
) j ON true
LEFT JOIN LATERAL (
    SELECT jsonb_agg(
        jsonb_build_object(
            'id', job_snapshot.id::text,
            'job_type', job_snapshot.job_type,
            'status', job_snapshot.status,
            'phase', job_snapshot.phase,
            'rows_processed', job_snapshot.rows_processed,
            'rows_flushed', job_snapshot.rows_flushed,
            'batches_completed', job_snapshot.batches_completed,
            'progress_current', job_snapshot.progress_current,
            'progress_total', job_snapshot.progress_total,
            'checkpoint_seq', job_snapshot.checkpoint_seq,
            'duration_ms', COALESCE(
                (job_snapshot.payload->>'duration_ms')::bigint,
                GREATEST(
                    0,
                    (EXTRACT(EPOCH FROM (
                        CASE
                            WHEN job_snapshot.status IN (
                                'completed', 'error', 'cancelled', 'dry_run'
                            )
                                THEN job_snapshot.updated_at
                            ELSE now()
                        END
                        - COALESCE(
                            (job_snapshot.payload->>'started_at')::timestamptz,
                            job_snapshot.created_at
                        )
                    )) * 1000)::bigint
                )
            ),
            'updated_at', job_snapshot.updated_at
        )
        ORDER BY job_snapshot.updated_at DESC, job_snapshot.id
    ) AS jobs
    FROM (
        SELECT
            id,
            job_type,
            status,
            phase,
            rows_processed,
            rows_flushed,
            batches_completed,
            progress_current,
            progress_total,
            checkpoint_seq,
            payload,
            created_at,
            updated_at
        FROM koldstore.jobs
        WHERE table_oid = s.table_oid
        ORDER BY updated_at DESC, id
        LIMIT 20
    ) AS job_snapshot
) jobs ON true
WHERE s.table_oid = $1::regclass::oid
  AND s.active
LIMIT 1
"#,
            table = table.quoted(),
            mirror = mirror.quoted(),
        ),
        [SqlParamType::Oid],
    )
    .map_err(|error| OpsError::Sql(error.to_string()))?;

    Ok(TableStatusPlan {
        table_name: table
            .as_table_name()
            .map_err(|error| OpsError::Sql(error.to_string()))?,
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
    let statement = koldstore_catalog::queries::plan_backup_manifest_rows()
        .map_err(|error| OpsError::Sql(error.to_string()))?;

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
    let statement = koldstore_catalog::queries::plan_validate_cold_storage_rows()
        .map_err(|error| OpsError::Sql(error.to_string()))?;

    Ok(ValidateColdStoragePlan {
        table_name,
        statement,
    })
}

/// Builds a recovery request plan for library callers / contract tests.
///
/// Live recovery is executed by extension SPI (`LIST` orphans, expire pending
/// catalog rows, quarantine/delete objects). This helper does not enqueue jobs.
///
/// # Errors
///
/// Currently infallible; kept as `Result` for API stability with other ops plans.
pub fn recover_segments_plan(
    table_name: Option<TableName>,
    dry_run: bool,
) -> Result<RecoverSegmentsPlan, OpsError> {
    Ok(RecoverSegmentsPlan {
        request: RecoverSegmentsRequest {
            table_name,
            dry_run,
        },
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
            let namespace = table_name.schema().unwrap_or("public");
            let archive_manifest_path =
                koldstore_manifest::relative_manifest_path(namespace, table_name.relation());
            let statement = koldstore_catalog::queries::plan_export_table_archive_segments()
                .map_err(|error| OpsError::Sql(error.to_string()))?;
            Ok(KoldstoreExecPlan {
                command: OpsCommand::ExportTable { table_name },
                archive_manifest_path,
                statement,
            })
        }
        OpsCommand::ImportTable { .. } => Err(OpsError::ImportUnsupported),
    }
}
fn validate_identifier(value: &str) -> Result<String, OpsError> {
    let trimmed = value.trim();
    if is_safe_identifier(trimmed) {
        Ok(quote_ident(trimmed))
    } else {
        Err(OpsError::Sql(format!("invalid identifier `{value}`")))
    }
}
