//! Operational SQL planning for flush jobs and maintenance commands.
//!
//! Owns parameterized catalog statements for flush enqueue, recovery, and table
//! status queries. Inline flush job lifecycle lives in `table_jobs`. PostgreSQL
//! `#[pg_extern]` wrappers stay in `pg_koldstore`.

use koldstore_common::{
    is_safe_identifier, quote_ident, QualifiedTableName, ScopeKey, SeqId, SqlParamType,
    SqlStatement, TableName,
};
use thiserror::Error;

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

/// Planned recovery query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverSegmentsPlan {
    /// Recovery request.
    pub request: RecoverSegmentsRequest,
    /// Parameterized recovery/job statement.
    pub statement: SqlStatement,
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

/// Plans enqueueing a flush job for a table/scope and optional `_seq` watermark.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn enqueue_flush_job_plan(
    request: FlushRequest,
    seq_upper_bound: Option<SeqId>,
) -> Result<FlushJobEnqueuePlan, OpsError> {
    let statement = SqlStatement::write(
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
    .map_err(|error| OpsError::Sql(error.to_string()))?;

    Ok(FlushJobEnqueuePlan {
        request,
        seq_upper_bound,
        statement,
    })
}

/// Plans clean-schema flush selection from the mirror and base table.
///
/// The query is bounded by a captured mirror `seq` cutoff and joins the base
/// table only for live rows. Delete mirror rows still produce PK + metadata
/// records so cold tombstones can mask older cold rows.
///
/// # Errors
///
/// Returns an error when identifiers are unsafe or statement metadata cannot be prepared.
pub fn plan_mirror_flush_selection(
    table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key_columns: &[String],
    base_columns: &[String],
    scope_column: Option<&str>,
) -> Result<MirrorFlushSelectionPlan, OpsError> {
    plan_mirror_flush_selection_inner(
        table,
        mirror_table,
        primary_key_columns,
        base_columns,
        scope_column,
        None,
        MirrorFlushPaging::Unbounded,
    )
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
    plan_mirror_flush_selection_inner(
        table,
        mirror_table,
        primary_key_columns,
        base_columns,
        scope_column,
        mirror_ops,
        MirrorFlushPaging::KeysetLimit,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MirrorFlushPaging {
    /// Full selection up to `$1` max seq (tests / non-streaming callers).
    Unbounded,
    /// Keyset page: `$1` max seq, `$2` after seq, `$3` limit.
    KeysetLimit,
}

fn plan_mirror_flush_selection_inner(
    table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key_columns: &[String],
    base_columns: &[String],
    scope_column: Option<&str>,
    mirror_ops: Option<&[i16]>,
    paging: MirrorFlushPaging,
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
    let join = pk_columns
        .iter()
        .map(|column| format!("mirror.{column} = hot.{column}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let mut select_columns = base_columns
        .iter()
        .map(|column| {
            if pk_columns.iter().any(|pk| pk == column) {
                format!("mirror.{column} AS {column}")
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
        "(mirror.\"op\" = 3) AS deleted".to_string(),
    ]);

    let mut where_clauses = vec!["mirror.\"seq\" <= $1::bigint".to_string()];
    let (mut param_types, operation, limit_sql, scope_param) = match paging {
        MirrorFlushPaging::Unbounded => (
            vec![SqlParamType::BigInt],
            "select mirror-backed flush rows",
            "",
            2_usize,
        ),
        MirrorFlushPaging::KeysetLimit => (
            vec![
                SqlParamType::BigInt,
                SqlParamType::BigInt,
                SqlParamType::BigInt,
            ],
            "select mirror-backed flush rows batch",
            "\nLIMIT $3::bigint",
            4_usize,
        ),
    };
    if matches!(paging, MirrorFlushPaging::KeysetLimit) {
        where_clauses.push("mirror.\"seq\" > $2::bigint".to_string());
    }
    if let Some(ops) = mirror_ops {
        if !ops.is_empty() {
            where_clauses.push(mirror_ops_where_clause(ops));
        }
    }
    if let Some(scope_column) = scope_column {
        let predicate =
            koldstore_common::scope::scope_predicate_sql("mirror", scope_column, scope_param)
                .map_err(|error| OpsError::Sql(error.to_string()))?;
        where_clauses.push(predicate);
        param_types.push(SqlParamType::Text);
    }
    let sql = format!(
        r#"
SELECT {select_columns}
FROM {mirror} AS mirror
LEFT JOIN ONLY {table} AS hot
  ON {join}
WHERE {where_clause}
ORDER BY mirror."seq" ASC{limit_sql}
"#,
        select_columns = select_columns.join(", "),
        mirror = mirror_table.quoted(),
        table = table.quoted(),
        join = join,
        where_clause = where_clauses.join(" AND "),
        limit_sql = limit_sql,
    );
    let statement = SqlStatement::read_with_params(operation, &sql, param_types)
        .map_err(|error| OpsError::Sql(error.to_string()))?;

    Ok(MirrorFlushSelectionPlan {
        table: table.clone(),
        mirror_table: mirror_table.clone(),
        statement,
    })
}

fn mirror_ops_where_clause(ops: &[i16]) -> String {
    if ops.len() == 1 {
        format!("mirror.\"op\" = {}", ops[0])
    } else {
        let literals = ops
            .iter()
            .map(i16::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        format!("mirror.\"op\" IN ({literals})")
    }
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
            'checkpoint_seq', job_snapshot.checkpoint_seq,
            'checkpoint_commit_seq', job_snapshot.checkpoint_commit_seq,
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
            checkpoint_seq,
            checkpoint_commit_seq,
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
    let statement = SqlStatement::read(
        "backup manifest",
        "SELECT manifest_path, etag, generation, max_seq, max_commit_seq FROM koldstore.manifest WHERE ($1::regclass IS NULL OR table_oid = $1::regclass::oid) AND ($2::text IS NULL OR scope_key = $2)",
    )
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
    let statement = SqlStatement::read(
        "validate cold storage",
        "SELECT m.manifest_path, cs.object_path, cs.row_count, cs.column_stats FROM koldstore.manifest m LEFT JOIN koldstore.cold_segments cs ON cs.table_oid = m.table_oid AND cs.scope_key = m.scope_key AND cs.status = 'active' WHERE ($1::regclass IS NULL OR m.table_oid = $1::regclass::oid)",
    )
    .map_err(|error| OpsError::Sql(error.to_string()))?;

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
    let statement = SqlStatement::write(
        "recover segments",
        "INSERT INTO koldstore.jobs (id, table_oid, job_type, status, attempts, error_trace) SELECT gen_random_uuid(), $1::regclass::oid, 'recover_segments', CASE WHEN $2::boolean THEN 'dry_run' ELSE 'pending' END, 0, NULL RETURNING id",
    )
    .map_err(|error| OpsError::Sql(error.to_string()))?;

    Ok(RecoverSegmentsPlan {
        request: RecoverSegmentsRequest {
            table_name,
            dry_run,
        },
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
            let namespace = table_name.schema().unwrap_or("public");
            let archive_manifest_path =
                koldstore_manifest::relative_manifest_path(namespace, table_name.relation());
            let statement = SqlStatement::read(
                "export table archive",
                "SELECT m.manifest_path, cs.object_path, cs.row_count, cs.byte_size FROM koldstore.manifest m LEFT JOIN koldstore.cold_segments cs ON cs.table_oid = m.table_oid AND cs.scope_key = m.scope_key AND cs.status = 'active' WHERE m.table_oid = $1::regclass::oid",
            )
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
