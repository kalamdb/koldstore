//! Durable migration job planning.
//!
//! Existing-row migration is intentionally split into small catalog-visible
//! phases. A worker can crash after any committed batch; the next worker claims
//! the same job through the `koldstore.jobs` lease and skips rows that already
//! received `_seq`.

use std::num::{NonZeroU32, NonZeroUsize};

use koldstore_core::SeqId;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::spi::SpiStatement;

use super::{
    order::{MigrationOrdering, OrderingSource},
    QualifiedTableName,
};

/// Managed table type recorded for migration activation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedTableType {
    /// Shared table with a single logical scope.
    Shared,
    /// User-scoped table with one cold scope per scope key.
    User,
}

impl ManagedTableType {
    /// Returns the catalog representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::User => "user",
        }
    }
}

/// Positive migration batch size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MigrationBatchSize(NonZeroUsize);

impl MigrationBatchSize {
    /// Creates a positive migration batch size.
    ///
    /// # Errors
    ///
    /// Returns an error when the batch size is zero.
    pub fn new(value: usize) -> Result<Self, MigrationJobError> {
        NonZeroUsize::new(value)
            .map(Self)
            .ok_or(MigrationJobError::InvalidBatchSize)
    }

    /// Returns the raw batch size.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// Positive migration job lease duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationLeaseSeconds(NonZeroU32);

impl MigrationLeaseSeconds {
    /// Creates a positive lease duration.
    ///
    /// # Errors
    ///
    /// Returns an error when the duration is zero.
    pub fn new(value: u32) -> Result<Self, MigrationJobError> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(MigrationJobError::InvalidLeaseSeconds)
    }

    /// Returns the raw seconds value.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Monotonic lease epoch for migration jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationLeaseEpoch(i64);

impl MigrationLeaseEpoch {
    /// Creates a non-negative lease epoch.
    ///
    /// # Errors
    ///
    /// Returns an error when the epoch is negative.
    pub const fn new(value: i64) -> Result<Self, MigrationJobError> {
        if value < 0 {
            Err(MigrationJobError::InvalidLeaseEpoch)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the raw epoch value.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Migration job type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationJobKind {
    /// Backfills existing rows with `_seq`, `_commit_seq`, and `_deleted`.
    Backfill,
}

impl MigrationJobKind {
    /// Returns the `koldstore.jobs.job_type` value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Backfill => "migrate_backfill",
        }
    }
}

/// Durable migration phase names stored in `koldstore.jobs.phase`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationJobPhase {
    /// Job is queued before schema-column preparation.
    Pending,
    /// System columns are being added and defaults installed for future writes.
    AddSystemColumns,
    /// Existing rows are being assigned ordered `_seq` values.
    BackfillSeq,
    /// Backfilled system columns are being made non-null.
    FinalizeSystemColumns,
    /// A watermarked flush job is being enqueued.
    EnqueueFlush,
    /// Flush policy is active for periodic flushing.
    ActivatePeriodicFlush,
    /// Migration job finished.
    Finished,
}

impl MigrationJobPhase {
    /// Returns the catalog representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::AddSystemColumns => "add_system_columns",
            Self::BackfillSeq => "backfill_seq",
            Self::FinalizeSystemColumns => "finalize_system_columns",
            Self::EnqueueFlush => "enqueue_flush",
            Self::ActivatePeriodicFlush => "activate_periodic_flush",
            Self::Finished => "finished",
        }
    }
}

/// JSON payload recorded on a migration backfill job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationBackfillPayload {
    /// Normalized target table name.
    pub table_name: String,
    /// Managed table type.
    pub table_type: ManagedTableType,
    /// Storage binding used when the table becomes active.
    pub storage_id: Uuid,
    /// Optional user-scope column.
    pub scope_column: Option<String>,
    /// Backfill order column.
    pub order_column: String,
    /// Backfill ordering source.
    pub order_source: OrderingSource,
    /// Bounded rows per batch.
    pub batch_size: MigrationBatchSize,
    /// Optional periodic flush policy activated after migration.
    pub flush_policy: Option<String>,
    /// Rows processed so far. The SQL job row is authoritative; this is for
    /// operator-readable payload snapshots.
    pub processed_rows: u64,
}

/// Migration backfill enqueue request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationBackfillJobRequest {
    /// Job id generated by the caller.
    pub job_id: Uuid,
    /// Target PostgreSQL table oid.
    pub table_oid: u32,
    /// Payload stored in `koldstore.jobs.payload`.
    pub payload: MigrationBackfillPayload,
}

impl MigrationBackfillJobRequest {
    /// Creates a migration backfill request.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        job_id: Uuid,
        table_oid: u32,
        table: &QualifiedTableName,
        table_type: ManagedTableType,
        storage_id: Uuid,
        scope_column: Option<String>,
        ordering: &MigrationOrdering,
        batch_size: MigrationBatchSize,
        flush_policy: Option<String>,
    ) -> Self {
        Self {
            job_id,
            table_oid,
            payload: MigrationBackfillPayload {
                table_name: normalized_table_name(table),
                table_type,
                storage_id,
                scope_column,
                order_column: ordering.column.clone(),
                order_source: ordering.source,
                batch_size,
                flush_policy,
                processed_rows: 0,
            },
        }
    }
}

/// Planned job enqueue statement.
#[derive(Debug, Clone, PartialEq)]
pub struct MigrationJobEnqueuePlan {
    /// Job id to bind as `$1`.
    pub job_id: Uuid,
    /// Table oid to bind as `$2`.
    pub table_oid: u32,
    /// Payload to bind as `$3`.
    pub payload: serde_json::Value,
    /// Parameterized enqueue SQL.
    pub statement: SpiStatement,
}

/// Planned migration job claim statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationJobClaimPlan {
    /// Maximum jobs to claim.
    pub limit: u32,
    /// Lease duration.
    pub lease_seconds: MigrationLeaseSeconds,
    /// Parameterized claim SQL.
    pub statement: SpiStatement,
}

/// Planned ordered backfill batch statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationBackfillBatchPlan {
    /// Target table.
    pub table: QualifiedTableName,
    /// Oldest-to-newest ordering.
    pub ordering: MigrationOrdering,
    /// Batch size to bind as `$1`.
    pub batch_size: MigrationBatchSize,
    /// Parameterized batch SQL.
    pub statement: SpiStatement,
}

/// Planned migration progress statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationJobProgressPlan {
    /// Job id.
    pub job_id: Uuid,
    /// Lease owner.
    pub lease_owner: Uuid,
    /// Lease epoch.
    pub lease_epoch: MigrationLeaseEpoch,
    /// Durable phase.
    pub phase: MigrationJobPhase,
    /// Last observed `_seq` checkpoint.
    pub checkpoint_seq: SeqId,
    /// Rows processed by this progress update.
    pub rows_processed_increment: u64,
    /// Parameterized progress SQL.
    pub statement: SpiStatement,
}

/// Planned backfill completion and flush handoff statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationBackfillFinishPlan {
    /// Target table.
    pub table: QualifiedTableName,
    /// Optional user-scope column used to enqueue one flush per scope.
    pub scope_column: Option<String>,
    /// Job id.
    pub job_id: Uuid,
    /// Lease owner.
    pub lease_owner: Uuid,
    /// Lease epoch.
    pub lease_epoch: MigrationLeaseEpoch,
    /// Inclusive `_seq` upper bound for the initial flush.
    pub flush_seq_upper_bound: SeqId,
    /// Parameterized finish SQL.
    pub statement: SpiStatement,
}

/// Migration job planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MigrationJobError {
    /// Table oid is missing.
    #[error("table_oid cannot be zero")]
    MissingTableOid,
    /// Batch size must be positive.
    #[error("migration batch_size must be greater than zero")]
    InvalidBatchSize,
    /// Lease seconds must be positive.
    #[error("migration lease_seconds must be greater than zero")]
    InvalidLeaseSeconds,
    /// Lease epoch must be non-negative.
    #[error("migration lease_epoch must be non-negative")]
    InvalidLeaseEpoch,
    /// Identifier is unsafe to quote.
    #[error("invalid migration identifier `{0}`")]
    InvalidIdentifier(String),
    /// Payload serialization failed.
    #[error("migration payload serialization failed: {0}")]
    Payload(String),
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
}

/// Builds the enqueue plan for a migration backfill job.
///
/// # Errors
///
/// Returns an error when table metadata is invalid or statement metadata cannot
/// be prepared.
pub fn enqueue_migration_backfill_job_plan(
    request: MigrationBackfillJobRequest,
) -> Result<MigrationJobEnqueuePlan, MigrationJobError> {
    if request.table_oid == 0 {
        return Err(MigrationJobError::MissingTableOid);
    }

    let payload = serde_json::to_value(&request.payload)
        .map_err(|error| MigrationJobError::Payload(error.to_string()))?;
    let statement = SpiStatement::write(
        "enqueue migration backfill job",
        r#"
INSERT INTO koldstore.jobs (
    id,
    table_oid,
    scope_key,
    job_type,
    status,
    phase,
    priority,
    rows_processed,
    payload
)
VALUES (
    $1::uuid,
    $2::oid,
    '',
    'migrate_backfill',
    'pending',
    'add_system_columns',
    100,
    0,
    $3::jsonb
)
ON CONFLICT DO NOTHING
RETURNING id
"#,
    )
    .map_err(|error| MigrationJobError::Spi(error.to_string()))?;

    Ok(MigrationJobEnqueuePlan {
        job_id: request.job_id,
        table_oid: request.table_oid,
        payload,
        statement,
    })
}

/// Builds a scalable migration-job claim plan.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be prepared.
pub fn claim_migration_jobs_plan(
    limit: u32,
    lease_seconds: MigrationLeaseSeconds,
) -> Result<MigrationJobClaimPlan, MigrationJobError> {
    let statement = SpiStatement::write(
        "claim migration jobs",
        r#"
WITH candidate AS (
    SELECT id
    FROM koldstore.jobs
    WHERE job_type IN ('migrate_backfill')
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
    phase = CASE WHEN j.phase = 'pending' THEN 'add_system_columns' ELSE j.phase END,
    attempts = CASE WHEN j.status = 'pending' THEN j.attempts + 1 ELSE j.attempts END,
    lease_owner = $2::uuid,
    lease_expires_at = now() + ($3::integer * interval '1 second'),
    lease_epoch = j.lease_epoch + 1,
    updated_at = now(),
    last_heartbeat_at = now()
FROM candidate
WHERE j.id = candidate.id
RETURNING j.id, j.table_oid, j.phase, j.lease_epoch, j.checkpoint_seq, j.rows_processed, j.payload
"#,
    )
    .map_err(|error| MigrationJobError::Spi(error.to_string()))?;

    Ok(MigrationJobClaimPlan {
        limit,
        lease_seconds,
        statement,
    })
}

/// Builds a bounded, ordered existing-row backfill batch plan.
///
/// # Errors
///
/// Returns an error when identifiers are unsafe or statement metadata cannot be
/// prepared.
pub fn backfill_batch_plan(
    table: &QualifiedTableName,
    ordering: MigrationOrdering,
    batch_size: MigrationBatchSize,
) -> Result<MigrationBackfillBatchPlan, MigrationJobError> {
    if !is_safe_identifier(&ordering.column) {
        return Err(MigrationJobError::InvalidIdentifier(ordering.column));
    }
    let order_column = quoted_ident(&ordering.column);
    let sql = format!(
        r#"
WITH candidate AS MATERIALIZED (
    SELECT ctid, {order_column} AS migration_order_value
    FROM ONLY {table}
    WHERE "_seq" IS NULL
    ORDER BY {order_column} ASC, ctid ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
),
assigned AS MATERIALIZED (
    SELECT
        ctid,
        nextval('koldstore.global_seq'::regclass) AS assigned_seq,
        nextval('koldstore.global_commit_seq'::regclass) AS assigned_commit_seq
    FROM candidate
    ORDER BY migration_order_value ASC, ctid ASC
)
UPDATE ONLY {table} AS hot
SET "_seq" = assigned.assigned_seq,
    "_commit_seq" = assigned.assigned_commit_seq,
    "_deleted" = false
FROM assigned
WHERE hot.ctid = assigned.ctid
  AND hot."_seq" IS NULL
RETURNING hot."_seq", hot."_commit_seq"
"#,
        table = table.quoted(),
    );
    let statement = SpiStatement::write("backfill migration batch", &sql)
        .map_err(|error| MigrationJobError::Spi(error.to_string()))?;

    Ok(MigrationBackfillBatchPlan {
        table: table.clone(),
        ordering,
        batch_size,
        statement,
    })
}

/// Builds a lease-guarded migration progress update.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be prepared.
pub fn migration_job_progress_plan(
    job_id: Uuid,
    lease_owner: Uuid,
    lease_epoch: MigrationLeaseEpoch,
    phase: MigrationJobPhase,
    checkpoint_seq: SeqId,
    rows_processed_increment: u64,
) -> Result<MigrationJobProgressPlan, MigrationJobError> {
    let statement = SpiStatement::write(
        "migration job progress",
        r#"
UPDATE koldstore.jobs
SET phase = $4::text,
    checkpoint_seq = GREATEST(checkpoint_seq, $5::bigint),
    rows_processed = rows_processed + $6::bigint,
    batches_completed = batches_completed + 1,
    payload = jsonb_set(payload, '{processed_rows}', to_jsonb(rows_processed + $6::bigint), true),
    last_heartbeat_at = now(),
    updated_at = now()
WHERE id = $1::uuid
  AND lease_owner = $2::uuid
  AND lease_epoch = $3::bigint
  AND status = 'running'
RETURNING id
"#,
    )
    .map_err(|error| MigrationJobError::Spi(error.to_string()))?;

    Ok(MigrationJobProgressPlan {
        job_id,
        lease_owner,
        lease_epoch,
        phase,
        checkpoint_seq,
        rows_processed_increment,
        statement,
    })
}

/// Builds the lease-guarded backfill finish and initial flush handoff plan.
///
/// # Errors
///
/// Returns an error when the optional scope column is unsafe or statement
/// metadata cannot be prepared.
pub fn finish_backfill_and_enqueue_flush_plan(
    table: &QualifiedTableName,
    scope_column: Option<&str>,
    job_id: Uuid,
    lease_owner: Uuid,
    lease_epoch: MigrationLeaseEpoch,
    flush_seq_upper_bound: SeqId,
) -> Result<MigrationBackfillFinishPlan, MigrationJobError> {
    let scope_column = scope_column
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(|column| {
            if is_safe_identifier(column) {
                Ok(column.to_string())
            } else {
                Err(MigrationJobError::InvalidIdentifier(column.to_string()))
            }
        })
        .transpose()?;
    let scope_source = scope_lateral_sql(table, scope_column.as_deref());
    let sql = format!(
        r#"
WITH finished AS (
    UPDATE koldstore.jobs
    SET status = 'completed',
        phase = 'finished',
        flush_seq_upper_bound = $4::bigint,
        lease_owner = NULL,
        lease_expires_at = NULL,
        last_heartbeat_at = now(),
        updated_at = now()
    WHERE id = $1::uuid
      AND lease_owner = $2::uuid
      AND lease_epoch = $3::bigint
      AND status = 'running'
    RETURNING table_oid
),
activated AS (
    UPDATE koldstore.schemas AS s
    SET active = true,
        options = jsonb_set(s.options, '{{migration_status}}', '"active"'::jsonb, true),
        updated_at = now()
    FROM finished
    WHERE s.table_oid = finished.table_oid
    RETURNING s.table_oid
),
flush_scope AS (
    {scope_source}
),
flush_job AS (
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
    SELECT
        gen_random_uuid(),
        finished.table_oid,
        flush_scope.scope_key,
        'flush',
        'pending',
        'pending',
        $4::bigint,
        jsonb_build_object('source', 'migration', 'force', false)
    FROM finished
    CROSS JOIN flush_scope
    ON CONFLICT DO NOTHING
    RETURNING id
)
SELECT
    (SELECT count(*) FROM activated) AS activated_tables,
    (SELECT count(*) FROM flush_job) AS flush_jobs
"#,
    );
    let statement = SpiStatement::write("finish migration backfill", &sql)
        .map_err(|error| MigrationJobError::Spi(error.to_string()))?;

    Ok(MigrationBackfillFinishPlan {
        table: table.clone(),
        scope_column,
        job_id,
        lease_owner,
        lease_epoch,
        flush_seq_upper_bound,
        statement,
    })
}

fn scope_lateral_sql(table: &QualifiedTableName, scope_column: Option<&str>) -> String {
    match scope_column {
        Some(scope_column) => format!(
            r#"SELECT DISTINCT COALESCE(hot.{scope_column}::text, '') AS scope_key
    FROM ONLY {table} AS hot
    WHERE hot."_seq" <= $4::bigint"#,
            scope_column = quoted_ident(scope_column),
            table = table.quoted(),
        ),
        None => "SELECT ''::text AS scope_key".to_string(),
    }
}

fn normalized_table_name(table: &QualifiedTableName) -> String {
    match table.schema.as_deref() {
        Some(schema) => format!("{schema}.{}", table.name),
        None => table.name.clone(),
    }
}

fn quoted_ident(identifier: &str) -> String {
    format!("\"{identifier}\"")
}

fn is_safe_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}
