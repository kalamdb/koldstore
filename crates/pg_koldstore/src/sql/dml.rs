//! Public DML SQL function boundaries.

use std::sync::atomic::{AtomicI64, Ordering};

use koldstore_core::{
    quote_ident, CommitSeq, MirrorOperation, PrimaryKeyColumnShape, Result, SeqId, TableName,
};
use koldstore_mirror::plan_upsert_mirror_row;
use thiserror::Error;

use crate::{
    migrate::QualifiedTableName, spi::SpiStatement, sql::session::snowflake_id_call_expression,
};

static NEXT_SEQ: AtomicI64 = AtomicI64::new(1);

/// Public cold-DML API function names exposed through pgrx.
pub const COLD_DML_FUNCTIONS: &[&str] = &[
    "koldstore.hydrate_pk",
    "koldstore.update_row",
    "koldstore.delete_row",
];

/// Change-log mirror DML capture planning result.
pub type MirrorCaptureResult<T> = std::result::Result<T, MirrorCaptureError>;

/// Change-log mirror DML capture planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MirrorCaptureError {
    /// A capture trigger cannot be generated without primary-key columns.
    #[error("mirror capture requires at least one primary-key column")]
    MissingPrimaryKey,
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
}

/// Planned mirror DML capture artifacts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorCapturePlan {
    /// Trigger function that upserts latest-state rows into the mirror.
    pub function: SpiStatement,
    /// INSERT capture trigger.
    pub insert_trigger: SpiStatement,
    /// UPDATE capture trigger.
    pub update_trigger: SpiStatement,
    /// DELETE capture trigger.
    pub delete_trigger: SpiStatement,
    /// Idempotent trigger cleanup statement.
    pub drop_triggers: SpiStatement,
    /// Idempotent trigger function cleanup statement.
    pub drop_function: SpiStatement,
}

impl MirrorCapturePlan {
    /// Trigger creation statements in dependency order.
    #[must_use]
    pub fn trigger_statements(&self) -> [&SpiStatement; 3] {
        [
            &self.insert_trigger,
            &self.update_trigger,
            &self.delete_trigger,
        ]
    }

    /// All create statements in dependency order.
    #[must_use]
    pub fn create_statements(&self) -> [&SpiStatement; 4] {
        [
            &self.function,
            &self.insert_trigger,
            &self.update_trigger,
            &self.delete_trigger,
        ]
    }
}

/// Plans transactional DML capture from a source table into its latest-state mirror.
///
/// # Errors
///
/// Returns an error when no primary-key columns are supplied or statement
/// metadata cannot be represented by the SPI helper.
pub fn plan_mirror_capture(
    source_table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key: &[PrimaryKeyColumnShape],
) -> MirrorCaptureResult<MirrorCapturePlan> {
    if primary_key.is_empty() {
        return Err(MirrorCaptureError::MissingPrimaryKey);
    }

    let function_name = QualifiedTableName {
        schema: Some("koldstore".to_string()),
        name: format!("{}_capture", mirror_table.name),
    };
    let source = source_table.quoted();
    let function_sql = capture_function_sql(&function_name, mirror_table, primary_key)?;
    let function = SpiStatement::write("create change-log mirror capture function", &function_sql)
        .map_err(|error| MirrorCaptureError::Spi(error.to_string()))?;
    let triggers: [SpiStatement; 3] = MirrorOperation::ALL
        .into_iter()
        .map(|operation| plan_capture_trigger(operation, mirror_table, &source, &function_name))
        .collect::<MirrorCaptureResult<Vec<_>>>()?
        .try_into()
        .expect("mirror capture has exactly three trigger operations");
    let [insert_trigger, update_trigger, delete_trigger] = triggers;
    let drop_triggers = SpiStatement::write(
        "drop change-log mirror capture triggers",
        &MirrorOperation::ALL
            .into_iter()
            .map(|operation| {
                format!(
                    "DROP TRIGGER IF EXISTS {} ON {source}",
                    quote_ident(&operation.capture_trigger_name(&mirror_table.name))
                )
            })
            .collect::<Vec<_>>()
            .join(";\n"),
    )
    .map_err(|error| MirrorCaptureError::Spi(error.to_string()))?;
    let drop_function = SpiStatement::write(
        "drop change-log mirror capture function",
        &format!("DROP FUNCTION IF EXISTS {}()", function_name.quoted()),
    )
    .map_err(|error| MirrorCaptureError::Spi(error.to_string()))?;

    Ok(MirrorCapturePlan {
        function,
        insert_trigger,
        update_trigger,
        delete_trigger,
        drop_triggers,
        drop_function,
    })
}

/// Plans idempotent teardown of mirror capture triggers and function.
///
/// Each statement is emitted separately so callers executing through SPI run
/// exactly one command per invocation.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be represented by the SPI
/// helper.
pub fn plan_mirror_capture_teardown(
    source_table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
) -> MirrorCaptureResult<Vec<SpiStatement>> {
    let function_name = QualifiedTableName {
        schema: Some("koldstore".to_string()),
        name: format!("{}_capture", mirror_table.name),
    };
    let source = source_table.quoted();
    let mut statements = Vec::with_capacity(4);
    for operation in MirrorOperation::ALL {
        let trigger_name = operation.capture_trigger_name(&mirror_table.name);
        statements.push(
            SpiStatement::write(
                &format!(
                    "drop change-log mirror {} capture trigger",
                    operation.capture_trigger_suffix()
                ),
                &format!(
                    "DROP TRIGGER IF EXISTS {} ON {source}",
                    quote_ident(&trigger_name)
                ),
            )
            .map_err(|error| MirrorCaptureError::Spi(error.to_string()))?,
        );
    }
    statements.push(
        SpiStatement::write(
            "drop change-log mirror capture function",
            &format!("DROP FUNCTION IF EXISTS {}()", function_name.quoted()),
        )
        .map_err(|error| MirrorCaptureError::Spi(error.to_string()))?,
    );
    Ok(statements)
}

fn capture_function_sql(
    function_name: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key: &[PrimaryKeyColumnShape],
) -> MirrorCaptureResult<String> {
    let mirror = mirror_table
        .as_mirror_relation()
        .map_err(|error| MirrorCaptureError::Spi(error.to_string()))?;
    let pk_names: Vec<&str> = primary_key
        .iter()
        .map(|column| column.column().as_str())
        .collect();
    let pk_values = |row_ref: &str| {
        primary_key
            .iter()
            .map(|column| format!("{row_ref}.{}", quote_ident(column.column().as_str())))
            .collect::<Vec<_>>()
    };
    let insert_upsert = plan_upsert_mirror_row(
        &mirror,
        &pk_names,
        &pk_values("NEW"),
        snowflake_id_call_expression(),
        MirrorOperation::Insert,
        "now()",
        "pg_current_wal_lsn()",
    )
    .map_err(|error| MirrorCaptureError::Spi(error.to_string()))?;
    let update_upsert = plan_upsert_mirror_row(
        &mirror,
        &pk_names,
        &pk_values("NEW"),
        snowflake_id_call_expression(),
        MirrorOperation::Update,
        "now()",
        "pg_current_wal_lsn()",
    )
    .map_err(|error| MirrorCaptureError::Spi(error.to_string()))?;
    let delete_upsert = plan_upsert_mirror_row(
        &mirror,
        &pk_names,
        &pk_values("OLD"),
        snowflake_id_call_expression(),
        MirrorOperation::Delete,
        "now()",
        "pg_current_wal_lsn()",
    )
    .map_err(|error| MirrorCaptureError::Spi(error.to_string()))?;
    let pk_update_guard = primary_key_update_guard(primary_key);

    Ok(format!(
        r#"
CREATE OR REPLACE FUNCTION {function_name}()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, koldstore
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        {insert_upsert}
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        {pk_update_guard}
        {update_upsert}
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        {delete_upsert}
        RETURN OLD;
    END IF;

    RAISE EXCEPTION 'unsupported pg-koldstore mirror capture operation %', TG_OP;
END;
$$
"#,
        function_name = function_name.quoted()
    ))
}

fn plan_capture_trigger(
    operation: MirrorOperation,
    mirror_table: &QualifiedTableName,
    source_table: &str,
    function_name: &QualifiedTableName,
) -> MirrorCaptureResult<SpiStatement> {
    let trigger_name = operation.capture_trigger_name(&mirror_table.name);
    SpiStatement::write(
        &format!(
            "create change-log mirror {} capture trigger",
            operation.capture_trigger_suffix()
        ),
        &capture_trigger_sql(&trigger_name, operation, source_table, function_name),
    )
    .map_err(|error| MirrorCaptureError::Spi(error.to_string()))
}

fn capture_trigger_sql(
    trigger_name: &str,
    operation: MirrorOperation,
    source_table: &str,
    function_name: &QualifiedTableName,
) -> String {
    format!(
        r#"
DROP TRIGGER IF EXISTS {trigger_name} ON {source_table};
CREATE TRIGGER {trigger_name}
AFTER {operation} ON {source_table}
FOR EACH ROW EXECUTE FUNCTION {function_name}()
"#,
        trigger_name = quote_ident(trigger_name),
        operation = operation.sql_trigger_event(),
        function_name = function_name.quoted()
    )
}

fn primary_key_update_guard(primary_key: &[PrimaryKeyColumnShape]) -> String {
    let changed_predicate = primary_key
        .iter()
        .map(|column| {
            let name = quote_ident(column.column().as_str());
            format!("OLD.{name} IS DISTINCT FROM NEW.{name}")
        })
        .collect::<Vec<_>>()
        .join(" OR ");

    format!(
        "IF {changed_predicate} THEN\n            RAISE EXCEPTION 'pg-koldstore does not support primary-key updates on managed table %', TG_TABLE_NAME;\n        END IF;"
    )
}

/// Result of a DML helper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DmlResult {
    /// Affected logical rows.
    pub affected_rows: i64,
    /// Whether a tombstone was written.
    pub tombstone_written: bool,
    /// Whether cold storage was read.
    pub cold_lookup_performed: bool,
}

/// Request for `koldstore.hydrate_pk`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HydratePkRequest {
    /// Relation name.
    pub table_name: TableName,
    /// Primary-key JSON.
    pub pk_json: serde_json::Value,
}

/// Managed DML operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedDmlOperation {
    /// Hot insert.
    Insert,
    /// Hot update.
    Update,
    /// Hot delete.
    Delete,
    /// Tombstone revive.
    Revive,
}

/// Request for `koldstore.update_row`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateRowRequest {
    /// Relation name.
    pub table_name: TableName,
    /// Primary-key JSON.
    pub pk_json: serde_json::Value,
    /// Patch JSON.
    pub patch_json: serde_json::Value,
    /// Whether the caller explicitly opted into cold lookup.
    pub lookup_cold: bool,
}

/// Cold-only update route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdUpdateOutcome {
    /// A live hot row can be updated with normal hot-path semantics.
    HotUpdate,
    /// Caller opted into a cold lookup and the row can be hydrated/updated.
    ColdLookupAndUpdate,
    /// Caller did not opt into cold lookup, so standard SQL affects zero rows.
    NoOpColdLookupDisabled,
    /// Caller opted into cold lookup but no local/cold candidate exists.
    NoOpNotFound,
}

impl UpdateRowRequest {
    /// Returns true when the request may read cold storage.
    #[must_use]
    pub const fn cold_lookup_allowed(&self) -> bool {
        self.lookup_cold
    }
}

/// Request for `koldstore.delete_row`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRowRequest {
    /// Relation name.
    pub table_name: TableName,
    /// Primary-key JSON.
    pub pk_json: serde_json::Value,
    /// Whether may-contain local metadata can produce an idempotent tombstone.
    pub allow_may_contain: bool,
}

/// Local state used to plan `koldstore.delete_row`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteInputState {
    /// A live hot row exists.
    HotRow,
    /// Exact local metadata says cold contains the PK.
    ColdExactLocalHint,
    /// May-contain local metadata says cold may contain the PK.
    ColdMayContainLocalHint,
    /// No hot row and no local cold hint.
    Missing,
}

impl DeleteRowRequest {
    /// Default `allow_may_contain` value from the SQL API contract.
    pub const DEFAULT_ALLOW_MAY_CONTAIN: bool = true;
}

impl ManagedDmlOperation {
    /// Returns whether the operation preserves the one-hot-row-per-PK invariant.
    #[must_use]
    pub const fn keeps_one_hot_row_per_pk(self) -> bool {
        matches!(
            self,
            Self::Insert | Self::Update | Self::Delete | Self::Revive
        )
    }
}

/// Stamp assigned to a managed DML effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DmlStamp {
    /// Row/effect sequence.
    pub seq: SeqId,
    /// Commit-order cursor.
    pub commit_seq: CommitSeq,
    /// Operation.
    pub operation: ManagedDmlOperation,
    /// Delete marker.
    pub deleted: bool,
}

impl DmlStamp {
    /// Creates a DML stamp.
    #[must_use]
    pub const fn new(seq: SeqId, commit_seq: CommitSeq, operation: ManagedDmlOperation) -> Self {
        stamp_dml_effect(seq, commit_seq, operation)
    }
}

/// Allocates a process-local row/effect sequence for non-pgrx tests.
///
/// PostgreSQL builds use `SNOWFLAKE_ID()` defaults and DML hook stamps at
/// runtime; this helper keeps pure Rust tests deterministic.
pub fn allocate_seq_for_tests() -> Result<SeqId> {
    SeqId::new(NEXT_SEQ.fetch_add(1, Ordering::SeqCst))
}

/// Plans `koldstore.hydrate_pk` for a single requested cold PK.
#[must_use]
pub fn plan_hydrate_pk(_request: &HydratePkRequest, cold_row_found: bool) -> DmlResult {
    DmlResult {
        affected_rows: i64::from(cold_row_found),
        tombstone_written: false,
        cold_lookup_performed: true,
    }
}

/// Plans `koldstore.update_row`.
#[must_use]
pub const fn plan_update_row(
    request: &UpdateRowRequest,
    hot_row_exists: bool,
    cold_pk_present: bool,
) -> ColdUpdateOutcome {
    if hot_row_exists {
        ColdUpdateOutcome::HotUpdate
    } else if !request.lookup_cold {
        ColdUpdateOutcome::NoOpColdLookupDisabled
    } else if cold_pk_present {
        ColdUpdateOutcome::ColdLookupAndUpdate
    } else {
        ColdUpdateOutcome::NoOpNotFound
    }
}

/// Plans standard SQL UPDATE of a cold-only row.
#[must_use]
pub const fn plan_standard_sql_cold_only_update(request: &UpdateRowRequest) -> ColdUpdateOutcome {
    let _ = request;
    ColdUpdateOutcome::NoOpColdLookupDisabled
}

/// Plans `koldstore.delete_row` without scanning object storage.
#[must_use]
pub const fn plan_delete_row(
    request: &DeleteRowRequest,
    input_state: DeleteInputState,
) -> DmlResult {
    match input_state {
        DeleteInputState::HotRow => DmlResult {
            affected_rows: 1,
            tombstone_written: false,
            cold_lookup_performed: false,
        },
        DeleteInputState::ColdExactLocalHint => DmlResult {
            affected_rows: 1,
            tombstone_written: true,
            cold_lookup_performed: false,
        },
        DeleteInputState::ColdMayContainLocalHint if request.allow_may_contain => DmlResult {
            affected_rows: 1,
            tombstone_written: true,
            cold_lookup_performed: false,
        },
        DeleteInputState::ColdMayContainLocalHint | DeleteInputState::Missing => DmlResult {
            affected_rows: 0,
            tombstone_written: false,
            cold_lookup_performed: false,
        },
    }
}

/// Builds a managed DML stamp from validated sequence newtypes.
#[must_use]
pub const fn stamp_dml_effect(
    seq: SeqId,
    commit_seq: CommitSeq,
    operation: ManagedDmlOperation,
) -> DmlStamp {
    DmlStamp {
        seq,
        commit_seq,
        operation,
        deleted: matches!(operation, ManagedDmlOperation::Delete),
    }
}

/// Delete route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteDecision {
    /// Remove the hot row physically.
    PhysicalDelete,
    /// Keep or insert a tombstone to mask cold rows.
    Tombstone,
}

/// Decides how to route a delete from local cold metadata.
#[must_use]
pub const fn delete_decision(cold_may_contain_pk: bool) -> DeleteDecision {
    delete_decision_with_flush_fence(cold_may_contain_pk, false)
}

/// Decides how to route a delete while an in-flight flush may have copied the row.
#[must_use]
pub const fn delete_decision_with_flush_fence(
    cold_may_contain_pk: bool,
    active_flush_fence: bool,
) -> DeleteDecision {
    if cold_may_contain_pk || active_flush_fence {
        DeleteDecision::Tombstone
    } else {
        DeleteDecision::PhysicalDelete
    }
}
