//! Public DML SQL function boundaries.

use std::sync::atomic::{AtomicI64, Ordering};

use koldstore_core::{CommitSeq, MirrorOperation, PrimaryKeyColumnShape, Result, SeqId, TableName};
use thiserror::Error;

use crate::{migrate::QualifiedTableName, spi::SpiStatement};

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
    let insert_trigger_name = format!("{}_insert_capture", mirror_table.name);
    let update_trigger_name = format!("{}_update_capture", mirror_table.name);
    let delete_trigger_name = format!("{}_delete_capture", mirror_table.name);
    let source = source_table.quoted();
    let function = SpiStatement::write(
        "create change-log mirror capture function",
        &capture_function_sql(&function_name, mirror_table, primary_key),
    )
    .map_err(|error| MirrorCaptureError::Spi(error.to_string()))?;
    let insert_trigger = SpiStatement::write(
        "create change-log mirror insert trigger",
        &capture_trigger_sql(&insert_trigger_name, "INSERT", &source, &function_name),
    )
    .map_err(|error| MirrorCaptureError::Spi(error.to_string()))?;
    let update_trigger = SpiStatement::write(
        "create change-log mirror update trigger",
        &capture_trigger_sql(&update_trigger_name, "UPDATE", &source, &function_name),
    )
    .map_err(|error| MirrorCaptureError::Spi(error.to_string()))?;
    let delete_trigger = SpiStatement::write(
        "create change-log mirror delete trigger",
        &capture_trigger_sql(&delete_trigger_name, "DELETE", &source, &function_name),
    )
    .map_err(|error| MirrorCaptureError::Spi(error.to_string()))?;
    let drop_triggers = SpiStatement::write(
        "drop change-log mirror capture triggers",
        &format!(
            "DROP TRIGGER IF EXISTS {} ON {source};\nDROP TRIGGER IF EXISTS {} ON {source};\nDROP TRIGGER IF EXISTS {} ON {source}",
            quote_ident(&insert_trigger_name),
            quote_ident(&update_trigger_name),
            quote_ident(&delete_trigger_name)
        ),
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

fn capture_function_sql(
    function_name: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key: &[PrimaryKeyColumnShape],
) -> String {
    let insert_upsert = upsert_sql(mirror_table, primary_key, MirrorOperation::Insert, "NEW");
    let update_upsert = upsert_sql(mirror_table, primary_key, MirrorOperation::Update, "NEW");
    let delete_upsert = upsert_sql(mirror_table, primary_key, MirrorOperation::Delete, "OLD");
    let pk_update_guard = primary_key_update_guard(primary_key);

    format!(
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
    )
}

fn capture_trigger_sql(
    trigger_name: &str,
    operation: &str,
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
        function_name = function_name.quoted()
    )
}

fn upsert_sql(
    mirror_table: &QualifiedTableName,
    primary_key: &[PrimaryKeyColumnShape],
    operation: MirrorOperation,
    row_ref: &str,
) -> String {
    let pk_columns = primary_key
        .iter()
        .map(|column| quote_ident(column.column().as_str()))
        .collect::<Vec<_>>();
    let pk_values = primary_key
        .iter()
        .map(|column| format!("{row_ref}.{}", quote_ident(column.column().as_str())))
        .collect::<Vec<_>>();
    let mut insert_columns = pk_columns.clone();
    insert_columns.extend([
        "\"seq\"".to_string(),
        "\"op\"".to_string(),
        "\"changed_at\"".to_string(),
        "\"commit_lsn\"".to_string(),
    ]);
    let mut values = pk_values;
    values.extend([
        "SNOWFLAKE_ID()".to_string(),
        operation.code().to_string(),
        "now()".to_string(),
        "pg_current_wal_lsn()".to_string(),
    ]);

    format!(
        "INSERT INTO {mirror} ({insert_columns})\n        VALUES ({values})\n        ON CONFLICT ({conflict_columns}) DO UPDATE\n        SET \"seq\" = EXCLUDED.\"seq\",\n            \"op\" = EXCLUDED.\"op\",\n            \"changed_at\" = EXCLUDED.\"changed_at\",\n            \"commit_lsn\" = EXCLUDED.\"commit_lsn\";",
        mirror = mirror_table.quoted(),
        insert_columns = insert_columns.join(", "),
        values = values.join(", "),
        conflict_columns = pk_columns.join(", ")
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

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
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

/// SQL fragment for reviving one hot tombstone row.
#[must_use]
pub fn revive_tombstone_sql(table_name: &str) -> String {
    format!("UPDATE {table_name} SET _deleted = false WHERE _deleted = true")
}
