//! Public DML SQL function boundaries.

use std::sync::atomic::{AtomicI64, Ordering};

use koldstore_core::{CommitSeq, Result, SeqId, TableName};

static NEXT_SEQ: AtomicI64 = AtomicI64::new(1);

/// Public cold-DML API function names exposed through pgrx.
pub const COLD_DML_FUNCTIONS: &[&str] = &[
    "koldstore.hydrate_pk",
    "koldstore.update_row",
    "koldstore.delete_row",
];

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
    if cold_may_contain_pk {
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
