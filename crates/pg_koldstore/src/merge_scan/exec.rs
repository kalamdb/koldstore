//! CustomScan execution glue.

use koldstore_common::{ColdRow, HotRow};
use koldstore_merge::{resolve_rows, ResolvedRow};
use thiserror::Error;

use crate::merge_scan::plan::MergeScanPlan;

/// Scan-state cleanup hook placeholder.
pub fn reset_scan_state() {}

/// Availability of cold storage for a scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdAvailability {
    /// Cold storage can be opened.
    Available,
    /// Cold storage cannot be reached.
    Unavailable,
}

/// Merge scan execution errors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MergeScanError {
    /// Cold segments are visible, so returning only hot rows would be incomplete.
    #[error("cold data required for managed read, but cold storage is unavailable")]
    ColdRequiredUnavailable,
}

/// Merge scan execution state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanState {
    /// Managed table oid.
    pub table_oid: u32,
    /// Visible active segment paths.
    pub visible_segments: Vec<String>,
    /// Selected row groups from safe pruning.
    pub selected_row_groups: Vec<usize>,
    /// Whether a PostgreSQL snapshot has been captured.
    pub snapshot_captured: bool,
    /// Whether cold streams have been opened.
    pub cold_streams_open: bool,
    /// Resource counters owned by this scan state.
    pub resources: ScanResourceCounters,
}

impl ScanState {
    /// Creates scan state from visible segment paths.
    #[must_use]
    pub fn begin(table_oid: u32, visible_segments: Vec<String>) -> Self {
        let cold_streams_open = !visible_segments.is_empty();
        let object_store_handles = visible_segments.len();
        Self {
            table_oid,
            selected_row_groups: Vec::new(),
            snapshot_captured: true,
            cold_streams_open,
            resources: ScanResourceCounters {
                object_store_handles,
                arrow_buffers: usize::from(cold_streams_open),
                memory_context_bytes: 0,
            },
            visible_segments,
        }
    }

    /// Releases cold stream handles.
    pub fn cleanup(&mut self) {
        self.cold_streams_open = false;
        self.visible_segments.clear();
        self.selected_row_groups.clear();
        self.resources = ScanResourceCounters::default();
    }

    /// Reinitializes this scan state for PostgreSQL `Rescan`.
    ///
    /// # Errors
    ///
    /// Returns [`MergeScanError::ColdRequiredUnavailable`] when the new scan requires cold
    /// segments but cold storage is unavailable.
    pub fn rescan(
        &mut self,
        plan: &MergeScanPlan,
        cold_availability: ColdAvailability,
    ) -> Result<(), MergeScanError> {
        self.cleanup();
        *self = begin_merge_scan_with_plan(plan, cold_availability)?;
        Ok(())
    }
}

/// Resources owned by a merge scan.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ScanResourceCounters {
    /// Object-store handles opened for cold segments.
    pub object_store_handles: usize,
    /// Arrow buffers currently retained by the scan.
    pub arrow_buffers: usize,
    /// Bytes allocated in the scan memory context.
    pub memory_context_bytes: usize,
}

/// Begins a merge scan with fail-closed cold availability checks.
///
/// # Errors
///
/// Returns [`MergeScanError::ColdRequiredUnavailable`] when visible cold segments exist but the
/// cold reader cannot be opened.
pub fn begin_merge_scan(
    table_oid: u32,
    visible_segments: Vec<String>,
    cold_availability: ColdAvailability,
) -> Result<ScanState, MergeScanError> {
    if !visible_segments.is_empty() && cold_availability == ColdAvailability::Unavailable {
        return Err(MergeScanError::ColdRequiredUnavailable);
    }

    Ok(ScanState::begin(table_oid, visible_segments))
}

/// Begins a merge scan from a serialized plan model.
///
/// # Errors
///
/// Returns [`MergeScanError::ColdRequiredUnavailable`] when cold segment hints exist but cold
/// storage is unavailable.
pub fn begin_merge_scan_with_plan(
    plan: &MergeScanPlan,
    cold_availability: ColdAvailability,
) -> Result<ScanState, MergeScanError> {
    let mut visible_segments = Vec::with_capacity(plan.segment_hints.len());
    let mut selected_row_groups = Vec::new();
    for hint in plan
        .segment_hints
        .iter()
        .filter(|hint| segment_matches_scope(plan.scope_key.as_ref(), hint.scope_key.as_ref()))
    {
        visible_segments.push(hint.object_path.clone());
        selected_row_groups.extend(hint.selected_row_groups.iter().copied());
    }

    let mut state = begin_merge_scan(plan.table_oid, visible_segments, cold_availability)?;
    state.selected_row_groups = selected_row_groups;
    Ok(state)
}

fn segment_matches_scope(
    plan_scope: Option<&koldstore_common::ScopeKey>,
    segment_scope: Option<&koldstore_common::ScopeKey>,
) -> bool {
    match (plan_scope, segment_scope) {
        (Some(plan_scope), Some(segment_scope)) => plan_scope == segment_scope,
        (None, None) => true,
        _ => false,
    }
}

/// Result of a logical merge-scan execution.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeScanResult {
    /// Visible logical rows after hot/cold winner resolution and tombstone masking.
    pub rows: Vec<ResolvedRow>,
    /// Number of hot candidates observed.
    pub hot_rows_seen: usize,
    /// Number of cold candidates observed.
    pub cold_rows_seen: usize,
    /// Number of hot tombstones that participated in masking.
    pub tombstones_masked: usize,
    /// Rows filtered by residual predicates after winner resolution.
    pub filtered_rows: usize,
    /// Rows filtered by security predicates after winner resolution.
    pub security_filtered_rows: usize,
}

/// Executes the pure hot/cold merge resolution step used by the PostgreSQL CustomScan executor.
///
/// # Errors
///
/// Reserved for executor failures once this helper is wired to PostgreSQL tuple conversion and
/// cold-stream I/O.
pub fn execute_merge_scan(
    hot_rows: Vec<HotRow>,
    cold_rows: Vec<ColdRow>,
) -> Result<MergeScanResult, MergeScanError> {
    let tombstones_masked = hot_rows.iter().filter(|row| row.deleted).count();
    let hot_rows_seen = hot_rows.len();
    let cold_rows_seen = cold_rows.len();
    let rows = resolve_rows(&hot_rows, &cold_rows);

    Ok(MergeScanResult {
        rows,
        hot_rows_seen,
        cold_rows_seen,
        tombstones_masked,
        filtered_rows: 0,
        security_filtered_rows: 0,
    })
}

/// Simplified residual/security filter plan for pure executor tests.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FilterPlan {
    residual_eq: Vec<(String, String)>,
    security_eq: Vec<(String, String)>,
}

impl FilterPlan {
    /// Creates an empty filter plan.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a residual JSON string equality filter.
    #[must_use]
    pub fn with_required_json_eq(
        mut self,
        column: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.residual_eq.push((column.into(), value.into()));
        self
    }

    /// Adds a security JSON equality filter.
    #[must_use]
    pub fn with_security_json_eq(mut self, column: impl Into<String>, value: i64) -> Self {
        self.security_eq.push((column.into(), value.to_string()));
        self
    }
}

/// Executes merge resolution and then applies residual/security filters.
///
/// # Errors
///
/// Reserved for PostgreSQL expression evaluation failures in the pgrx executor.
pub fn execute_merge_scan_with_filters(
    hot_rows: Vec<HotRow>,
    cold_rows: Vec<ColdRow>,
    filters: FilterPlan,
) -> Result<MergeScanResult, MergeScanError> {
    let mut result = execute_merge_scan(hot_rows, cold_rows)?;
    let before_residual = result.rows.len();
    result
        .rows
        .retain(|row| row_matches(&row.row_image, &filters.residual_eq));
    result.filtered_rows = before_residual.saturating_sub(result.rows.len());

    let before_security = result.rows.len();
    result
        .rows
        .retain(|row| row_matches(&row.row_image, &filters.security_eq));
    result.security_filtered_rows = before_security.saturating_sub(result.rows.len());

    Ok(result)
}

fn row_matches(row: &serde_json::Value, filters: &[(String, String)]) -> bool {
    filters.iter().all(|(column, expected)| {
        row.get(column)
            .is_some_and(|value| value_matches_expected(value, expected))
    })
}

fn value_matches_expected(value: &serde_json::Value, expected: &str) -> bool {
    if let Some(actual) = value.as_str() {
        return actual == expected;
    }
    if let Some(actual) = value.as_i64() {
        return expected.parse::<i64>() == Ok(actual);
    }
    if let Some(actual) = value.as_u64() {
        return expected.parse::<u64>() == Ok(actual);
    }
    if let Some(actual) = value.as_f64() {
        return expected
            .parse::<f64>()
            .is_ok_and(|expected| (actual - expected).abs() < f64::EPSILON);
    }
    if let Some(actual) = value.as_bool() {
        return expected.parse::<bool>() == Ok(actual);
    }
    value.is_null() && expected == "null"
}

/// Returns true when a residual/security qual must be evaluated after merge.
#[must_use]
pub const fn evaluate_after_winner_resolution(is_safe_prune: bool) -> bool {
    !is_safe_prune
}
