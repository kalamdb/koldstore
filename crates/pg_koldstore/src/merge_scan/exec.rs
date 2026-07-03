//! CustomScan execution glue.

/// Scan-state cleanup hook placeholder.
pub fn reset_scan_state() {}

/// Merge scan execution state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanState {
    /// Managed table oid.
    pub table_oid: u32,
    /// Visible active segment paths.
    pub visible_segments: Vec<String>,
    /// Whether cold streams have been opened.
    pub cold_streams_open: bool,
}

impl ScanState {
    /// Creates scan state from visible segment paths.
    #[must_use]
    pub fn begin(table_oid: u32, visible_segments: Vec<String>) -> Self {
        Self {
            table_oid,
            cold_streams_open: !visible_segments.is_empty(),
            visible_segments,
        }
    }

    /// Releases cold stream handles.
    pub fn cleanup(&mut self) {
        self.cold_streams_open = false;
        self.visible_segments.clear();
    }
}

/// Returns true when a residual/security qual must be evaluated after merge.
#[must_use]
pub const fn evaluate_after_winner_resolution(is_safe_prune: bool) -> bool {
    !is_safe_prune
}
