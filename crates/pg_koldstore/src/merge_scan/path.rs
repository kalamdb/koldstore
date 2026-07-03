//! CustomPath construction glue.

/// Custom scan provider name.
pub const CUSTOM_PATH_NAME: &str = "KoldstoreMergeScan";

/// Simplified planner path kind used by pure Rust planner tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerPathKind {
    /// PostgreSQL heap sequential scan.
    SeqScan,
    /// PostgreSQL heap index scan.
    IndexScan,
    /// PostgreSQL heap bitmap scan.
    BitmapScan,
    /// pg-koldstore custom scan wrapping the hot child path.
    CustomScan,
}

/// Simplified PostgreSQL path descriptor.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannerPath {
    /// Stable test/debug label.
    pub name: String,
    /// Path kind.
    pub kind: PlannerPathKind,
    /// Comparable planner cost.
    pub cost: f64,
}

impl PlannerPath {
    /// Creates a heap sequential scan path.
    #[must_use]
    pub fn seq_scan(name: impl Into<String>, cost: f64) -> Self {
        Self {
            name: name.into(),
            kind: PlannerPathKind::SeqScan,
            cost,
        }
    }

    /// Creates a heap index scan path.
    #[must_use]
    pub fn index_scan(name: impl Into<String>, cost: f64) -> Self {
        Self {
            name: name.into(),
            kind: PlannerPathKind::IndexScan,
            cost,
        }
    }

    /// Creates a heap bitmap scan path.
    #[must_use]
    pub fn bitmap_scan(name: impl Into<String>, cost: f64) -> Self {
        Self {
            name: name.into(),
            kind: PlannerPathKind::BitmapScan,
            cost,
        }
    }

    /// Creates the final custom scan path.
    #[must_use]
    pub fn custom_scan(cost: f64) -> Self {
        Self {
            name: CUSTOM_PATH_NAME.to_string(),
            kind: PlannerPathKind::CustomScan,
            cost,
        }
    }

    /// Returns the `EXPLAIN` label for this path.
    #[must_use]
    pub fn explain_label(&self) -> String {
        match self.kind {
            PlannerPathKind::CustomScan => custom_scan_explain_label().to_string(),
            PlannerPathKind::SeqScan => "Seq Scan".to_string(),
            PlannerPathKind::IndexScan => "Index Scan".to_string(),
            PlannerPathKind::BitmapScan => "Bitmap Heap Scan".to_string(),
        }
    }
}

/// Planned path replacement for a managed-table read.
#[derive(Debug, Clone, PartialEq)]
pub struct PathReplacementDecision {
    /// User-visible final paths for the managed relation.
    pub final_paths: Vec<PlannerPath>,
    /// Hot heap paths retained inside the custom path.
    pub custom_child_paths: Vec<PlannerPath>,
    /// Number of heap paths removed from final path choices.
    pub removed_heap_final_paths: usize,
}

impl PathReplacementDecision {
    /// Returns whether a heap-only path remains user-selectable as final scan.
    #[must_use]
    pub fn heap_only_final_path_available(&self) -> bool {
        self.final_paths
            .iter()
            .any(|path| path.kind != PlannerPathKind::CustomScan)
    }
}

/// Returns the `EXPLAIN` label for the custom scan node.
#[must_use]
pub const fn custom_scan_explain_label() -> &'static str {
    "Custom Scan (KoldstoreMergeScan)"
}

/// Returns whether heap-only final paths must be replaced for a managed relation.
#[must_use]
pub const fn replace_heap_final_path(is_managed: bool) -> bool {
    is_managed
}

/// Builds the pure path replacement decision for a relation.
///
/// Managed relations expose only the KoldstoreMergeScan final path; the best
/// hot heap path remains available as the custom child.
#[must_use]
pub fn build_path_replacement(
    is_managed: bool,
    hot_heap_paths: Vec<PlannerPath>,
) -> Option<PathReplacementDecision> {
    if !is_managed {
        return Some(PathReplacementDecision {
            final_paths: hot_heap_paths,
            custom_child_paths: Vec::new(),
            removed_heap_final_paths: 0,
        });
    }

    let best_child = hot_heap_paths
        .iter()
        .min_by(|left, right| left.cost.total_cmp(&right.cost))
        .cloned()?;
    Some(PathReplacementDecision {
        final_paths: vec![PlannerPath::custom_scan(best_child.cost)],
        custom_child_paths: vec![best_child],
        removed_heap_final_paths: hot_heap_paths.len(),
    })
}
