//! CustomPath construction models for managed hot/cold reads.
//!
//! Pure planner portfolio decisions live here. PostgreSQL `add_path` / pathkeys
//! wiring stays in `pg_koldstore`.

use super::strategy::{
    classify_path_strategy, KoldPathStrategy, OrderColumnSupport, OrderedPathSpec, StrategyRequest,
};

/// Custom scan provider name.
pub const CUSTOM_PATH_NAME: &str = "KoldMergeScan";

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

/// One KoldMergeScan wrapper candidate for PostgreSQL `add_path`.
#[derive(Debug, Clone, PartialEq)]
pub struct PortfolioPathEntry {
    /// Execution strategy this custom path represents.
    pub strategy: KoldPathStrategy,
    /// Native hot child kind this wrapper prefers.
    pub prefers_hot_path_kind: PlannerPathKind,
    /// Hot child path retained inside the custom path.
    pub hot_child: PlannerPath,
    /// Added to the hot child's startup cost when forming the CustomPath.
    pub startup_bias: f64,
    /// True when this path advertises real output pathkeys to PostgreSQL.
    pub advertises_order: bool,
}

/// Multi-path portfolio replacing bare heap finals on a managed relation.
#[derive(Debug, Clone, PartialEq)]
pub struct PathPortfolioDecision {
    /// Custom-path wrappers offered via `add_path` (ordered + general, etc.).
    pub entries: Vec<PortfolioPathEntry>,
    /// Number of heap paths removed from final path choices.
    pub removed_heap_final_paths: usize,
}

impl PathPortfolioDecision {
    /// Returns whether a heap-only path remains user-selectable as final scan.
    #[must_use]
    pub fn heap_only_final_path_available(&self) -> bool {
        false
    }

    /// Synthetic final custom paths for tests that still expect `PlannerPath` finals.
    #[must_use]
    pub fn final_paths(&self) -> Vec<PlannerPath> {
        self.entries
            .iter()
            .map(|entry| {
                PlannerPath::custom_scan(entry.hot_child.cost + entry.startup_bias)
            })
            .collect()
    }
}

/// Native hot path available for wrapping, with order/PK classification hints.
#[derive(Debug, Clone, PartialEq)]
pub struct HotChildCandidate {
    /// PostgreSQL-generated hot heap/index path.
    pub path: PlannerPath,
    /// Supported order this child's pathkeys can provide, if any.
    pub order_column: Option<OrderColumnSupport>,
    /// Catalog sort-order id when `order_column` is supported.
    pub sort_order_id: i32,
    /// Leading order column id when supported.
    pub leading_column_id: i16,
    /// Primary-key column ids.
    pub primary_key_columns: Vec<i16>,
    /// True when quals are exact equality on every primary-key column.
    pub exact_full_primary_key_equality: bool,
    /// Single-scope key; default `""`.
    pub scope_key: String,
}

impl HotChildCandidate {
    /// Builds a candidate with default empty `scope_key`.
    #[must_use]
    pub fn new(
        path: PlannerPath,
        order_column: Option<OrderColumnSupport>,
        sort_order_id: i32,
        leading_column_id: i16,
        primary_key_columns: Vec<i16>,
        exact_full_primary_key_equality: bool,
    ) -> Self {
        Self {
            path,
            order_column,
            sort_order_id,
            leading_column_id,
            primary_key_columns,
            exact_full_primary_key_equality,
            scope_key: String::new(),
        }
    }

    fn strategy_request(&self) -> StrategyRequest {
        let mut request = StrategyRequest::new(
            self.exact_full_primary_key_equality,
            self.order_column,
            self.sort_order_id,
            self.leading_column_id,
            self.primary_key_columns.clone(),
        );
        request.scope_key = self.scope_key.clone();
        request
    }
}

/// Planned path replacement for a managed-table read (legacy single-wrapper shape).
///
/// Prefer [`build_path_portfolio`] for multi-strategy planning. This type remains
/// for callers that model one cheapest-child wrap.
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
    "Custom Scan (KoldMergeScan)"
}

/// Builds a multi-path portfolio for a relation.
///
/// Managed relations expose only KoldMergeScan finals: one fallback wrapper
/// around the cheapest hot child, plus an ordered wrapper for each child whose
/// pathkeys match a supported immutable order. Callers must still clear
/// `partial_pathlist` (see [`clear_partial_heap_paths`]).
#[must_use]
pub fn build_path_portfolio(
    is_managed: bool,
    children: Vec<HotChildCandidate>,
    startup_bias: f64,
) -> Option<PathPortfolioDecision> {
    if !is_managed {
        return Some(PathPortfolioDecision {
            entries: Vec::new(),
            removed_heap_final_paths: 0,
        });
    }
    if children.is_empty() {
        return None;
    }

    let removed_heap_final_paths = children.len();
    let cheapest_idx = children
        .iter()
        .enumerate()
        .min_by(|(_, left), (_, right)| left.path.cost.total_cmp(&right.path.cost))
        .map(|(idx, _)| idx)?;

    let mut entries = Vec::new();

    // Fallback / primary wrapper around the cheapest hot child.
    let cheapest = &children[cheapest_idx];
    let fallback_strategy = match classify_path_strategy(&cheapest.strategy_request()) {
        KoldPathStrategy::OrderedProgressive(_) => {
            // Cheapest path may be ordered; still offer a non-ordering general
            // merge so PostgreSQL can pick unordered plans when cheaper.
            KoldPathStrategy::GeneralMerge
        }
        other => other,
    };
    entries.push(PortfolioPathEntry {
        strategy: fallback_strategy,
        prefers_hot_path_kind: cheapest.path.kind,
        hot_child: cheapest.path.clone(),
        startup_bias,
        advertises_order: false,
    });

    // Ordered progressive wrappers for children that can advertise pathkeys.
    for child in &children {
        let strategy = classify_path_strategy(&child.strategy_request());
        if let KoldPathStrategy::OrderedProgressive(spec) = strategy {
            // Skip duplicate if cheapest was already classified ordered and we
            // chose GeneralMerge as fallback — still add the ordered entry.
            entries.push(PortfolioPathEntry {
                strategy: KoldPathStrategy::OrderedProgressive(spec),
                prefers_hot_path_kind: child.path.kind,
                hot_child: child.path.clone(),
                startup_bias,
                advertises_order: true,
            });
        }
    }

    Some(PathPortfolioDecision {
        entries,
        removed_heap_final_paths,
    })
}

/// Builds the pure path replacement decision for a relation.
///
/// Managed relations expose only the KoldMergeScan final path; the best
/// hot heap path remains available as the custom child. Callers that model
/// PostgreSQL's planner must also drop parallel partial heap paths: Gather /
/// Gather Merge are built after `set_rel_pathlist` and would otherwise leak
/// hot-heap-only ordered scans after flush.
///
/// This is the legacy single-wrapper API. New planning should use
/// [`build_path_portfolio`].
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

/// Returns whether parallel partial heap paths must be cleared for a managed
/// relation (same contract as clearing `RelOptInfo.partial_pathlist`).
#[must_use]
pub const fn clear_partial_heap_paths(is_managed: bool) -> bool {
    is_managed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_relations_clear_partial_heap_paths() {
        assert!(clear_partial_heap_paths(true));
        assert!(!clear_partial_heap_paths(false));
    }

    #[test]
    fn managed_path_replacement_drops_heap_finals() {
        let decision = build_path_replacement(
            true,
            vec![
                PlannerPath::seq_scan("heap", 100.0),
                PlannerPath::index_scan("pk", 40.0),
            ],
        )
        .expect("managed relation with heap paths");
        assert_eq!(decision.final_paths.len(), 1);
        assert_eq!(decision.final_paths[0].kind, PlannerPathKind::CustomScan);
        assert_eq!(decision.custom_child_paths.len(), 1);
        assert_eq!(decision.custom_child_paths[0].cost, 40.0);
        assert_eq!(decision.removed_heap_final_paths, 2);
        assert!(clear_partial_heap_paths(true));
    }

    #[test]
    fn portfolio_offers_general_and_ordered_wrappers() {
        let decision = build_path_portfolio(
            true,
            vec![
                HotChildCandidate::new(
                    PlannerPath::seq_scan("heap", 100.0),
                    None,
                    0,
                    0,
                    vec![1],
                    false,
                ),
                HotChildCandidate::new(
                    PlannerPath::index_scan("created_at_desc", 55.0),
                    Some(OrderColumnSupport::SegmentOrder),
                    7,
                    3,
                    vec![1],
                    false,
                ),
                HotChildCandidate::new(
                    PlannerPath::index_scan("pk", 40.0),
                    Some(OrderColumnSupport::PrimaryKey),
                    1,
                    1,
                    vec![1],
                    false,
                ),
            ],
            1.5,
        )
        .expect("managed portfolio");

        assert!(clear_partial_heap_paths(true));
        assert!(!decision.heap_only_final_path_available());
        assert_eq!(decision.removed_heap_final_paths, 3);
        assert_eq!(decision.entries.len(), 3);

        let fallback = &decision.entries[0];
        assert_eq!(fallback.hot_child.name, "pk");
        assert_eq!(fallback.prefers_hot_path_kind, PlannerPathKind::IndexScan);
        assert_eq!(fallback.startup_bias, 1.5);
        assert!(!fallback.advertises_order);
        // Cheapest child was PK-ordered; fallback is still non-ordering GeneralMerge.
        assert_eq!(fallback.strategy, KoldPathStrategy::GeneralMerge);

        let ordered: Vec<_> = decision
            .entries
            .iter()
            .filter(|entry| entry.advertises_order)
            .collect();
        assert_eq!(ordered.len(), 2);
        assert!(ordered.iter().all(|entry| {
            matches!(entry.strategy, KoldPathStrategy::OrderedProgressive(_))
        }));
        assert!(ordered.iter().any(|entry| entry.hot_child.name == "created_at_desc"));
        assert!(ordered.iter().any(|entry| entry.hot_child.name == "pk"));
        assert!(ordered.iter().any(|entry| {
            matches!(
                &entry.strategy,
                KoldPathStrategy::OrderedProgressive(OrderedPathSpec {
                    sort_order_id: 7,
                    leading_column_id: 3,
                    ..
                })
            )
        }));
    }

    #[test]
    fn portfolio_exact_pk_uses_exact_primary_key_strategy() {
        let decision = build_path_portfolio(
            true,
            vec![HotChildCandidate::new(
                PlannerPath::index_scan("pk", 10.0),
                None,
                0,
                0,
                vec![1],
                true,
            )],
            0.5,
        )
        .expect("pk portfolio");

        assert_eq!(decision.entries.len(), 1);
        assert_eq!(
            decision.entries[0].strategy,
            KoldPathStrategy::ExactPrimaryKey
        );
        assert!(!decision.entries[0].advertises_order);
    }

    #[test]
    fn portfolio_mutable_order_stays_general_merge_only() {
        let decision = build_path_portfolio(
            true,
            vec![HotChildCandidate::new(
                PlannerPath::index_scan("expr", 20.0),
                Some(OrderColumnSupport::MutableOrUnsupported),
                0,
                9,
                vec![1],
                false,
            )],
            1.0,
        )
        .expect("mutable portfolio");

        assert_eq!(decision.entries.len(), 1);
        assert_eq!(decision.entries[0].strategy, KoldPathStrategy::GeneralMerge);
        assert!(!decision.entries[0].advertises_order);
    }

    #[test]
    fn unmanaged_portfolio_has_no_custom_entries() {
        let decision = build_path_portfolio(
            false,
            vec![HotChildCandidate::new(
                PlannerPath::seq_scan("heap", 10.0),
                None,
                0,
                0,
                vec![],
                false,
            )],
            1.0,
        )
        .expect("unmanaged");
        assert!(decision.entries.is_empty());
        assert_eq!(decision.removed_heap_final_paths, 0);
    }
}
