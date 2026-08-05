//! Path strategy identity for managed hot/cold reads.
//!
//! Owns which progressive or fallback execution shape a `CustomPath` represents.
//! PostgreSQL pathkeys and CustomScan FFI stay in `pg_koldstore`.

/// Planner/executor strategy for a KoldMergeScan path.
///
/// Each offered path must be a complete logical-table strategy: after cold
/// publication, unwrapped heap-only paths must not remain selectable. Strategy
/// selection is PG-free; the extension maps these variants onto `CustomPath`
/// costing, pathkeys, and emit modes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KoldPathStrategy {
    /// Exact full-primary-key equality lookup (`WHERE id = ?`).
    ExactPrimaryKey,
    /// `LIMIT` without a supported order: emit visible hot first, defer cold.
    UnorderedHotFirst,
    /// Supported immutable `ORDER BY` (± `LIMIT`): bound-gated progressive merge.
    OrderedProgressive(OrderedPathSpec),
    /// Unsupported order/expression/metadata: conservative full logical merge.
    GeneralMerge,
}

/// Immutable order identity advertised by an ordered progressive path.
///
/// Invariant: all versions of one logical primary key share the same optimized
/// order identity `(leading order column(s), primary key)`. Only advertise this
/// when comparison semantics match PostgreSQL for the encoded columns.
///
/// `scope_key` is forward-compatible plumbing: one query binds one scope.
/// Product per-user partitions are out of scope; default is `""`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderedPathSpec {
    /// Catalog sort-order identifier for composite bound lookup.
    pub sort_order_id: i32,
    /// Leading immutable order column id (attnum / column_id).
    pub leading_column_id: i16,
    /// Primary-key column ids used as deterministic tie-break.
    pub primary_key_columns: Vec<i16>,
    /// Single-scope key for catalog frontiers; default `""` today.
    pub scope_key: String,
}

impl OrderedPathSpec {
    /// Builds a spec with default empty `scope_key`.
    #[must_use]
    pub fn new(sort_order_id: i32, leading_column_id: i16, primary_key_columns: Vec<i16>) -> Self {
        Self {
            sort_order_id,
            leading_column_id,
            primary_key_columns,
            scope_key: String::new(),
        }
    }

    /// Builds a spec for an explicit single scope.
    #[must_use]
    pub fn with_scope_key(
        sort_order_id: i32,
        leading_column_id: i16,
        primary_key_columns: Vec<i16>,
        scope_key: impl Into<String>,
    ) -> Self {
        Self {
            sort_order_id,
            leading_column_id,
            primary_key_columns,
            scope_key: scope_key.into(),
        }
    }
}

/// Whether a requested order column may drive `OrderedProgressive`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderColumnSupport {
    /// Column is the immutable primary key (or a PK prefix KoldStore can prove).
    PrimaryKey,
    /// Column is the configured immutable segment-order column.
    SegmentOrder,
    /// Column may change across versions or has unsupported compare semantics.
    MutableOrUnsupported,
}

/// Classifier inputs for choosing a cold-capable path strategy.
///
/// Proven-hot-only and empty-manifest early returns stay in the planner hook;
/// this helper classifies shapes that still need a KoldMergeScan wrapper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyRequest {
    /// True when quals are exact equality on every primary-key column.
    pub exact_full_primary_key_equality: bool,
    /// Requested leading order column support; `None` if no `ORDER BY`.
    pub order_column: Option<OrderColumnSupport>,
    /// Catalog sort-order id when `order_column` is supported; ignored otherwise.
    pub sort_order_id: i32,
    /// Leading order column id when supported.
    pub leading_column_id: i16,
    /// Primary-key column ids for tie-break / exact-PK identity.
    pub primary_key_columns: Vec<i16>,
    /// Single-scope key; empty string is the default unscoped table.
    pub scope_key: String,
}

impl StrategyRequest {
    /// Builds a request with default empty `scope_key`.
    #[must_use]
    pub fn new(
        exact_full_primary_key_equality: bool,
        order_column: Option<OrderColumnSupport>,
        sort_order_id: i32,
        leading_column_id: i16,
        primary_key_columns: Vec<i16>,
    ) -> Self {
        Self {
            exact_full_primary_key_equality,
            order_column,
            sort_order_id,
            leading_column_id,
            primary_key_columns,
            scope_key: String::new(),
        }
    }
}

/// Classifies a cold-capable query shape into a `KoldPathStrategy`.
///
/// Exact full-PK equality wins over order requests (point lookup path).
/// Supported PK or segment-order columns yield `OrderedProgressive`.
/// Missing order yields `UnorderedHotFirst` (LIMIT-friendly; executor may still
/// fall back). Mutable/unsupported order yields `GeneralMerge`.
#[must_use]
pub fn classify_path_strategy(request: &StrategyRequest) -> KoldPathStrategy {
    if request.exact_full_primary_key_equality {
        return KoldPathStrategy::ExactPrimaryKey;
    }

    match request.order_column {
        None => KoldPathStrategy::UnorderedHotFirst,
        Some(OrderColumnSupport::MutableOrUnsupported) => KoldPathStrategy::GeneralMerge,
        Some(OrderColumnSupport::PrimaryKey | OrderColumnSupport::SegmentOrder) => {
            KoldPathStrategy::OrderedProgressive(OrderedPathSpec::with_scope_key(
                request.sort_order_id,
                request.leading_column_id,
                request.primary_key_columns.clone(),
                request.scope_key.clone(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_path_spec_defaults_scope_key_empty() {
        let spec = OrderedPathSpec::new(1, 2, vec![1]);
        assert_eq!(spec.scope_key, "");
    }

    #[test]
    fn strategy_request_defaults_scope_key_empty() {
        let request = StrategyRequest::new(false, None, 0, 0, vec![1]);
        assert_eq!(request.scope_key, "");
    }

    #[test]
    fn supported_segment_order_plus_pk_yields_ordered_progressive() {
        let request =
            StrategyRequest::new(false, Some(OrderColumnSupport::SegmentOrder), 7, 3, vec![1]);
        let strategy = classify_path_strategy(&request);
        assert_eq!(
            strategy,
            KoldPathStrategy::OrderedProgressive(OrderedPathSpec::new(7, 3, vec![1]))
        );
    }

    #[test]
    fn supported_primary_key_order_yields_ordered_progressive() {
        let request =
            StrategyRequest::new(false, Some(OrderColumnSupport::PrimaryKey), 1, 1, vec![1]);
        match classify_path_strategy(&request) {
            KoldPathStrategy::OrderedProgressive(spec) => {
                assert_eq!(spec.leading_column_id, 1);
                assert_eq!(spec.primary_key_columns, vec![1]);
                assert_eq!(spec.scope_key, "");
            }
            other => panic!("expected OrderedProgressive, got {other:?}"),
        }
    }

    #[test]
    fn mutable_or_unknown_order_yields_general_merge() {
        let request = StrategyRequest::new(
            false,
            Some(OrderColumnSupport::MutableOrUnsupported),
            0,
            5,
            vec![1],
        );
        assert_eq!(
            classify_path_strategy(&request),
            KoldPathStrategy::GeneralMerge
        );
    }

    #[test]
    fn exact_full_pk_equality_yields_exact_primary_key() {
        let request =
            StrategyRequest::new(true, Some(OrderColumnSupport::SegmentOrder), 7, 3, vec![1]);
        assert_eq!(
            classify_path_strategy(&request),
            KoldPathStrategy::ExactPrimaryKey
        );
    }

    #[test]
    fn no_order_yields_unordered_hot_first() {
        let request = StrategyRequest::new(false, None, 0, 0, vec![1]);
        assert_eq!(
            classify_path_strategy(&request),
            KoldPathStrategy::UnorderedHotFirst
        );
    }

    #[test]
    fn ordered_progressive_preserves_explicit_scope_key() {
        let mut request = StrategyRequest::new(
            false,
            Some(OrderColumnSupport::SegmentOrder),
            2,
            4,
            vec![1, 2],
        );
        request.scope_key = "user:42".to_string();
        match classify_path_strategy(&request) {
            KoldPathStrategy::OrderedProgressive(spec) => {
                assert_eq!(spec.scope_key, "user:42");
            }
            other => panic!("expected OrderedProgressive, got {other:?}"),
        }
    }
}
