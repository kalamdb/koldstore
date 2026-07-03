//! Full benchmark suite registration.

/// One scenario in the full benchmark suite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BenchmarkScenario {
    /// Stable scenario name.
    pub name: &'static str,
    /// Success criterion tracked by the report.
    pub success_criterion: &'static str,
}

/// Suite scenario names.
pub const SCENARIOS: &[&str] = &[
    "hot_insert_vs_heap",
    "hot_update_vs_heap",
    "hot_delete_vs_heap",
    "pk_select_hot_only",
    "pk_select_cold_required",
    "flush_throughput",
    "demigration_throughput",
];

/// Complete benchmark suite definitions.
pub const FULL_SUITE: &[BenchmarkScenario] = &[
    BenchmarkScenario {
        name: "hot_insert_vs_heap",
        success_criterion: "SC-002 hot DML within 10 percent of regular heap",
    },
    BenchmarkScenario {
        name: "hot_update_vs_heap",
        success_criterion: "SC-002 hot DML within 10 percent of regular heap",
    },
    BenchmarkScenario {
        name: "hot_delete_vs_heap",
        success_criterion: "SC-002 hot DML within 10 percent of regular heap",
    },
    BenchmarkScenario {
        name: "pk_select_hot_only",
        success_criterion: "logical point lookup returns one current row",
    },
    BenchmarkScenario {
        name: "pk_select_cold_required",
        success_criterion: "SC-006 PK lookup skips at least 90 percent of row groups",
    },
    BenchmarkScenario {
        name: "flush_throughput",
        success_criterion: "flush publishes manifest after durable cold segments",
    },
    BenchmarkScenario {
        name: "demigration_throughput",
        success_criterion: "demigration rehydrates logical rows before deactivation",
    },
];
