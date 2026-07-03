//! Hot DML benchmark scenario.

use crate::client::BenchmarkTable;

/// Benchmark scenario name.
pub const NAME: &str = "hot_dml_vs_heap";

/// Benchmark scenario definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HotDmlScenario {
    /// Heap baseline table.
    pub heap: BenchmarkTable,
    /// Managed table.
    pub managed: BenchmarkTable,
}

impl HotDmlScenario {
    /// Creates the default hot-DML comparison.
    #[must_use]
    pub fn default_tables() -> Self {
        Self {
            heap: BenchmarkTable::heap_baseline(),
            managed: BenchmarkTable::koldstore_managed(),
        }
    }
}
