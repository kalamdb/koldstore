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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::TableFlavor;

    #[test]
    fn default_hot_dml_scenario_compares_heap_and_managed_tables() {
        let scenario = HotDmlScenario::default_tables();

        assert_eq!(NAME, "hot_dml_vs_heap");
        assert_eq!(scenario.heap.flavor, TableFlavor::Heap);
        assert_eq!(scenario.managed.flavor, TableFlavor::Koldstore);
        assert_ne!(scenario.heap.name, scenario.managed.name);
        assert!(scenario.heap.create_table_sql().contains("PRIMARY KEY"));
        assert!(scenario.managed.create_table_sql().contains("PRIMARY KEY"));
    }
}
