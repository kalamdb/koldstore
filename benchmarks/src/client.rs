//! Benchmark client helpers.

/// Logical table flavor used by benchmark scenarios.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFlavor {
    /// Plain PostgreSQL heap.
    Heap,
    /// pg-koldstore managed heap.
    Koldstore,
}

/// Benchmark table definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkTable {
    /// SQL table name.
    pub name: String,
    /// Table flavor.
    pub flavor: TableFlavor,
}

impl BenchmarkTable {
    /// Returns the heap baseline table.
    #[must_use]
    pub fn heap_baseline() -> Self {
        Self {
            name: "bench.heap_items".to_string(),
            flavor: TableFlavor::Heap,
        }
    }

    /// Returns the pg-koldstore managed table.
    #[must_use]
    pub fn koldstore_managed() -> Self {
        Self {
            name: "bench.koldstore_items".to_string(),
            flavor: TableFlavor::Koldstore,
        }
    }

    /// Creates the table SQL for the benchmark schema.
    #[must_use]
    pub fn create_table_sql(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} (id bigint PRIMARY KEY, body text NOT NULL, value bigint NOT NULL)",
            self.name
        )
    }
}
